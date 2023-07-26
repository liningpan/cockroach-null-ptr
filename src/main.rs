use diesel::expression::AsExpression;
use diesel::helper_types::AsExprOf;
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::sql_types;
use std::env;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TableName {
    pub sql_name: String,
    pub rust_name: String,
    pub schema: Option<String>,
}
impl TableName {
    pub fn from_name<T: Into<String>>(name: T) -> Self {
        let name = name.into();

        TableName {
            rust_name: name.clone(),
            sql_name: name,
            schema: None,
        }
    }

    pub fn new<T, U>(name: T, schema: U) -> Self
    where
        T: Into<String>,
        U: Into<String>,
    {
        let name = name.into();

        TableName {
            rust_name: name.clone(),
            sql_name: name,
            schema: Some(schema.into()),
        }
    }
}

diesel::postfix_operator!(Regclass, "::regclass", sql_types::Oid, backend: Pg);
fn regclass(table: &TableName) -> Regclass<AsExprOf<String, sql_types::Text>> {
    let table_name = match table.schema {
        Some(ref schema_name) => format!("\"{}\".\"{}\"", schema_name, table.sql_name),
        None => format!("\"{}\"", table.sql_name),
    };

    Regclass::new(<String as AsExpression<sql_types::Text>>::as_expression(
        table_name,
    ))
}

sql_function!(fn obj_description(oid: sql_types::Oid, catalog: sql_types::Text) -> Nullable<Text>);

pub fn get_table_comment(
    conn: &mut PgConnection,
    table: &TableName,
) -> QueryResult<Option<String>> {
    diesel::select(obj_description(regclass(table), "pg_class")).get_result(conn)
}

fn main() -> anyhow::Result<()> {
    let connection_url = env::var("PG_DATABASE_URL")
        .or_else(|_| env::var("DATABASE_URL"))
        .expect("DATABASE_URL must be set in order to run tests");
    let mut connection = PgConnection::establish(&connection_url)?;

    // The same issue disappears when using a transaction
    // connection.transaction(comment_test)?;
    comment_test(&mut connection)?;
    Ok(())
}

fn comment_test(connection: &mut PgConnection) -> anyhow::Result<()> {
    diesel::sql_query("CREATE SCHEMA test_schema").execute(connection)?;
    diesel::sql_query(
        "CREATE TABLE test_schema.table_1 (id SERIAL PRIMARY KEY, text_col VARCHAR, not_null TEXT NOT NULL)",
    ).execute( connection)?;
    diesel::sql_query("COMMENT ON TABLE test_schema.table_1 IS 'table comment'")
        .execute(connection)?;
    diesel::sql_query("CREATE TABLE test_schema.table_2 (array_col VARCHAR[] NOT NULL)")
        .execute(connection)?;

    let table_1 = TableName::new("table_1", "test_schema");
    let table_2 = TableName::new("table_2", "test_schema");

    println!("{:?}", get_table_comment(connection, &table_1)?);
    println!("{:?}", get_table_comment(connection, &table_2)?);
    Ok(())
}
