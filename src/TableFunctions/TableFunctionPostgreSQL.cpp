#include "config.h"

#if USE_LIBPQXX

#include <TableFunctions/ITableFunction.h>
#include <Core/PostgreSQL/PoolWithFailover.h>
#include <Storages/StoragePostgreSQL.h>
#include <Storages/PostgreSQL/PostgreSQLSettings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/Exception.h>
#include <TableFunctions/registerTableFunctions.h>


namespace DB
{
namespace PostgreSQLSetting
{
    extern const PostgreSQLSettingsUInt64 postgresql_connection_pool_size;
    extern const PostgreSQLSettingsUInt64 postgresql_connection_pool_wait_timeout;
    extern const PostgreSQLSettingsUInt64 postgresql_connection_pool_retries;
    extern const PostgreSQLSettingsBool postgresql_connection_pool_auto_close_connection;
    extern const PostgreSQLSettingsUInt64 postgresql_connection_attempt_timeout;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_QUERY;
}

namespace
{

class TableFunctionPostgreSQL : public ITableFunction
{
public:
    static constexpr auto name = "postgresql";
    std::string getName() const override { return name; }

    /// The 3rd argument may be a query passed to PostgreSQL as is - a subquery `(SELECT ...)` or `query('SELECT ...')`.
    /// Such an argument must not be analyzed as an ordinary expression.
    VectorWithMemoryTracking<size_t> skipAnalysisForArguments(const QueryTreeNodePtr &, ContextPtr) const override { return {2}; }

private:
    StoragePtr executeImpl(
            const ASTPtr & ast_function, ContextPtr context,
            const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return "PostgreSQL"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    postgres::PoolWithFailoverPtr connection_pool;
    std::optional<StoragePostgreSQL::Configuration> configuration;
};

StoragePtr TableFunctionPostgreSQL::executeImpl(const ASTPtr & /*ast_function*/,
        ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const
{
    /// Reject the insert before constructing the storage, so that read-only query-backed sources do not contact
    /// the external database for schema inference (which could run an expensive or volatile query) only to fail.
    if (is_insert_query && configuration->table_or_query.isQuery())
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Cannot INSERT into the 'postgresql' table function: it represents the result of a query passed to PostgreSQL, which is read-only");

    auto result = std::make_shared<StoragePostgreSQL>(
        StorageID(getDatabaseName(), table_name),
        connection_pool,
        configuration->table_or_query,
        cached_columns,
        ConstraintsDescription{},
        String{},
        context,
        configuration->schema,
        configuration->on_conflict);

    result->startup();
    return result;
}


ColumnsDescription TableFunctionPostgreSQL::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    /// A query-backed insert is rejected in executeImpl, which is the only path taken by INSERT INTO TABLE
    /// FUNCTION (it is called with empty cached columns, before any external contact). It must not be rejected
    /// here, because DESCRIBE TABLE also calls getActualTableStructure with is_insert_query = true and must
    /// keep returning the inferred structure.
    return StoragePostgreSQL::getTableStructureFromData(connection_pool, configuration->table_or_query, configuration->schema, context);
}


void TableFunctionPostgreSQL::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function 'PostgreSQL' must have arguments.");

    /// The connection-pool parameters are seeded from the query-level `postgresql_*` settings
    /// (preserving the historical behaviour); a named collection may override them, and a trailing
    /// `SETTINGS ...` clause takes the final precedence, like on the table engine.
    PostgreSQLSettings postgresql_settings;
    postgresql_settings.loadFromQueryContext(*context);

    auto & args = func_args.arguments->children;
    ASTPtr settings_ast;
    for (auto it = args.begin(); it != args.end(); ++it)
    {
        if ((*it)->as<ASTSetQuery>())
        {
            settings_ast = *it;
            args.erase(it);
            break;
        }
    }

    configuration.emplace(StoragePostgreSQL::getConfiguration(args, context, &postgresql_settings));

    /// Applied after getConfiguration, so that the explicit SETTINGS clause wins over the values
    /// stored in a named collection.
    if (settings_ast)
        postgresql_settings.loadFromQuery(settings_ast->as<ASTSetQuery &>());

    if (!postgresql_settings[PostgreSQLSetting::postgresql_connection_pool_size])
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "postgresql_connection_pool_size cannot be zero.");

    connection_pool = std::make_shared<postgres::PoolWithFailover>(
        *configuration,
        postgresql_settings[PostgreSQLSetting::postgresql_connection_pool_size],
        postgresql_settings[PostgreSQLSetting::postgresql_connection_pool_wait_timeout],
        postgresql_settings[PostgreSQLSetting::postgresql_connection_pool_retries],
        postgresql_settings[PostgreSQLSetting::postgresql_connection_pool_auto_close_connection],
        postgresql_settings[PostgreSQLSetting::postgresql_connection_attempt_timeout]);
}

}


void registerTableFunctionPostgreSQL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionPostgreSQL>({.description = R"DOCS_MD(
Allows `SELECT` and `INSERT` queries to be performed on data that is stored on a remote PostgreSQL server.

## Syntax {#syntax}

```sql
postgresql({host:port, database, table, user, password[, schema, [, on_conflict]] | named_collection[, option=value [,..]]} [, SETTINGS name=value, ...])
```

## Arguments {#arguments}

| Argument      | Description                                                                |
|---------------|----------------------------------------------------------------------------|
| `host:port`   | PostgreSQL server address.                                                 |
| `database`    | Remote database name.                                                      |
| `table`       | Remote table name, or a query passed to PostgreSQL as is (see [Passing a query instead of a table name](#passing-a-query)). |
| `user`        | PostgreSQL user.                                                           |
| `password`    | User password.                                                             |
| `schema`      | Non-default table schema. Optional.                                        |
| `on_conflict` | Conflict resolution strategy. Example: `ON CONFLICT DO NOTHING`. Optional. |

Arguments also can be passed using [named collections](/concepts/features/configuration/server-config/named-collections). In this case `host` and `port` should be specified separately. This approach is recommended for production environment.

## Returned value {#returned_value}

A table object with the same columns as the original PostgreSQL table.

<Note>
In the `INSERT` query to distinguish table function `postgresql(...)` from table name with column names list you must use keywords `FUNCTION` or `TABLE FUNCTION`. See examples below.
</Note>

## Settings {#settings}

The connection pool used by the `postgresql` table function (and the [`PostgreSQL`](/reference/engines/table-engines/integrations/postgresql) table engine) can be configured with a trailing `SETTINGS` clause. When a setting is not specified, it defaults to the value of the corresponding query-level `postgresql_*` setting. See the table engine's [Settings](/reference/engines/table-engines/integrations/postgresql#settings) section for the full list of `postgresql_connection_pool_*` and `postgresql_connection_attempt_timeout` settings and their defaults.

Example:

```sql
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password', SETTINGS postgresql_connection_pool_size = 32);
```

## Implementation Details {#implementation-details}

`SELECT` queries on PostgreSQL side run as `COPY (SELECT ...) TO STDOUT` inside read-only PostgreSQL transaction with commit after each `SELECT` query.

Simple `WHERE` clauses such as `=`, `!=`, `>`, `>=`, `<`, `<=`, and `IN` are executed on the PostgreSQL server.

All joins, aggregations, sorting, `IN [ array ]` conditions and the `LIMIT` sampling constraint are executed in ClickHouse only after the query to PostgreSQL finishes.

## Passing a query instead of a table name {#passing-a-query}

Instead of a table name, the third argument can be a `SELECT` query that is passed to PostgreSQL as is. The structure of the resulting table is inferred from the query result. The query can be written either as a subquery, or wrapped into the `query` function:

```sql
SELECT * FROM postgresql('localhost:5432', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
SELECT * FROM postgresql('localhost:5432', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

This is useful to push down joins, aggregations or any other processing to PostgreSQL. Such a table is read-only: `INSERT` into it is not allowed. The same syntax is supported by the [`PostgreSQL`](/reference/engines/table-engines/integrations/postgresql) table engine.

<Note>
The subquery form `(SELECT ...)` is parsed by ClickHouse and re-serialized in the PostgreSQL dialect (PostgreSQL identifier quoting and string-literal escaping) before being sent to the server. It must therefore be valid ClickHouse SQL. To pass PostgreSQL-specific syntax that ClickHouse does not parse, use the `query('...')` form, whose text is sent to PostgreSQL verbatim.

Any outer `WHERE`, `LIMIT`, aggregation, etc. of the surrounding ClickHouse query is **not** pushed down into the passed query — it is applied in ClickHouse after the full query result is fetched. To restrict the data read from PostgreSQL, put the filter inside the passed query. With [`external_table_strict_query = 1`](/reference/settings/session-settings#external_table_strict_query) an outer filter that cannot be pushed down is rejected with an exception instead of being applied locally.
</Note>

`INSERT` queries on PostgreSQL side run as `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` inside PostgreSQL transaction with auto-commit after each `INSERT` statement.

PostgreSQL Array types converts into ClickHouse arrays.

<Note>
Be careful, in PostgreSQL an array data type column like Integer[] may contain arrays of different dimensions in different rows, but in ClickHouse it is only allowed to have multidimensional arrays of the same dimension in all rows.
</Note>

Supports multiple replicas that must be listed by `|`. For example:

```sql
SELECT name FROM postgresql(`postgres{1|2|3}:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

or

```sql
SELECT name FROM postgresql(`postgres1:5431|postgres2:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

Supports replicas priority for PostgreSQL dictionary source. The bigger the number in map, the less the priority. The highest priority is `0`.

## Examples {#examples}

Table in PostgreSQL:

```text
postgres=# CREATE TABLE "public"."test" (
"int_id" SERIAL,
"int_nullable" INT NULL DEFAULT NULL,
"float" FLOAT NOT NULL,
"str" VARCHAR(100) NOT NULL DEFAULT '',
"float_nullable" FLOAT NULL DEFAULT NULL,
PRIMARY KEY (int_id));

CREATE TABLE

postgres=# INSERT INTO test (int_id, str, "float") VALUES (1,'test',2);
INSERT 0 1

postgresql> SELECT * FROM test;
  int_id | int_nullable | float | str  | float_nullable
 --------+--------------+-------+------+----------------
       1 |              |     2 | test |
(1 row)
```

Selecting data from ClickHouse using plain arguments:

```sql
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password') WHERE str IN ('test');
```

Or using [named collections](/concepts/features/configuration/server-config/named-collections):

```sql
CREATE NAMED COLLECTION mypg AS
        host = 'localhost',
        port = 5432,
        database = 'test',
        user = 'postgresql_user',
        password = 'password';
SELECT * FROM postgresql(mypg, table='test') WHERE str IN ('test');
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

Inserting:

```sql
INSERT INTO TABLE FUNCTION postgresql('localhost:5432', 'test', 'test', 'postgrsql_user', 'password') (int_id, float) VALUES (2, 3);
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password');
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
│      2 │         ᴺᵁᴸᴸ │     3 │      │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

Using Non-default Schema:

```text
postgres=# CREATE SCHEMA "nice.schema";

postgres=# CREATE TABLE "nice.schema"."nice.table" (a integer);

postgres=# INSERT INTO "nice.schema"."nice.table" SELECT i FROM generate_series(0, 99) as t(i)
```

```sql
CREATE TABLE pg_table_schema_with_dots (a UInt32)
        ENGINE PostgreSQL('localhost:5432', 'clickhouse', 'nice.table', 'postgrsql_user', 'password', 'nice.schema');
```

## Related {#related}

- [The PostgreSQL table engine](/reference/engines/table-engines/integrations/postgresql)
- [Using PostgreSQL as a dictionary source](/reference/statements/create/dictionary/sources/postgresql)

### Replicating or migrating Postgres data with PeerDB {#replicating-or-migrating-postgres-data-with-peerdb}

> In addition to table functions, you can always use [PeerDB](https://docs.peerdb.io/introduction) by ClickHouse to set up a continuous data pipeline from Postgres to ClickHouse. PeerDB is a tool designed specifically to replicate data from Postgres to ClickHouse using change data capture (CDC).
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction});
}

}

#endif
