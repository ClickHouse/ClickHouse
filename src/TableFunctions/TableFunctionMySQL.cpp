#include "config.h"

#if USE_MYSQL

#include <Storages/StorageMySQL.h>

#include <Core/Settings.h>
#include <Processors/Sources/MySQLSource.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Storages/MySQL/MySQLSettings.h>
#include <Storages/MySQL/MySQLHelpers.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/Exception.h>
#include <Common/parseAddress.h>
#include <Common/quoteString.h>
#include <TableFunctions/registerTableFunctions.h>

#include <Databases/MySQL/DatabaseMySQL.h>
#include <Common/parseRemoteDescription.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 external_storage_connect_timeout_sec;
    extern const SettingsUInt64 external_storage_rw_timeout_sec;
    extern const SettingsMySQLDataTypesSupport mysql_datatypes_support_level;
}

namespace MySQLSetting
{
    extern const MySQLSettingsUInt64 connect_timeout;
    extern const MySQLSettingsUInt64 read_write_timeout;
    extern const MySQLSettingsMySQLDataTypesSupport mysql_datatypes_support_level;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
}

namespace
{

/* mysql ('host:port', database, table, user, password) - creates a temporary StorageMySQL.
 * The structure of the table is taken from the mysql query DESCRIBE table.
 * If there is no such table, an exception is thrown.
 */
class TableFunctionMySQL : public ITableFunction
{
public:
    static constexpr auto name = "mysql";
    std::string getName() const override
    {
        return name;
    }

    /// The 3rd argument may be a query passed to MySQL as is - a subquery `(SELECT ...)` or `query('SELECT ...')`.
    /// Such an argument must not be analyzed as an ordinary expression.
    VectorWithMemoryTracking<size_t> skipAnalysisForArguments(const QueryTreeNodePtr &, ContextPtr) const override { return {2}; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;
    const char * getStorageEngineName() const override { return "MySQL"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    mutable std::optional<mysqlxx::PoolWithFailover> pool;
    std::optional<StorageMySQL::Configuration> configuration;

    /// The effective settings for this `mysql(...)` call, with `mysql_datatypes_support_level` set to
    /// the query-context value overridden by a function-local `SETTINGS` clause or named collection.
    /// The type-mapping level must be used during schema inference instead of the (default) engine
    /// settings, otherwise an opt-out passed to the table function would be silently ignored.
    std::optional<MySQLSettings> effective_settings;
};

void TableFunctionMySQL::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function 'mysql' must have arguments.");

    auto & args = args_func.arguments->children;

    MySQLSettings mysql_settings;

    const auto & settings = context->getSettingsRef();
    mysql_settings[MySQLSetting::connect_timeout] = settings[Setting::external_storage_connect_timeout_sec];
    mysql_settings[MySQLSetting::read_write_timeout] = settings[Setting::external_storage_rw_timeout_sec];

    /// Seed the type-mapping level from the query context so that it is the default for schema
    /// inference. A function-local `SETTINGS` clause (below) or named collection (in
    /// `getConfiguration`) overrides it.
    mysql_settings[MySQLSetting::mysql_datatypes_support_level] = settings[Setting::mysql_datatypes_support_level];

    for (auto it = args.begin(); it != args.end(); ++it)
    {
        const ASTSetQuery * settings_ast = (*it)->as<ASTSetQuery>();
        if (settings_ast)
        {
            mysql_settings.loadFromQuery(*settings_ast);
            args.erase(it);
            break;
        }
    }

    configuration = StorageMySQL::getConfiguration(args, context, mysql_settings);
    effective_settings.emplace(mysql_settings);
    pool.emplace(createMySQLPoolWithFailover(*configuration, mysql_settings));
}

ColumnsDescription TableFunctionMySQL::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    /// A query-backed insert is rejected in executeImpl, which is the only path taken by INSERT INTO TABLE
    /// FUNCTION (it is called with empty cached columns, before any external contact). It must not be rejected
    /// here, because DESCRIBE TABLE also calls getActualTableStructure with is_insert_query = true and must
    /// keep returning the inferred structure.
    ///
    /// Use the effective type-mapping level computed in `parseArguments` (the query-context value,
    /// overridden by a function-local `SETTINGS` clause or named collection).
    return StorageMySQL::getTableStructureFromData(
        *pool, configuration->database, configuration->table_or_query, context,
        (*effective_settings)[MySQLSetting::mysql_datatypes_support_level]);
}

StoragePtr TableFunctionMySQL::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription cached_columns,
    bool is_insert_query) const
{
    /// Reject the insert before constructing the storage, so that read-only query-backed sources do not contact
    /// the external database for schema inference (which could run an expensive or volatile query) only to fail.
    if (is_insert_query && configuration->table_or_query.isQuery())
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Cannot INSERT into the 'mysql' table function: it represents the result of a query passed to MySQL, which is read-only");

    /// Carry the effective type-mapping level so that, when the columns are not provided and
    /// `StorageMySQL` infers them itself, it honors the same level as `getActualTableStructure`.
    MySQLSettings mysql_settings;
    mysql_settings[MySQLSetting::mysql_datatypes_support_level]
        = (*effective_settings)[MySQLSetting::mysql_datatypes_support_level];


    auto res = std::make_shared<StorageMySQL>(
        StorageID(getDatabaseName(), table_name),
        std::move(*pool),
        configuration->database,
        configuration->table_or_query,
        configuration->replace_query,
        configuration->on_duplicate_clause,
        cached_columns,
        ConstraintsDescription{},
        String{},
        context,
        mysql_settings);

    pool.reset();

    res->startup();
    return res;
}

}


void registerTableFunctionMySQL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMySQL>({.description = R"DOCS_MD(
Allows `SELECT` and `INSERT` queries to be performed on data that are stored on a remote MySQL server.

## Syntax {#syntax}

```sql
mysql({host:port, database, table, user, password[, replace_query, on_duplicate_clause] | named_collection[, option=value [,..]]})
```

## Arguments {#arguments}

| Argument            | Description                                                                                                                                                                                                                                                           |
|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `host:port`         | MySQL server address.                                                                                                                                                                                                                                                 |
| `database`          | Remote database name.                                                                                                                                                                                                                                                 |
| `table`             | Remote table name, or a query passed to MySQL as is (see [Passing a query instead of a table name](#passing-a-query)).                                                                                                                                                  |
| `user`              | MySQL user.                                                                                                                                                                                                                                                           |
| `password`          | User password.                                                                                                                                                                                                                                                        |
| `replace_query`     | Flag that converts `INSERT INTO` queries to `REPLACE INTO`. Possible values:<br/>    - `0` - The query is executed as `INSERT INTO`.<br/>    - `1` - The query is executed as `REPLACE INTO`.                                                                          |
| `on_duplicate_clause` | The `ON DUPLICATE KEY on_duplicate_clause` expression that is added to the `INSERT` query. Can be specified only with `replace_query = 0` (if you simultaneously pass `replace_query = 1` and `on_duplicate_clause`, ClickHouse generates an exception).<br/>    Example: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1;`<br/>    `on_duplicate_clause` here is `UPDATE c2 = c2 + 1`. See the MySQL documentation to find which `on_duplicate_clause` you can use with the `ON DUPLICATE KEY` clause. |

Arguments also can be passed using [named collections](/concepts/features/configuration/server-config/named-collections). In this case `host` and `port` should be specified separately. This approach is recommended for production environment.

Simple `WHERE` clauses such as `=, !=, >, >=, <, <=` are currently executed on the MySQL server.

The rest of the conditions and the `LIMIT` sampling constraint are executed in ClickHouse only after the query to MySQL finishes.

## Passing a query instead of a table name {#passing-a-query}

Instead of a table name, the third argument can be a `SELECT` query that is passed to MySQL as is. The structure of the resulting table is inferred from the query result. The query can be written either as a subquery, or wrapped into the `query` function:

```sql
SELECT * FROM mysql('localhost:3306', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
SELECT * FROM mysql('localhost:3306', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

This is useful to push down joins, aggregations or any other processing to MySQL. Such a table is read-only: `INSERT` into it is not allowed. The same syntax is supported by the [`MySQL`](/reference/engines/table-engines/integrations/mysql) table engine.

<Note>
The subquery form `(SELECT ...)` is parsed by ClickHouse and re-serialized in the MySQL dialect (backtick identifier quoting) before being sent to the server. It must therefore be valid ClickHouse SQL. To pass MySQL-specific syntax that ClickHouse does not parse, use the `query('...')` form, whose text is sent to MySQL verbatim.

Any outer `WHERE`, `LIMIT`, aggregation, etc. of the surrounding ClickHouse query is **not** pushed down into the passed query — it is applied in ClickHouse after the full query result is fetched. To restrict the data read from MySQL, put the filter inside the passed query. With [`external_table_strict_query = 1`](/reference/settings/session-settings#external_table_strict_query) an outer filter that cannot be pushed down is rejected with an exception instead of being applied locally.
</Note>

Supports multiple replicas that must be listed by `|`. For example:

```sql
SELECT name FROM mysql(`mysql{1|2|3}:3306`, 'mysql_database', 'mysql_table', 'user', 'password');
```

or

```sql
SELECT name FROM mysql(`mysql1:3306|mysql2:3306|mysql3:3306`, 'mysql_database', 'mysql_table', 'user', 'password');
```

## Returned value {#returned_value}

A table object with the same columns as the original MySQL table.

<Note>
Some data types of MySQL can be mapped to different ClickHouse types - this is addressed by query-level setting [mysql_datatypes_support_level](/reference/settings/session-settings#mysql_datatypes_support_level)
</Note>

<Note>
In the `INSERT` query to distinguish table function `mysql(...)` from table name with column names list, you must use keywords `FUNCTION` or `TABLE FUNCTION`. See examples below.
</Note>

## Examples {#examples}

Table in MySQL:

```text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `float` FLOAT NOT NULL,
    ->   PRIMARY KEY (`int_id`));

mysql> INSERT INTO test (`int_id`, `float`) VALUES (1,2);

mysql> SELECT * FROM test;
+--------+-------+
| int_id | float |
+--------+-------+
|      1 |     2 |
+--------+-------+
```

Selecting data from ClickHouse:

```sql
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123');
```

Or using [named collections](/concepts/features/configuration/server-config/named-collections):

```sql
CREATE NAMED COLLECTION creds AS
        host = 'localhost',
        port = 3306,
        database = 'test',
        user = 'bayonet',
        password = '123';
SELECT * FROM mysql(creds, table='test');
```

```text
┌─int_id─┬─float─┐
│      1 │     2 │
└────────┴───────┘
```

### `enable_compression` {#enable-compression}

Enables compression for the MySQL protocol connection.

Default value: `false`.

This setting applies to:

- the `mysql` table function;
- the `MySQL` table engine;
- the `MySQL` database engine;
- named collections used by MySQL integrations.

When enabled, ClickHouse requests compression for the connection.

Example:

```sql
SELECT *
FROM mysql(
    'mysql80:3306',
    'clickhouse',
    'test_table',
    'root',
    'password',
    SETTINGS enable_compression = 1
);
```

Replacing and inserting:

```sql
INSERT INTO FUNCTION mysql('localhost:3306', 'test', 'test', 'bayonet', '123', 1) (int_id, float) VALUES (1, 3);
INSERT INTO TABLE FUNCTION mysql('localhost:3306', 'test', 'test', 'bayonet', '123', 0, 'UPDATE int_id = int_id + 1') (int_id, float) VALUES (1, 4);
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123');
```

```text
┌─int_id─┬─float─┐
│      1 │     3 │
│      2 │     4 │
└────────┴───────┘
```

Copying data from MySQL table into ClickHouse table:

```sql
CREATE TABLE mysql_copy
(
   `id` UInt64,
   `datetime` DateTime('UTC'),
   `description` String,
)
ENGINE = MergeTree
ORDER BY (id,datetime);

INSERT INTO mysql_copy
SELECT * FROM mysql('host:port', 'database', 'table', 'user', 'password');
```

Or if copying only an incremental batch from MySQL based on the max current id:

```sql
INSERT INTO mysql_copy
SELECT * FROM mysql('host:port', 'database', 'table', 'user', 'password')
WHERE id > (SELECT max(id) FROM mysql_copy);
```

## Related {#related}

- [The 'MySQL' table engine](/reference/engines/table-engines/integrations/mysql)
- [Using MySQL as a dictionary source](/reference/statements/create/dictionary/sources/mysql)
- [mysql_datatypes_support_level](/reference/settings/session-settings#mysql_datatypes_support_level)
- [mysql_map_fixed_string_to_text_in_show_columns](/reference/settings/session-settings#mysql_map_fixed_string_to_text_in_show_columns)
- [mysql_map_string_to_text_in_show_columns](/reference/settings/session-settings#mysql_map_string_to_text_in_show_columns)
- [mysql_max_rows_to_insert](/reference/settings/session-settings#mysql_max_rows_to_insert)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction});
}

}

#endif
