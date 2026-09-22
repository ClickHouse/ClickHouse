#include "config.h"

#if USE_SQLITE

#include <Common/Exception.h>
#include <TableFunctions/ITableFunction.h>
#include <Storages/StorageSQLite.h>

#include <Databases/SQLite/SQLiteUtils.h>
#include <TableFunctions/registerTableFunctions.h>

#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTFunction.h>

#include <TableFunctions/TableFunctionFactory.h>

#include <Storages/checkAndGetLiteralArgument.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_QUERY;
}

namespace
{

class TableFunctionSQLite : public ITableFunction
{
public:
    static constexpr auto name = "sqlite";
    std::string getName() const override { return name; }

    /// The 2nd argument may be a query passed to SQLite as is - a subquery `(SELECT ...)` or `query('SELECT ...')`.
    /// Such an argument must not be analyzed as an ordinary expression.
    VectorWithMemoryTracking<size_t> skipAnalysisForArguments(const QueryTreeNodePtr &, ContextPtr) const override { return {1}; }

private:
    StoragePtr executeImpl(
            const ASTPtr & ast_function, ContextPtr context,
            const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return "SQLite"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    /// Open the SQLite database on external contact (structure inference or execution) rather than in
    /// `parseArguments`. The table function never creates the database file (`allow_create` is always false), so
    /// a `SELECT` / `DESCRIBE` / `INSERT` against a missing path fails closed instead of fabricating an empty
    /// database (matching the storage engine and the `SQLite` format reader).
    ///
    /// The connection is never retained by the table function. A `CREATE TABLE ... AS sqlite(...)` proxy keeps
    /// the table function alive until the nested storage is first resolved, so a handle kept here would pin the
    /// database file the path pointed to at that moment, and a same-path replacement made before the first use
    /// would neither be observed by the nested storage's construction-time schema classification nor release
    /// the old file. Structure inference and execution are never called on the same instance anyway (see
    /// `InterpreterCreateQuery`, which instantiates the function once per step), so nothing is lost.
    std::shared_ptr<sqlite3> openConnection(ContextPtr context) const;

    String database_path;
    TableNameOrQuery remote_table_or_query;
};

StoragePtr TableFunctionSQLite::executeImpl(const ASTPtr & /*ast_function*/,
        ContextPtr context, const String & table_name, ColumnsDescription cached_columns, bool is_insert_query) const
{
    /// Reject the insert before constructing the storage, so that read-only query-backed sources do not run
    /// schema inference (preparing the user's query against SQLite) only to fail.
    if (is_insert_query && remote_table_or_query.isQuery())
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Cannot INSERT into the 'sqlite' table function: it represents the result of a query passed to SQLite, which is read-only");

    /// Open here (not in `parseArguments`), and never create the database file: a table function always refers
    /// to an already-existing table, so a missing path can only be a mistake. Even an `INSERT` would fail with
    /// `no such table` after fabricating an empty database, so opening with `allow_create` would only leave a
    /// junk file behind. A missing path therefore fails closed for reads and writes alike. The connection serves
    /// the construction-time schema inference of `StorageSQLite` only; neither the storage nor this table
    /// function retains it.
    ///
    /// When the structure is provided (`cached_columns`), do not open the database here at all and let
    /// `StorageSQLite` open it lazily on the first read or write instead. This path is taken when the nested
    /// storage of a `CREATE TABLE ... AS sqlite(...)` proxy is instantiated - e.g. by a `SELECT` from
    /// `system.tables` - and such a metadata-only access must not fail (or touch the file) just because the
    /// database file is unavailable, mirroring how `ATTACH` of the `SQLite` engine leaves the connection
    /// unopened. The generated-column classification of the explicit column list is then still pending and is
    /// repaired on the first successful open (see `updateExternalDynamicMetadataIfExists`), through a fresh
    /// connection that sees the current database file even if it was replaced at the same path since the table
    /// was created; a query-backed source is read-only and needs no classification.
    std::shared_ptr<sqlite3> connection;
    bool generated_columns_reclassification_pending = false;
    if (cached_columns.empty())
        connection = openConnection(context);
    else
        generated_columns_reclassification_pending = !remote_table_or_query.isQuery();

    auto storage = std::make_shared<StorageSQLite>(StorageID(getDatabaseName(), table_name),
                                         connection,
                                         database_path,
                                         remote_table_or_query,
                                         cached_columns, ConstraintsDescription{}, /* comment = */ "", context,
                                         generated_columns_reclassification_pending);

    storage->startup();
    return storage;
}


std::shared_ptr<sqlite3> TableFunctionSQLite::openConnection(ContextPtr context) const
{
    return openSQLiteDB(database_path, context, /* throw_on_error */ true, /* allow_create */ false);
}


ColumnsDescription TableFunctionSQLite::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    /// A query-backed insert is rejected in executeImpl, which is the only path taken by INSERT INTO TABLE
    /// FUNCTION (it is called with empty cached columns, before any external contact). It must not be rejected
    /// here, because DESCRIBE TABLE also calls getActualTableStructure with is_insert_query = true and must
    /// keep returning the inferred structure.
    ///
    /// Inferring a structure never creates the database file: a read of a missing path must fail closed rather
    /// than materialize an empty database (fail-open review finding).
    return StorageSQLite::getTableStructureFromData(openConnection(context), remote_table_or_query);
}


void TableFunctionSQLite::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();

    if (!func_args.arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function 'sqlite' must have arguments.");

    ASTs & args = func_args.arguments->children;

    if (args.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "SQLite database requires 2 arguments: database path, table name (or query)");

    /// The 2nd argument is either a table name, or a query passed to SQLite as is - `(SELECT ...)` or `query('SELECT ...')`.
    auto maybe_query = tryGetExternalDatabaseQuery(
        args[1], context, IdentifierQuotingStyle::BackticksSQLite, LiteralEscapingStyle::SQLite, IdentifierQuotingRule::Always);
    for (size_t i = 0; i < args.size(); ++i)
    {
        if (i == 1 && maybe_query)
            continue;
        args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);
    }

    database_path = checkAndGetLiteralArgument<String>(args[0], "database_path");
    if (maybe_query)
        remote_table_or_query = TableNameOrQuery(TableNameOrQuery::Type::QUERY, *maybe_query);
    else
        remote_table_or_query = TableNameOrQuery(TableNameOrQuery::Type::TABLE, checkAndGetLiteralArgument<String>(args[1], "table_name"));

    /// The database is opened on first external contact (see `openConnection`) so a `SELECT` of a missing path
    /// does not fabricate an empty database file here.
}

}

void registerTableFunctionSQLite(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionSQLite>({.description = R"DOCS_MD(
Allows to perform queries on data stored in a [SQLite](/reference/engines/database-engines/sqlite) database.

## Syntax {#syntax}

```sql
sqlite('db_path', 'table_name')
```

## Arguments {#arguments}

- `db_path` — Path to a file with an SQLite database. [String](/reference/data-types/string).
- `table_name` — Name of a table in the SQLite database, or a query passed to SQLite as is (see [Passing a query instead of a table name](#passing-a-query)). [String](/reference/data-types/string).

## Returned value {#returned-value}

- A table object with the same columns as in the original `SQLite` table.

## Passing a query instead of a table name {#passing-a-query}

Instead of a table name, the second argument can be a `SELECT` query that is passed to SQLite as is. The structure of the resulting table is inferred from the query result. SQLite reports a declared type only for a result column that is a direct column of a table; for an expression, a literal or an aggregate it reports nothing. A declared type that maps to `String` (see the [type mapping](/reference/engines/database-engines/sqlite#data_types-support)) is used as is. Every other column - one with a numeric declared type, and one without a declared type - is resolved against the storage class of its value in the first row of the query result: an `INTEGER` value gives `Int64`, a `REAL` value gives `Float64`, and any other value (`TEXT`, `BLOB`, `NULL`), as well as an empty result, gives `String`. A numeric declared type is kept only if the storage class of that row agrees with it, because SQLite reports a declared type for a compound `SELECT` as well, taken from one of its arms while the rows come from all of them; otherwise the column is typed from the storage class, like an undeclared one. The inferred type is therefore only ever widened, never narrowed. Inferring such a column starts the query in SQLite. The first row does not speak for the rest: SQLite is free to return a different storage class in every row (for example, `CASE WHEN id = 1 THEN 1 ELSE 1.5 END`), so a query-backed read is fail-closed for every column read through a numeric type - not only for the ones without a declared type. A value whose storage class does not match that type, or which is not exactly representable in it (an `INTEGER` cell of `300` in a `UInt8` column, a `REAL` cell of `16777217` in a `Float32` column), fails the read instead of being silently coerced. To read a column with values of mixed storage classes, declare it as `String` or cast it to text in the SQLite query. Every inferred column is `Nullable`. The query can be written either as a subquery, or wrapped into the `query` function:

```sql
SELECT * FROM sqlite('sqlite.db', (SELECT col1, col2 FROM table1 WHERE col2 > 1));
SELECT * FROM sqlite('sqlite.db', query('SELECT col1, col2 FROM table1 WHERE col2 > 1'));
```

Such a table is read-only: `INSERT` into it is not allowed. The same syntax is supported by the [`SQLite`](/reference/engines/table-engines/integrations/sqlite) table engine.

<Note>
The subquery form `(SELECT ...)` is parsed by ClickHouse and re-serialized before being sent to SQLite. It must therefore be valid ClickHouse SQL. To pass SQLite-specific syntax that ClickHouse does not parse, use the `query('...')` form, whose text is sent to SQLite verbatim.

Any outer `WHERE`, `LIMIT`, aggregation, etc. of the surrounding ClickHouse query is **not** pushed down into the passed query — it is applied in ClickHouse after the full query result is fetched. To restrict the data read from SQLite, put the filter inside the passed query. With [`external_table_strict_query = 1`](/reference/settings/session-settings/external-table#external_table_strict_query) an outer filter on the columns of the table function is rejected with an exception instead of being applied locally, because it cannot be pushed into the passed query. The check covers the top-level `WHERE` predicate and each conjunct of a top-level `AND`. A `PREWHERE` on the columns of this table is not a case for this setting: this table engine do not support `PREWHERE`, and such a query is rejected with `ILLEGAL_PREWHERE` regardless of the setting. The check runs only where a filter could be pushed down at all: when this table is the only table of the query, on either side of an `INNER JOIN`, or on the preserving side of an outer join (the left side of a `LEFT JOIN`, the right side of a `RIGHT JOIN`). On the non-preserving side of a `LEFT`/`RIGHT JOIN` and on either side of a `FULL JOIN` nothing is pushed down and nothing is checked, so a filter on the columns of this table is applied locally after the join even in strict mode. Where the check runs, a predicate that references other tables joined in the surrounding query is not pushed down and is excluded from the check, whether it references only the joined side or mixes it with this table inside one non-`AND` expression (for example an `OR`); such a predicate keeps its usual ClickHouse evaluation point (`WHERE` after the join, `PREWHERE` before it) and is not rejected.
</Note>

## Example {#example}

```sql title="Query"
SELECT * FROM sqlite('sqlite.db', 'table1') ORDER BY col2;
```

```text title="Response"
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```

## Related {#related}

- [SQLite](/reference/engines/table-engines/integrations/sqlite) table engine
- [SQLite database engine](/reference/engines/database-engines/sqlite) — Data types support section
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction});
}

}

#endif
