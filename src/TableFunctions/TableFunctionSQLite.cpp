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

    String database_path;
    TableNameOrQuery remote_table_or_query;
    std::shared_ptr<sqlite3> sqlite_db;
};

StoragePtr TableFunctionSQLite::executeImpl(const ASTPtr & /*ast_function*/,
        ContextPtr context, const String & table_name, ColumnsDescription cached_columns, bool is_insert_query) const
{
    /// Reject the insert before constructing the storage, so that read-only query-backed sources do not run
    /// schema inference (preparing the user's query against SQLite) only to fail.
    if (is_insert_query && remote_table_or_query.isQuery())
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Cannot INSERT into the 'sqlite' table function: it represents the result of a query passed to SQLite, which is read-only");

    auto storage = std::make_shared<StorageSQLite>(StorageID(getDatabaseName(), table_name),
                                         sqlite_db,
                                         database_path,
                                         remote_table_or_query,
                                         cached_columns, ConstraintsDescription{}, /* comment = */ "", context);

    storage->startup();
    return storage;
}


ColumnsDescription TableFunctionSQLite::getActualTableStructure(ContextPtr /* context */, bool /*is_insert_query*/) const
{
    /// A query-backed insert is rejected in executeImpl, which is the only path taken by INSERT INTO TABLE
    /// FUNCTION (it is called with empty cached columns, before any external contact). It must not be rejected
    /// here, because DESCRIBE TABLE also calls getActualTableStructure with is_insert_query = true and must
    /// keep returning the inferred structure.
    return StorageSQLite::getTableStructureFromData(sqlite_db, remote_table_or_query);
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
        args[1], context, IdentifierQuotingStyle::DoubleQuotes, LiteralEscapingStyle::Regular);
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

    sqlite_db = openSQLiteDB(database_path, context);
}

}

void registerTableFunctionSQLite(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionSQLite>({});
}

}

#endif
