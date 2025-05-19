#include "config.h"

#if USE_SQLITE

#include <Common/Exception.h>
#include <TableFunctions/ITableFunction.h>
#include <Storages/StorageSQLite.h>
#include <Storages/TableNameOrQuery.h>

#include <Databases/SQLite/SQLiteUtils.h>
#include "registerTableFunctions.h"

#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>

#include <TableFunctions/TableFunctionFactory.h>

#include <Storages/checkAndGetLiteralArgument.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

class TableFunctionSQLite : public ITableFunction
{
public:
    static constexpr auto name = "sqlite";
    std::string getName() const override { return name; }

    std::vector<size_t> skipAnalysisForArguments(const QueryTreeNodePtr &, ContextPtr) const override { return {1}; }

private:
    StoragePtr executeImpl(
            const ASTPtr & ast_function, ContextPtr context,
            const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "SQLite"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    String database_path;
    TableNameOrQuery external_table_or_query;
    std::shared_ptr<sqlite3> sqlite_db;
};

StoragePtr TableFunctionSQLite::executeImpl(const ASTPtr & /*ast_function*/,
        ContextPtr context, const String & table_name, ColumnsDescription cached_columns, bool /*is_insert_query*/) const
{
    auto storage = std::make_shared<StorageSQLite>(StorageID(getDatabaseName(), table_name),
                                         sqlite_db,
                                         database_path,
                                         external_table_or_query,
                                         cached_columns, ConstraintsDescription{}, /* comment = */ "", context);

    storage->startup();
    return storage;
}


ColumnsDescription TableFunctionSQLite::getActualTableStructure(ContextPtr /* context */, bool /*is_insert_query*/) const
{
    return StorageSQLite::getTableStructureFromData(sqlite_db, external_table_or_query);
}


void TableFunctionSQLite::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();

    if (!func_args.arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function 'sqlite' must have arguments.");

    ASTs & args = func_args.arguments->children;

    if (args.size() != 2)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function 'sqlite' requires 2 arguments: database path, table name (or select query)");

    std::optional<String> maybe_query;
    if (auto * subquery_ast = args[1]->as<ASTSubquery>())
    {
        maybe_query = subquery_ast->children[0]->formatWithSecretsOneLine();
    }
    else if (auto * function_ast = args[1]->as<ASTFunction>(); function_ast && function_ast->name == "query")
    {
        if (function_ast->arguments->children.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function 'sqlite' expects exactly one argument in query() function");

        auto evaluated_query_arg = evaluateConstantExpressionOrIdentifierAsLiteral(function_ast->arguments->children[0], context);
        auto * query_lit = evaluated_query_arg->as<ASTLiteral>();

        if (!query_lit || query_lit->value.getType() != Field::Types::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function 'sqlite' expects string literal inside query()");
        maybe_query = query_lit->value.safeGet<String>();
    }

    for (size_t i = 0; i < args.size(); ++i)
    {
        auto & arg = args[i];
        if (i == 1 && maybe_query)
            continue;
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);
    }

    database_path = checkAndGetLiteralArgument<String>(args[0], "database_path");

    if (maybe_query)
        external_table_or_query = TableNameOrQuery(TableNameOrQuery::Type::QUERY, *maybe_query);
    else
        external_table_or_query = TableNameOrQuery(TableNameOrQuery::Type::TABLE, checkAndGetLiteralArgument<String>(args[1], "table_name"));

    sqlite_db = openSQLiteDB(database_path, context);
}

}

void registerTableFunctionSQLite(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionSQLite>();
}

}

#endif
