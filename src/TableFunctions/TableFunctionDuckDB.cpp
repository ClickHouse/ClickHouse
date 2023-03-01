#include <TableFunctions/TableFunctionDuckDB.h>

#if USE_DUCKDB

#include <Common/Exception.h>
#include <Common/quoteString.h>

#include <Databases/DuckDB/fetchDuckDBTableStructure.h>
#include <Databases/DuckDB/DuckDBUtils.h>
#include "registerTableFunctions.h"

#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTFunction.h>

#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>

#include <Storages/checkAndGetLiteralArgument.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int DUCKDB_ENGINE_ERROR;
}


StoragePtr TableFunctionDuckDB::executeImpl(const ASTPtr & /*ast_function*/,
        ContextPtr context, const String & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);

    auto storage = std::make_shared<StorageDuckDB>(StorageID(getDatabaseName(), table_name),
                                         duckdb_instance,
                                         database_path,
                                         remote_table_name,
                                         columns, ConstraintsDescription{}, context);

    storage->startup();
    return storage;
}


ColumnsDescription TableFunctionDuckDB::getActualTableStructure(ContextPtr /* context */) const
{
    auto columns = fetchDuckDBTableStructure(*duckdb_instance, remote_table_name);

    if (!columns)
        throw Exception(ErrorCodes::DUCKDB_ENGINE_ERROR, "Failed to fetch table structure for {}", remote_table_name);

    return ColumnsDescription{*columns};
}


void TableFunctionDuckDB::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();

    if (!func_args.arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function 'duckdb' must have arguments.");

    ASTs & args = func_args.arguments->children;

    if (args.size() != 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "DuckDB database requires 2 arguments: database path, table name");

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    database_path = checkAndGetLiteralArgument<String>(args[0], "database_path");
    remote_table_name = checkAndGetLiteralArgument<String>(args[1], "table_name");

    duckdb_instance = openDuckDB(database_path, context);
}


void registerTableFunctionDuckDB(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionDuckDB>(
        {.documentation
         = {R"(The table function can be used to read the table from DuckDB database.)",
            Documentation::Examples{{"duckdb", "SELECT * FROM duckdb(database_path, table_name)"}},
            Documentation::Categories{"DuckDB"}},
         .allow_readonly = false});
}

}

#endif
