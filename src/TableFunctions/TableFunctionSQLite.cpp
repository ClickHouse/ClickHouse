#include <TableFunctions/TableFunctionSQLite.h>

#if USE_SQLITE

#include <Common/Exception.h>
#include <Common/quoteString.h>

#include "registerTableFunctions.h"

#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Databases/SQLite/fetchSQLiteTableStructure.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int SQLITE_ENGINE_ERROR;
}


StoragePtr TableFunctionSQLite::executeImpl(const ASTPtr & /*ast_function*/,
        ContextPtr context, const String & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);

    auto storage = StorageSQLite::create(StorageID(getDatabaseName(), table_name),
                                         sqlite_db,
                                         remote_table_name,
                                         columns, ConstraintsDescription{}, context);

    storage->startup();
    return storage;
}


ColumnsDescription TableFunctionSQLite::getActualTableStructure(ContextPtr /* context */) const
{
    auto columns = fetchSQLiteTableStructure(sqlite_db.get(), remote_table_name);

    if (!columns)
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR, "Failed to fetch table structure for {}", remote_table_name);

    return ColumnsDescription{*columns};
}


void TableFunctionSQLite::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();

    if (!func_args.arguments)
        throw Exception("Table function 'sqlite' must have arguments.", ErrorCodes::BAD_ARGUMENTS);

    ASTs & args = func_args.arguments->children;

    if (args.size() != 2)
        throw Exception("SQLite database requires 2 arguments: database path, table name",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    database_path = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    remote_table_name = args[1]->as<ASTLiteral &>().value.safeGet<String>();

    sqlite3 * tmp_sqlite_db = nullptr;
    int status = sqlite3_open(database_path.c_str(), &tmp_sqlite_db);
    if (status != SQLITE_OK)
        throw Exception(ErrorCodes::SQLITE_ENGINE_ERROR,
                        "Failed to open sqlite database. Status: {}. Message: {}",
                        status, sqlite3_errstr(status));

    sqlite_db = std::shared_ptr<sqlite3>(tmp_sqlite_db, sqlite3_close);
}


void registerTableFunctionSQLite(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionSQLite>();
}

}

#endif
