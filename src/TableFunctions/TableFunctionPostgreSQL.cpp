#include <TableFunctions/TableFunctionPostgreSQL.h>

#if USE_LIBPQXX
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>
#include <Storages/StoragePostgreSQL.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/Exception.h>
#include <Common/parseAddress.h>
#include "registerTableFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}


StoragePtr TableFunctionPostgreSQL::executeImpl(const ASTPtr & /*ast_function*/,
        const Context & context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);
    auto result = std::make_shared<StoragePostgreSQL>(
            StorageID(getDatabaseName(), table_name), remote_table_name,
            connection, columns, ConstraintsDescription{}, context);

    result->startup();
    return result;
}


ColumnsDescription TableFunctionPostgreSQL::getActualTableStructure(const Context & context) const
{
    const bool use_nulls = context.getSettingsRef().external_table_functions_use_nulls;
    auto columns = fetchPostgreSQLTableStructure(connection->conn(), remote_table_name, use_nulls).columns;

    return ColumnsDescription{*columns};
}


void TableFunctionPostgreSQL::parseArguments(const ASTPtr & ast_function, const Context & context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();

    if (!func_args.arguments)
        throw Exception("Table function 'PostgreSQL' must have arguments.", ErrorCodes::BAD_ARGUMENTS);

    ASTs & args = func_args.arguments->children;

    if (args.size() != 5)
        throw Exception("Table function 'PostgreSQL' requires 5 parameters: "
                        "PostgreSQL('host:port', 'database', 'table', 'user', 'password').",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    auto parsed_host_port = parseAddress(args[0]->as<ASTLiteral &>().value.safeGet<String>(), 5432);
    remote_table_name = args[2]->as<ASTLiteral &>().value.safeGet<String>();

    connection = std::make_shared<PostgreSQLConnection>(
        args[1]->as<ASTLiteral &>().value.safeGet<String>(),
        parsed_host_port.first,
        parsed_host_port.second,
        args[3]->as<ASTLiteral &>().value.safeGet<String>(),
        args[4]->as<ASTLiteral &>().value.safeGet<String>());
}


void registerTableFunctionPostgreSQL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionPostgreSQL>();
}

}

#endif
