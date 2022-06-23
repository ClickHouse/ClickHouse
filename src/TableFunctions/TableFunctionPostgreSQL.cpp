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
#include <Common/quoteString.h>
#include <Common/parseRemoteDescription.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}


StoragePtr TableFunctionPostgreSQL::executeImpl(const ASTPtr & /*ast_function*/,
        ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);
    auto result = std::make_shared<StoragePostgreSQL>(
        StorageID(getDatabaseName(), table_name),
        connection_pool,
        remote_table_name,
        columns,
        ConstraintsDescription{},
        String{},
        remote_table_schema);

    result->startup();
    return result;
}


ColumnsDescription TableFunctionPostgreSQL::getActualTableStructure(ContextPtr context) const
{
    const bool use_nulls = context->getSettingsRef().external_table_functions_use_nulls;
    auto connection_holder = connection_pool->get();
    auto columns = fetchPostgreSQLTableStructure(
            connection_holder->get(), remote_table_name, remote_table_schema, use_nulls).columns;

    if (!columns)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table structure not returned");
    return ColumnsDescription{*columns};
}


void TableFunctionPostgreSQL::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();

    if (!func_args.arguments)
        throw Exception("Table function 'PostgreSQL' must have arguments.", ErrorCodes::BAD_ARGUMENTS);

    ASTs & args = func_args.arguments->children;

    if (args.size() < 5 || args.size() > 6)
        throw Exception("Table function 'PostgreSQL' requires from 5 to 6 parameters: "
                        "PostgreSQL('host:port', 'database', 'table', 'user', 'password', [, 'schema']).",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    /// Split into replicas if needed. 5432 is a default postgresql port.
    const auto & host_port = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    size_t max_addresses = context->getSettingsRef().glob_expansion_max_elements;
    auto addresses = parseRemoteDescriptionForExternalDatabase(host_port, max_addresses, 5432);

    remote_table_name = args[2]->as<ASTLiteral &>().value.safeGet<String>();

    if (args.size() == 6)
        remote_table_schema = args[5]->as<ASTLiteral &>().value.safeGet<String>();

    connection_pool = std::make_shared<postgres::PoolWithFailover>(
        args[1]->as<ASTLiteral &>().value.safeGet<String>(),
        addresses,
        args[3]->as<ASTLiteral &>().value.safeGet<String>(),
        args[4]->as<ASTLiteral &>().value.safeGet<String>());
}


void registerTableFunctionPostgreSQL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionPostgreSQL>();
}

}

#endif
