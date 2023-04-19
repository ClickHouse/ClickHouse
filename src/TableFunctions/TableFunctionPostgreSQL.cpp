#include <TableFunctions/TableFunctionPostgreSQL.h>

#if USE_LIBPQXX
#include <Databases/PostgreSQL/fetchPostgreSQLTableStructure.h>
#include <Storages/StoragePostgreSQL.h>

#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/Exception.h>
#include "registerTableFunctions.h"
#include <Common/parseRemoteDescription.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


StoragePtr TableFunctionPostgreSQL::executeImpl(const ASTPtr & /*ast_function*/,
        ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);
    auto result = std::make_shared<StoragePostgreSQL>(
        StorageID(getDatabaseName(), table_name),
        connection_pool,
        configuration->table,
        columns,
        ConstraintsDescription{},
        String{},
        configuration->schema,
        configuration->on_conflict);

    result->startup();
    return result;
}


ColumnsDescription TableFunctionPostgreSQL::getActualTableStructure(ContextPtr context) const
{
    const bool use_nulls = context->getSettingsRef().external_table_functions_use_nulls;
    auto connection_holder = connection_pool->get();
    auto columns_info = fetchPostgreSQLTableStructure(
            connection_holder->get(), configuration->table, configuration->schema, use_nulls).physical_columns;

    if (!columns_info)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table structure not returned");

    return ColumnsDescription{columns_info->columns};
}


void TableFunctionPostgreSQL::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception("Table function 'PostgreSQL' must have arguments.", ErrorCodes::BAD_ARGUMENTS);

    configuration.emplace(StoragePostgreSQL::getConfiguration(func_args.arguments->children, context));
    const auto & settings = context->getSettingsRef();
    connection_pool = std::make_shared<postgres::PoolWithFailover>(
        *configuration,
        settings.postgresql_connection_pool_size,
        settings.postgresql_connection_pool_wait_timeout,
        POSTGRESQL_POOL_WITH_FAILOVER_DEFAULT_MAX_TRIES,
        settings.postgresql_connection_pool_auto_close_connection);
}


void registerTableFunctionPostgreSQL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionPostgreSQL>();
}

}

#endif
