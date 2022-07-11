#include "config_core.h"

#if USE_MYSQL
#include <Databases/MySQL/FetchTablesColumnsList.h>
#include <Processors/Sources/MySQLSource.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Storages/StorageMySQL.h>
#include <Storages/MySQL/MySQLSettings.h>
#include <Storages/MySQL/MySQLHelpers.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionMySQL.h>
#include <Common/Exception.h>
#include <Common/parseAddress.h>
#include <Common/quoteString.h>
#include "registerTableFunctions.h"

#include <Databases/MySQL/DatabaseMySQL.h> // for fetchTablesColumnsList
#include <Common/parseRemoteDescription.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TABLE;
}

void TableFunctionMySQL::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception("Table function 'mysql' must have arguments.", ErrorCodes::LOGICAL_ERROR);

    MySQLSettings mysql_settings;
    configuration = StorageMySQL::getConfiguration(args_func.arguments->children, context, mysql_settings);
    const auto & settings = context->getSettingsRef();
    mysql_settings.connect_timeout = settings.external_storage_connect_timeout_sec;
    mysql_settings.read_write_timeout = settings.external_storage_rw_timeout_sec;
    pool.emplace(createMySQLPoolWithFailover(*configuration, mysql_settings));
}

ColumnsDescription TableFunctionMySQL::getActualTableStructure(ContextPtr context) const
{
    const auto & settings = context->getSettingsRef();
    const auto tables_and_columns = fetchTablesColumnsList(*pool, configuration->database, {configuration->table}, settings, settings.mysql_datatypes_support_level);

    const auto columns = tables_and_columns.find(configuration->table);
    if (columns == tables_and_columns.end())
        throw Exception("MySQL table " + (configuration->database.empty() ? "" : (backQuote(configuration->database) + "."))
            + backQuote(configuration->table) + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

    return columns->second;
}

StoragePtr TableFunctionMySQL::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription /*cached_columns*/) const
{
    auto columns = getActualTableStructure(context);

    auto res = StorageMySQL::create(
        StorageID(getDatabaseName(), table_name),
        std::move(*pool),
        configuration->database,
        configuration->table,
        configuration->replace_query,
        configuration->on_duplicate_clause,
        columns,
        ConstraintsDescription{},
        String{},
        context,
        MySQLSettings{});

    pool.reset();

    res->startup();
    return res;
}


void registerTableFunctionMySQL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMySQL>();
}
}

#endif
