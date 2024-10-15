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
#include "registerTableFunctions.h"

#include <Databases/MySQL/DatabaseMySQL.h>
#include <Common/parseRemoteDescription.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 external_storage_connect_timeout_sec;
    extern const SettingsUInt64 external_storage_rw_timeout_sec;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;
    const char * getStorageTypeName() const override { return "MySQL"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    mutable std::optional<mysqlxx::PoolWithFailover> pool;
    std::optional<StorageMySQL::Configuration> configuration;
};

void TableFunctionMySQL::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function 'mysql' must have arguments.");

    auto & args = args_func.arguments->children;

    MySQLSettings mysql_settings;

    const auto & settings = context->getSettingsRef();
    mysql_settings.connect_timeout = settings[Setting::external_storage_connect_timeout_sec];
    mysql_settings.read_write_timeout = settings[Setting::external_storage_rw_timeout_sec];

    for (auto * it = args.begin(); it != args.end(); ++it)
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
    pool.emplace(createMySQLPoolWithFailover(*configuration, mysql_settings));
}

ColumnsDescription TableFunctionMySQL::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    return StorageMySQL::getTableStructureFromData(*pool, configuration->database, configuration->table, context);
}

StoragePtr TableFunctionMySQL::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription cached_columns,
    bool /*is_insert_query*/) const
{
    auto res = std::make_shared<StorageMySQL>(
        StorageID(getDatabaseName(), table_name),
        std::move(*pool),
        configuration->database,
        configuration->table,
        configuration->replace_query,
        configuration->on_duplicate_clause,
        cached_columns,
        ConstraintsDescription{},
        String{},
        context,
        MySQLSettings{});

    pool.reset();

    res->startup();
    return res;
}

}


void registerTableFunctionMySQL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMySQL>();
}

}

#endif
