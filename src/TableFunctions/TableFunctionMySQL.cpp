#include "config.h"

#if USE_MYSQL

#include <Storages/StorageMySQL.h>

#include <Core/Settings.h>
#include <Processors/Sources/MySQLSource.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/MySQL/MySQLHelpers.h>
#include <Storages/MySQL/MySQLSettings.h>
#include <Storages/MySQL/StorageMySQLSelect.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/registerTableFunctions.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/Exception.h>
#include <Common/parseAddress.h>
#include <Common/quoteString.h>

#include <Databases/MySQL/DatabaseMySQL.h>
#include <Common/parseRemoteDescription.h>

#include <boost/algorithm/string/predicate.hpp>

#include <variant>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 external_storage_connect_timeout_sec;
    extern const SettingsUInt64 external_storage_rw_timeout_sec;
}

namespace MySQLSetting
{
    extern const MySQLSettingsUInt64 connect_timeout;
    extern const MySQLSettingsUInt64 read_write_timeout;
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
 *
 * Alternatively, instead of table name, one can pass a SELECT query.
 * In which case, the table structure is taken from the result of the SELECT query provided by MySQL API.
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
    // stores configuration and indicates which type of syntax was used (table vs SELECT)
    std::variant<StorageMySQL::Configuration, StorageMySQLSelect::Configuration> configuration;
};

void TableFunctionMySQL::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function 'mysql' must have arguments.");

    auto & args = args_func.arguments->children;

    MySQLSettings mysql_settings;

    const auto & settings = context->getSettingsRef();
    mysql_settings[MySQLSetting::connect_timeout] = settings[Setting::external_storage_connect_timeout_sec];
    mysql_settings[MySQLSetting::read_write_timeout] = settings[Setting::external_storage_rw_timeout_sec];

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
    // Check for `mysql('host:port', database, <SELECT query>, ...)` syntax
    if (args.size() > 2)
    {
        auto maybe_select_ast = args[2];
        if (auto * maybe_select_lit = maybe_select_ast->as<ASTLiteral>())
        {
            if (maybe_select_lit->value.getType() == Field::Types::String)
            {
                auto maybe_select = maybe_select_lit->value.safeGet<String>();
                if (boost::istarts_with(maybe_select, "SELECT"))
                {
                    auto select_cfg = StorageMySQLSelect::getConfiguration(args, context);
                    configuration = select_cfg;
                    pool.emplace(createMySQLPoolWithFailover(
                        select_cfg.database, select_cfg.addresses, select_cfg.username, select_cfg.password, mysql_settings));
                    return;
                }
            }
        }
    }

    configuration = StorageMySQL::getConfiguration(args, context, mysql_settings);
    pool.emplace(createMySQLPoolWithFailover(std::get<StorageMySQL::Configuration>(configuration), mysql_settings));
}

ColumnsDescription TableFunctionMySQL::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    if (std::holds_alternative<StorageMySQL::Configuration>(configuration)) {
        auto storage_cfg = std::get<StorageMySQL::Configuration>(configuration);
        return StorageMySQL::getTableStructureFromData(*pool, storage_cfg.database, storage_cfg.table, context);
    } else {
        auto select_cfg = std::get<StorageMySQLSelect::Configuration>(configuration);
        return StorageMySQLSelect::doQueryResultStructure(*pool, select_cfg.select_query, context);
    }
}

StoragePtr TableFunctionMySQL::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription cached_columns,
    bool /*is_insert_query*/) const
{
    if (std::holds_alternative<StorageMySQLSelect::Configuration>(configuration))
    {
        auto select_cfg = std::get<StorageMySQLSelect::Configuration>(configuration);
        auto res = std::make_shared<StorageMySQLSelect>(
            StorageID(getDatabaseName(), table_name),
            std::move(*pool),
            select_cfg.database,
            select_cfg.select_query,
            cached_columns,
            context,
            MySQLSettings{});
        pool.reset();
        res->startup();
        return res;
    }
    auto mysql_cfg = std::get<StorageMySQL::Configuration>(configuration);
    auto res = std::make_shared<StorageMySQL>(
        StorageID(getDatabaseName(), table_name),
        std::move(*pool),
        mysql_cfg.database,
        mysql_cfg.table,
        mysql_cfg.replace_query,
        mysql_cfg.on_duplicate_clause,
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
