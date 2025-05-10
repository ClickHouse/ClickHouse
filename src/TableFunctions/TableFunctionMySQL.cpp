#include "config.h"

#if USE_MYSQL

#include <Storages/StorageMySQL.h>

#include <Core/Settings.h>
#include <Processors/Sources/MySQLSource.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <Storages/MySQL/MySQLHelpers.h>
#include <Storages/MySQL/MySQLSettings.h>
#include <Storages/MySQL/StorageMySQLSelect.h>
#include <Storages/NamedCollectionsHelpers.h>
#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/registerTableFunctions.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/Exception.h>
#include <Common/parseAddress.h>
#include <Common/quoteString.h>

#include <Databases/MySQL/DatabaseMySQL.h>
#include <Common/parseRemoteDescription.h>

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
    extern const int BAD_ARGUMENTS;
}

namespace
{

/* mysql ('host:port', database, table, user, password) - creates a temporary StorageMySQL.
 * The structure of the table is taken from the mysql query DESCRIBE table.
 * If there is no such table, an exception is thrown.
 *
 * Alternatively, instead of table, one can pass a SELECT query. Either as (SELECT x FROM y...) or as query('SELECT x FROM y...').
 * In which case, the table structure is taken from the query execution result provided by MySQL API.
 */
class TableFunctionMySQL : public ITableFunction
{
public:
    static constexpr auto name = "mysql";
    std::string getName() const override
    {
        return name;
    }

    std::vector<size_t> skipAnalysisForArguments(const QueryTreeNodePtr &, ContextPtr) const override { return {2}; }

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
    // Check for named collection args with `query` option -> user's query syntax
    if (auto named_collection = tryGetNamedCollectionWithOverrides(args, context); named_collection && named_collection->has("query"))
    {
        auto select_cfg = StorageMySQLSelect::getConfiguration(args, context, mysql_settings);
        configuration = select_cfg;
        pool.emplace(createMySQLPoolWithFailover(
            select_cfg.database, select_cfg.addresses, select_cfg.username, select_cfg.password, "", "", "", mysql_settings));
        return;
    }
    bool pass_select_query = false;
    // Check for `mysql('host:port', database, <SELECT query>, ...)` syntax and put <SELECT query> as string literal
    if (args.size() > 2)
    {
        // Check if it's in a form `mysql(host:port, db, (SELECT ...), ...)`, that is, an unquoted SELECT query
        if (auto * maybe_select_ast = args[2]->as<ASTSubquery>())
        {
            args[2] = std::make_shared<ASTLiteral>(maybe_select_ast->formatWithSecretsOneLine());
            pass_select_query = true;
        }
        // Check if it's in a form `mysql(host:port, db, query('SELECT ...'), ...)`
        else if (auto * func_wrapped_query = args[2]->as<ASTFunction>(); func_wrapped_query && func_wrapped_query->name == "query")
        {
            if (func_wrapped_query->arguments->children.size() != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function 'mysql' with query(...) must have 1 argument in it.");

            auto * maybe_query_literal = func_wrapped_query->arguments->children[0]->as<ASTLiteral>();
            if (!maybe_query_literal || maybe_query_literal->value.getType() != Field::Types::String)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Table function 'mysql' with query(...) expects a string literal as its argument.");

            args[2] = func_wrapped_query->arguments->children[0];
            pass_select_query = true;
        }
    }
    if (pass_select_query)
    {
        auto select_cfg = StorageMySQLSelect::getConfiguration(args, context, mysql_settings);
        configuration = select_cfg;
        pool.emplace(createMySQLPoolWithFailover(
            select_cfg.database, select_cfg.addresses, select_cfg.username, select_cfg.password, "", "", "", mysql_settings));
        return;
    }

    configuration = StorageMySQL::getConfiguration(args, context, mysql_settings);
    pool.emplace(createMySQLPoolWithFailover(std::get<StorageMySQL::Configuration>(configuration), mysql_settings));
}

ColumnsDescription TableFunctionMySQL::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    if (std::holds_alternative<StorageMySQL::Configuration>(configuration))
    {
        auto storage_cfg = std::get<StorageMySQL::Configuration>(configuration);
        return StorageMySQL::getTableStructureFromData(*pool, storage_cfg.database, storage_cfg.table, context);
    }
    else
    {
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
            /*comment=*/String{},
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
