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
#include <TableFunctions/registerTableFunctions.h>

#include <Databases/MySQL/DatabaseMySQL.h>
#include <Common/parseRemoteDescription.h>


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 external_storage_connect_timeout_sec;
    extern const SettingsUInt64 external_storage_rw_timeout_sec;
    extern const SettingsMySQLDataTypesSupport mysql_datatypes_support_level;
}

namespace MySQLSetting
{
    extern const MySQLSettingsUInt64 connect_timeout;
    extern const MySQLSettingsUInt64 read_write_timeout;
    extern const MySQLSettingsMySQLDataTypesSupport mysql_datatypes_support_level;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
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

    /// The 3rd argument may be a query passed to MySQL as is - a subquery `(SELECT ...)` or `query('SELECT ...')`.
    /// Such an argument must not be analyzed as an ordinary expression.
    VectorWithMemoryTracking<size_t> skipAnalysisForArguments(const QueryTreeNodePtr &, ContextPtr) const override { return {2}; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;
    const char * getStorageEngineName() const override { return "MySQL"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    mutable std::optional<mysqlxx::PoolWithFailover> pool;
    std::optional<StorageMySQL::Configuration> configuration;

    /// The effective settings for this `mysql(...)` call, with `mysql_datatypes_support_level` set to
    /// the query-context value overridden by a function-local `SETTINGS` clause or named collection.
    /// The type-mapping level must be used during schema inference instead of the (default) engine
    /// settings, otherwise an opt-out passed to the table function would be silently ignored.
    std::optional<MySQLSettings> effective_settings;
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

    /// Seed the type-mapping level from the query context so that it is the default for schema
    /// inference. A function-local `SETTINGS` clause (below) or named collection (in
    /// `getConfiguration`) overrides it.
    mysql_settings[MySQLSetting::mysql_datatypes_support_level] = settings[Setting::mysql_datatypes_support_level];

    for (auto it = args.begin(); it != args.end(); ++it)
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
    effective_settings.emplace(mysql_settings);
    pool.emplace(createMySQLPoolWithFailover(*configuration, mysql_settings));
}

ColumnsDescription TableFunctionMySQL::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    /// A query-backed insert is rejected in executeImpl, which is the only path taken by INSERT INTO TABLE
    /// FUNCTION (it is called with empty cached columns, before any external contact). It must not be rejected
    /// here, because DESCRIBE TABLE also calls getActualTableStructure with is_insert_query = true and must
    /// keep returning the inferred structure.
    ///
    /// Use the effective type-mapping level computed in `parseArguments` (the query-context value,
    /// overridden by a function-local `SETTINGS` clause or named collection).
    return StorageMySQL::getTableStructureFromData(
        *pool, configuration->database, configuration->table_or_query, context,
        (*effective_settings)[MySQLSetting::mysql_datatypes_support_level]);
}

StoragePtr TableFunctionMySQL::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const std::string & table_name,
    ColumnsDescription cached_columns,
    bool is_insert_query) const
{
    /// Reject the insert before constructing the storage, so that read-only query-backed sources do not contact
    /// the external database for schema inference (which could run an expensive or volatile query) only to fail.
    if (is_insert_query && configuration->table_or_query.isQuery())
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Cannot INSERT into the 'mysql' table function: it represents the result of a query passed to MySQL, which is read-only");

    /// Carry the effective type-mapping level so that, when the columns are not provided and
    /// `StorageMySQL` infers them itself, it honors the same level as `getActualTableStructure`.
    MySQLSettings mysql_settings;
    mysql_settings[MySQLSetting::mysql_datatypes_support_level]
        = (*effective_settings)[MySQLSetting::mysql_datatypes_support_level];


    auto res = std::make_shared<StorageMySQL>(
        StorageID(getDatabaseName(), table_name),
        std::move(*pool),
        configuration->database,
        configuration->table_or_query,
        configuration->replace_query,
        configuration->on_duplicate_clause,
        cached_columns,
        ConstraintsDescription{},
        String{},
        context,
        mysql_settings);

    pool.reset();

    res->startup();
    return res;
}

}


void registerTableFunctionMySQL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMySQL>({});
}

}

#endif
