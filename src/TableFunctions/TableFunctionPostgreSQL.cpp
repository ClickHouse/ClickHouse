#include "config.h"

#if USE_LIBPQXX

#include <TableFunctions/ITableFunction.h>
#include <Core/PostgreSQL/PoolWithFailover.h>
#include <Core/Settings.h>
#include <Storages/StoragePostgreSQL.h>
#include <Parsers/ASTFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/Exception.h>
#include <Common/parseRemoteDescription.h>
#include "registerTableFunctions.h"


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 postgresql_connection_attempt_timeout;
    extern const SettingsBool postgresql_connection_pool_auto_close_connection;
    extern const SettingsUInt64 postgresql_connection_pool_retries;
    extern const SettingsUInt64 postgresql_connection_pool_size;
    extern const SettingsUInt64 postgresql_connection_pool_wait_timeout;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

class TableFunctionPostgreSQL : public ITableFunction
{
public:
    static constexpr auto name = "postgresql";
    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(
            const ASTPtr & ast_function, ContextPtr context,
            const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "PostgreSQL"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    postgres::PoolWithFailoverPtr connection_pool;
    std::optional<StoragePostgreSQL::Configuration> configuration;
};

StoragePtr TableFunctionPostgreSQL::executeImpl(const ASTPtr & /*ast_function*/,
        ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool /*is_insert_query*/) const
{
    auto result = std::make_shared<StoragePostgreSQL>(
        StorageID(getDatabaseName(), table_name),
        connection_pool,
        configuration->table,
        cached_columns,
        ConstraintsDescription{},
        String{},
        context,
        configuration->schema,
        configuration->on_conflict);

    result->startup();
    return result;
}


ColumnsDescription TableFunctionPostgreSQL::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    return StoragePostgreSQL::getTableStructureFromData(connection_pool, configuration->table, configuration->schema, context);
}


void TableFunctionPostgreSQL::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function 'PostgreSQL' must have arguments.");

    configuration.emplace(StoragePostgreSQL::getConfiguration(func_args.arguments->children, context));
    const auto & settings = context->getSettingsRef();
    connection_pool = std::make_shared<postgres::PoolWithFailover>(
        *configuration,
        settings[Setting::postgresql_connection_pool_size],
        settings[Setting::postgresql_connection_pool_wait_timeout],
        settings[Setting::postgresql_connection_pool_retries],
        settings[Setting::postgresql_connection_pool_auto_close_connection],
        settings[Setting::postgresql_connection_attempt_timeout]);
}

}


void registerTableFunctionPostgreSQL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionPostgreSQL>();
}

}

#endif
