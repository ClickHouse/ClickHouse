#include "config.h"

#if USE_LIBPQXX

#include <TableFunctions/ITableFunction.h>
#include <Core/PostgreSQL/PoolWithFailover.h>
#include <Core/Settings.h>
#include <Storages/StoragePostgreSQL.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/Exception.h>
#include <TableFunctions/registerTableFunctions.h>


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
    extern const int INCORRECT_QUERY;
}

namespace
{

class TableFunctionPostgreSQL : public ITableFunction
{
public:
    static constexpr auto name = "postgresql";
    std::string getName() const override { return name; }

    /// The 3rd argument may be a query passed to PostgreSQL as is - a subquery `(SELECT ...)` or `query('SELECT ...')`.
    /// Such an argument must not be analyzed as an ordinary expression.
    VectorWithMemoryTracking<size_t> skipAnalysisForArguments(const QueryTreeNodePtr &, ContextPtr) const override { return {2}; }

private:
    StoragePtr executeImpl(
            const ASTPtr & ast_function, ContextPtr context,
            const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return "PostgreSQL"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    postgres::PoolWithFailoverPtr connection_pool;
    std::optional<StoragePostgreSQL::Configuration> configuration;
};

StoragePtr TableFunctionPostgreSQL::executeImpl(const ASTPtr & /*ast_function*/,
        ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const
{
    /// Reject the insert before constructing the storage, so that read-only query-backed sources do not contact
    /// the external database for schema inference (which could run an expensive or volatile query) only to fail.
    if (is_insert_query && configuration->table_or_query.isQuery())
        throw Exception(ErrorCodes::INCORRECT_QUERY,
            "Cannot INSERT into the 'postgresql' table function: it represents the result of a query passed to PostgreSQL, which is read-only");

    auto result = std::make_shared<StoragePostgreSQL>(
        StorageID(getDatabaseName(), table_name),
        connection_pool,
        configuration->table_or_query,
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
    /// A query-backed insert is rejected in executeImpl, which is the only path taken by INSERT INTO TABLE
    /// FUNCTION (it is called with empty cached columns, before any external contact). It must not be rejected
    /// here, because DESCRIBE TABLE also calls getActualTableStructure with is_insert_query = true and must
    /// keep returning the inferred structure.
    return StoragePostgreSQL::getTableStructureFromData(connection_pool, configuration->table_or_query, configuration->schema, context);
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
    factory.registerFunction<TableFunctionPostgreSQL>({});
}

}

#endif
