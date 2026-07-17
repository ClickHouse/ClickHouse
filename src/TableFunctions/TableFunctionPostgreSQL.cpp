#include "config.h"

#if USE_LIBPQXX

#include <TableFunctions/ITableFunction.h>
#include <Core/PostgreSQL/PoolWithFailover.h>
#include <Storages/StoragePostgreSQL.h>
#include <Storages/PostgreSQL/PostgreSQLSettings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Common/Exception.h>
#include <TableFunctions/registerTableFunctions.h>


namespace DB
{
namespace PostgreSQLSetting
{
    extern const PostgreSQLSettingsUInt64 postgresql_connection_pool_size;
    extern const PostgreSQLSettingsUInt64 postgresql_connection_pool_wait_timeout;
    extern const PostgreSQLSettingsUInt64 postgresql_connection_pool_retries;
    extern const PostgreSQLSettingsBool postgresql_connection_pool_auto_close_connection;
    extern const PostgreSQLSettingsUInt64 postgresql_connection_attempt_timeout;
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

    /// The connection-pool parameters are seeded from the query-level `postgresql_*` settings
    /// (preserving the historical behaviour); a named collection may override them, and a trailing
    /// `SETTINGS ...` clause takes the final precedence, like on the table engine.
    PostgreSQLSettings postgresql_settings;
    postgresql_settings.loadFromQueryContext(*context);

    auto & args = func_args.arguments->children;
    ASTPtr settings_ast;
    for (auto it = args.begin(); it != args.end(); ++it)
    {
        if ((*it)->as<ASTSetQuery>())
        {
            settings_ast = *it;
            args.erase(it);
            break;
        }
    }

    configuration.emplace(StoragePostgreSQL::getConfiguration(args, context, &postgresql_settings));

    /// Applied after getConfiguration, so that the explicit SETTINGS clause wins over the values
    /// stored in a named collection.
    if (settings_ast)
        postgresql_settings.loadFromQuery(settings_ast->as<ASTSetQuery &>());

    if (!postgresql_settings[PostgreSQLSetting::postgresql_connection_pool_size])
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "postgresql_connection_pool_size cannot be zero.");

    connection_pool = std::make_shared<postgres::PoolWithFailover>(
        *configuration,
        postgresql_settings[PostgreSQLSetting::postgresql_connection_pool_size],
        postgresql_settings[PostgreSQLSetting::postgresql_connection_pool_wait_timeout],
        postgresql_settings[PostgreSQLSetting::postgresql_connection_pool_retries],
        postgresql_settings[PostgreSQLSetting::postgresql_connection_pool_auto_close_connection],
        postgresql_settings[PostgreSQLSetting::postgresql_connection_attempt_timeout]);
}

}


void registerTableFunctionPostgreSQL(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionPostgreSQL>({});
}

}

#endif
