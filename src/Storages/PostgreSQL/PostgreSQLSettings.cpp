#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/PostgreSQL/PostgreSQLSettings.h>
#include <Common/Exception.h>
#include <Common/NamedCollections/NamedCollections.h>

#include <algorithm>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

/// The setting names deliberately match the query-level `postgresql_*` settings, so that the existing
/// behaviour (taking the pool parameters from the query context) and the new per-table SETTINGS clause
/// use the same, already-documented names.
#define LIST_OF_POSTGRESQL_SETTINGS(DECLARE, ALIAS) \
    DECLARE(UInt64, postgresql_connection_pool_size, 16, "Connection pool size for PostgreSQL table engine and database engine.", 0) \
    DECLARE(UInt64, postgresql_connection_pool_wait_timeout, 5000, "Connection pool push/pop timeout on empty pool for PostgreSQL table engine and database engine. By default it will block on empty pool.", 0) \
    DECLARE(UInt64, postgresql_connection_pool_retries, 2, "Connection pool push/pop retries number for PostgreSQL table engine and database engine.", 0) \
    DECLARE(Bool, postgresql_connection_pool_auto_close_connection, false, "Close connection before returning connection to the pool.", 0) \
    DECLARE(UInt64, postgresql_connection_attempt_timeout, 2, "Connection timeout in seconds of a single attempt to connect PostgreSQL end-point. The value is passed as a `connect_timeout` parameter of the connection URL.", 0) \

DECLARE_SETTINGS_TRAITS(PostgreSQLSettingsTraits, LIST_OF_POSTGRESQL_SETTINGS, POSTGRESQL_SETTINGS_SUPPORTED_TYPES)
IMPLEMENT_SETTINGS_TRAITS(PostgreSQLSettingsTraits, LIST_OF_POSTGRESQL_SETTINGS, PostgreSQLSettings, PostgreSQLSetting)

PostgreSQLSettings::PostgreSQLSettings() : impl(std::make_unique<PostgreSQLSettingsImpl>())
{
}

PostgreSQLSettings::PostgreSQLSettings(const PostgreSQLSettings & settings) : impl(std::make_unique<PostgreSQLSettingsImpl>(*settings.impl))
{
}

PostgreSQLSettings::PostgreSQLSettings(PostgreSQLSettings && settings) noexcept = default;

PostgreSQLSettings::~PostgreSQLSettings() = default;

POSTGRESQL_SETTINGS_SUPPORTED_TYPES(PostgreSQLSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)


void PostgreSQLSettings::loadFromQuery(const ASTSetQuery & settings_def)
{
    impl->applyChanges(settings_def.changes);
}

void PostgreSQLSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            loadFromQuery(*storage_def.settings);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                e.addMessage("for storage " + storage_def.engine->name);
            throw;
        }
    }
}

void PostgreSQLSettings::loadFromQueryContext(const Context & context, const ASTSetQuery * settings, ASTStorage * store_into)
{
    const Settings & query_settings = context.getSettingsRef();
    /// The values a table takes when it is attached again in the global context, e.g. on server startup.
    const Settings * server_settings = store_into ? &context.getGlobalContext()->getSettingsRef() : nullptr;

    for (const auto & setting : impl->all())
    {
        const auto & name = setting.getName();
        if (setting.isValueChanged() || (settings && std::ranges::contains(settings->default_settings, name)))
            continue;

        /// A setting of the engine only, with no query-level counterpart to take the value from.
        if (!query_settings.has(name))
            continue;

        Field value = query_settings.get(name);
        impl->set(name, value);

        /// A value equal to the server's is not stored: the table takes it again when it is attached. This depends only on the
        /// value and the server's settings, not on whether the session changes it: in a `Replicated` database another replica
        /// drops a forwarded setting equal to its own value, which keeps the value but loses that the session changed it. So
        /// replicas with the same configuration store the same definition, and none stores a value the query does not set.
        if (!store_into || value == server_settings->get(name))
            continue;

        if (!store_into->settings)
        {
            auto settings_ast = make_intrusive<ASTSetQuery>();
            settings_ast->is_standalone = false;
            store_into->set(store_into->settings, settings_ast);
        }
        store_into->settings->changes.emplace_back(name, std::move(value));
    }
}

void PostgreSQLSettings::loadFromNamedCollection(const NamedCollection & named_collection)
{
    for (const auto & setting : impl->all())
    {
        const auto & setting_name = setting.getName();
        if (named_collection.has(setting_name))
            impl->set(setting_name, named_collection.get<String>(setting_name));
    }
}

VectorWithMemoryTracking<std::string_view> PostgreSQLSettings::getAllRegisteredNames() const
{
    VectorWithMemoryTracking<std::string_view> all_settings;
    for (const auto & setting_field : impl->all())
        all_settings.push_back(setting_field.getName());
    return all_settings;
}

bool PostgreSQLSettings::hasBuiltin(std::string_view name)
{
    return PostgreSQLSettingsImpl::hasBuiltin(name);
}

}
