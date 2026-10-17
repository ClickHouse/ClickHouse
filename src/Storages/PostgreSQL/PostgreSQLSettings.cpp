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

void PostgreSQLSettings::loadFromQueryContext(const Context & context)
{
    loadFromQueryContextImpl(context, nullptr, false);
}

void PostgreSQLSettings::loadFromQueryContext(const Context & context, ASTStorage & storage_def, bool store_values)
{
    loadFromQueryContextImpl(context, &storage_def, store_values);
}

void PostgreSQLSettings::loadFromQueryContextImpl(const Context & context, ASTStorage * storage_def, bool store_values)
{
    const Settings & settings = context.getSettingsRef();
    const auto * reset_settings = storage_def && storage_def->settings ? &storage_def->settings->default_settings : nullptr;

    for (const auto & setting : impl->all())
    {
        const auto & name = setting.getName();
        /// Set by a named collection or the SETTINGS clause, or reset there with `name = DEFAULT`.
        if (setting.isValueChanged() || (reset_settings && std::ranges::contains(*reset_settings, name)))
            continue;

        /// Not changed yet, so it holds the default of the engine.
        const Field default_value = setting.getValue();
        Field value = settings.get(name);
        impl->set(name, value);

        if (!store_values || value == default_value)
            continue;

        if (!storage_def->settings)
        {
            auto settings_ast = make_intrusive<ASTSetQuery>();
            settings_ast->is_standalone = false;
            storage_def->set(storage_def->settings, settings_ast);
        }
        storage_def->settings->changes.emplace_back(name, std::move(value));
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
