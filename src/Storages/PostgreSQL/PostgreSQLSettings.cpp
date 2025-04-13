#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Core/Settings.h>
#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTFunction.h>
#include <Storages/PostgreSQL/PostgreSQLSettings.h>
#include <Common/NamedCollections/NamedCollections.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

#define LIST_OF_POSTGRESQL_SETTINGS(DECLARE, ALIAS) \
    DECLARE(UInt64, connection_pool_size, 16, "Size of connection pool (if all connections are in use, the query will wait until some connection will be freed).", 0) \
    DECLARE(UInt64, connection_max_tries, 3, "Number of retries for pool with failover", 0) \
    DECLARE(UInt64, connection_wait_timeout, 5, "Timeout (in seconds) for waiting for free connection (in case of there is already connection_pool_size active connections), 0 - do not wait.", 0) \
    DECLARE(Bool, connection_auto_close, true, "Auto-close connection after query execution, i.e. disable connection reuse.", 0)                                      \
    DECLARE(UInt64, connect_timeout, DBMS_DEFAULT_CONNECT_TIMEOUT_SEC, "Connect timeout (in seconds)", 0) \
    DECLARE(UInt64, read_write_timeout, DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, "Read/write timeout (in seconds)", 0)                                                       \

DECLARE_SETTINGS_TRAITS(PostgreSQLSettingsTraits, LIST_OF_POSTGRESQL_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(PostgreSQLSettingsTraits, LIST_OF_POSTGRESQL_SETTINGS)

struct PostgreSQLSettingsImpl : public BaseSettings<PostgreSQLSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) PostgreSQLSettings##TYPE NAME = &PostgreSQLSettingsImpl ::NAME;

namespace PostgreSQLSetting
{
LIST_OF_POSTGRESQL_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

PostgreSQLSettings::PostgreSQLSettings() : impl(std::make_unique<PostgreSQLSettingsImpl>())
{
}

PostgreSQLSettings::PostgreSQLSettings(const PostgreSQLSettings & settings) : impl(std::make_unique<PostgreSQLSettingsImpl>(*settings.impl))
{
}

PostgreSQLSettings::PostgreSQLSettings(PostgreSQLSettings && settings) noexcept : impl(std::make_unique<PostgreSQLSettingsImpl>(std::move(*settings.impl)))
{
}

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
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }
}

std::vector<std::string_view> PostgreSQLSettings::getAllRegisteredNames() const
{
    std::vector<std::string_view> all_settings;
    for (const auto & setting_field : impl->all())
        all_settings.push_back(setting_field.getName());
    return all_settings;
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

bool PostgreSQLSettings::hasBuiltin(std::string_view name)
{
    return PostgreSQLSettingsImpl::hasBuiltin(name);
}
}
