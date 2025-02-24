#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/MySQL/MySQLSettings.h>
#include <Common/Exception.h>
#include <Common/NamedCollections/NamedCollections.h>


namespace DB
{
namespace Setting
{
    extern const SettingsMySQLDataTypesSupport mysql_datatypes_support_level;
}

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

#define LIST_OF_MYSQL_SETTINGS(DECLARE, ALIAS) \
    DECLARE(UInt64, connection_pool_size, 16, "Size of connection pool (if all connections are in use, the query will wait until some connection will be freed).", 0) \
    DECLARE(UInt64, connection_max_tries, 3, "Number of retries for pool with failover", 0) \
    DECLARE(UInt64, connection_wait_timeout, 5, "Timeout (in seconds) for waiting for free connection (in case of there is already connection_pool_size active connections), 0 - do not wait.", 0) \
    DECLARE(Bool, connection_auto_close, true, "Auto-close connection after query execution, i.e. disable connection reuse.", 0) \
    DECLARE(UInt64, connect_timeout, DBMS_DEFAULT_CONNECT_TIMEOUT_SEC, "Connect timeout (in seconds)", 0) \
    DECLARE(UInt64, read_write_timeout, DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, "Read/write timeout (in seconds)", 0) \
    DECLARE(MySQLDataTypesSupport, mysql_datatypes_support_level, MySQLDataTypesSupportList{}, "Which MySQL types should be converted to corresponding ClickHouse types (rather than being represented as String). Can be empty or any combination of 'decimal' or 'datetime64'. When empty MySQL's DECIMAL and DATETIME/TIMESTAMP with non-zero precision are seen as String on ClickHouse's side.", 0) \

DECLARE_SETTINGS_TRAITS(MySQLSettingsTraits, LIST_OF_MYSQL_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(MySQLSettingsTraits, LIST_OF_MYSQL_SETTINGS)

struct MySQLSettingsImpl : public BaseSettings<MySQLSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) MySQLSettings##TYPE NAME = &MySQLSettingsImpl ::NAME;

namespace MySQLSetting
{
LIST_OF_MYSQL_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

MySQLSettings::MySQLSettings() : impl(std::make_unique<MySQLSettingsImpl>())
{
}

MySQLSettings::MySQLSettings(const MySQLSettings & settings) : impl(std::make_unique<MySQLSettingsImpl>(*settings.impl))
{
}

MySQLSettings::MySQLSettings(MySQLSettings && settings) noexcept : impl(std::make_unique<MySQLSettingsImpl>(std::move(*settings.impl)))
{
}

MySQLSettings::~MySQLSettings() = default;

MYSQL_SETTINGS_SUPPORTED_TYPES(MySQLSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)


void MySQLSettings::loadFromQuery(const ASTSetQuery & settings_def)
{
    impl->applyChanges(settings_def.changes);
}

void MySQLSettings::loadFromQuery(ASTStorage & storage_def)
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

void MySQLSettings::loadFromQueryContext(ContextPtr context, ASTStorage & storage_def)
{
    if (!context->hasQueryContext())
        return;

    const Settings & settings = context->getQueryContext()->getSettingsRef();

    if (settings[Setting::mysql_datatypes_support_level].value != impl->mysql_datatypes_support_level.value)
    {
        static constexpr auto setting_name = "mysql_datatypes_support_level";
        impl->mysql_datatypes_support_level = settings[Setting::mysql_datatypes_support_level];

        if (!storage_def.settings)
        {
            auto settings_ast = std::make_shared<ASTSetQuery>();
            settings_ast->is_standalone = false;
            storage_def.set(storage_def.settings, settings_ast);
        }

        auto & changes = storage_def.settings->changes;
        if (changes.end() == std::find_if(
                changes.begin(), changes.end(),
                [](const SettingChange & c) { return c.name == setting_name; }))
        {
            changes.push_back(SettingChange{setting_name, settings[Setting::mysql_datatypes_support_level].toString()});
        }
    }
}

std::vector<std::string_view> MySQLSettings::getAllRegisteredNames() const
{
    std::vector<std::string_view> all_settings;
    for (const auto & setting_field : impl->all())
        all_settings.push_back(setting_field.getName());
    return all_settings;
}

void MySQLSettings::loadFromNamedCollection(const NamedCollection & named_collection)
{
    for (const auto & setting : impl->all())
    {
        const auto & setting_name = setting.getName();
        if (named_collection.has(setting_name))
            impl->set(setting_name, named_collection.get<String>(setting_name));
    }
}
}
