#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Storages/YTsaurus/YTsaurusSettings.h>
#include <Common/Exception.h>
#include <Common/NamedCollections/NamedCollections.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

#define LIST_OF_YTSAURUS_SETTINGS(DECLARE, ALIAS) \
    DECLARE(Bool, check_table_schema, true, "Check the ClickHouse and YTsaurus table schema for compatibility", 0) \
    DECLARE(Bool, skip_unknown_columns, true, "Skip columns with unknown type", 0) \
    DECLARE(Bool, force_read_table, false, "Force the use of read table instead of lookups for dynamic tables.", 0) \
    DECLARE(Bool, encode_utf8, false, "Enable the utf8 encoding in ytsaurus responses.", 0) \
    DECLARE(Bool, enable_heavy_proxy_redirection, true, "Enable redirection to heavy proxies for heavy queries. See: https://ytsaurus.tech/docs/en/user-guide/proxy/http-reference#hosts", 0) \
    DECLARE(Bool, use_lock, true, "Lock table to do request, to get consistent snapshot. See: https://ytsaurus.tech/docs/en/user-guide/storage/transactions#locks", 0) \
    DECLARE(Milliseconds, transaction_timeout_ms, 150000, "Timeout for YTSaurus transaction.", 0) \
    DECLARE(UInt64, min_rows_for_spawn_stream, 1000, "Min number of rows to spawn the new stream. To use 8 streams the table must hold at least 8 * `min_rows_for_spawn_stream` rows.", 0) \
    DECLARE(UInt64, max_streams, 4, "Max number of streams to read from static table.", 0) \

DECLARE_SETTINGS_TRAITS(YTsaurusSettingsTraits, LIST_OF_YTSAURUS_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(YTsaurusSettingsTraits, LIST_OF_YTSAURUS_SETTINGS)

struct YTsaurusSettingsImpl : public BaseSettings<YTsaurusSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS, ...) YTsaurusSettings##TYPE NAME = &YTsaurusSettingsImpl ::NAME;

namespace YTsaurusSetting
{
LIST_OF_YTSAURUS_SETTINGS(INITIALIZE_SETTING_EXTERN, INITIALIZE_SETTING_EXTERN)
}

#undef INITIALIZE_SETTING_EXTERN

YTsaurusSettings::YTsaurusSettings() : impl(std::make_unique<YTsaurusSettingsImpl>())
{
}

YTsaurusSettings::YTsaurusSettings(const YTsaurusSettings & settings) : impl(std::make_unique<YTsaurusSettingsImpl>(*settings.impl))
{
}

YTsaurusSettings::YTsaurusSettings(YTsaurusSettings && settings) noexcept : impl(std::make_unique<YTsaurusSettingsImpl>(std::move(*settings.impl)))
{
}

YTsaurusSettings::~YTsaurusSettings() = default;

YTSAURUS_SETTINGS_SUPPORTED_TYPES(YTsaurusSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR);

void YTsaurusSettings::loadFromQuery(const ASTSetQuery & settings_def)
{
    impl->applyChanges(settings_def.changes);
}

YTsaurusSettings YTsaurusSettings::createFromQuery(const ASTSetQuery & settings_def) {
    YTsaurusSettings settings;
    settings.loadFromQuery(settings_def);
    return settings;
}

void YTsaurusSettings::loadFromQuery(ASTStorage & storage_def)
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

YTsaurusSettings YTsaurusSettings::createFromQuery(ASTStorage & storage_def) {
    YTsaurusSettings settings;
    settings.loadFromQuery(storage_def);
    return settings;
}

std::vector<std::string_view> YTsaurusSettings::getAllRegisteredNames() const
{
    std::vector<std::string_view> all_settings;
    for (const auto & setting_field : impl->all())
        all_settings.push_back(setting_field.getName());
    return all_settings;
}

void YTsaurusSettings::loadFromNamedCollection(const NamedCollection & named_collection)
{
    for (const auto & setting : impl->all())
    {
        const auto & setting_name = setting.getName();
        if (named_collection.has(setting_name))
            impl->set(setting_name, named_collection.get<String>(setting_name));
    }
}

void YTsaurusSettings::set(const std::string & name, const std::string & value)
{
    impl->set(name, value);
}

bool YTsaurusSettings::hasBuiltin(std::string_view name)
{
    return YTsaurusSettingsImpl::hasBuiltin(name);
}


}
