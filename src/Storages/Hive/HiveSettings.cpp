#include <Storages/Hive/HiveSettings.h>

#if USE_HIVE

#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Core/FormatFactorySettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSetQuery.h>
#include <Common/Exception.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
}

#define HIVE_RELATED_SETTINGS(DECLARE, ALIAS) \
    DECLARE(Char, hive_text_field_delimeter, '\x01', "How to split one row of hive data with format text", 0) \
    DECLARE(Bool, enable_orc_stripe_minmax_index, false, "Enable using ORC stripe level minmax index.", 0) \
    DECLARE(Bool, enable_parquet_rowgroup_minmax_index, false, "Enable using Parquet row-group level minmax index.", 0) \
    DECLARE(Bool, enable_orc_file_minmax_index, true, "Enable using ORC file level minmax index.", 0)

#define LIST_OF_HIVE_SETTINGS(M, ALIAS) \
    HIVE_RELATED_SETTINGS(M, ALIAS) \
    LIST_OF_ALL_FORMAT_SETTINGS(M, ALIAS)

DECLARE_SETTINGS_TRAITS(HiveSettingsTraits, LIST_OF_HIVE_SETTINGS)
IMPLEMENT_SETTINGS_TRAITS(HiveSettingsTraits, LIST_OF_HIVE_SETTINGS)

struct HiveSettingsImpl : public BaseSettings<HiveSettingsTraits>
{
};

#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS) HiveSettings##TYPE NAME = &HiveSettingsImpl ::NAME;

namespace HiveSetting
{
LIST_OF_HIVE_SETTINGS(INITIALIZE_SETTING_EXTERN, SKIP_ALIAS)
}

#undef INITIALIZE_SETTING_EXTERN

HiveSettings::HiveSettings() : impl(std::make_unique<HiveSettingsImpl>())
{
}

HiveSettings::HiveSettings(const HiveSettings & settings) : impl(std::make_unique<HiveSettingsImpl>(*settings.impl))
{
}

HiveSettings::HiveSettings(HiveSettings && settings) noexcept : impl(std::make_unique<HiveSettingsImpl>(std::move(*settings.impl)))
{
}

HiveSettings::~HiveSettings() = default;

HIVE_SETTINGS_SUPPORTED_TYPES(HiveSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

void HiveSettings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    try
    {
        for (const String & key : config_keys)
            impl->set(key, config.getString(config_elem + "." + key));
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_SETTING)
            e.addMessage("in MergeTree config");
        throw;
    }
}

void HiveSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            impl->applyChanges(storage_def.settings->changes);
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

}
#endif
