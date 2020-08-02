#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTFunction.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_SETTING;
}

IMPLEMENT_SETTINGS_TRAITS(MergeTreeSettingsTraits, LIST_OF_MERGE_TREE_SETTINGS)

void MergeTreeSettings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    try
    {
        for (const String & key : config_keys)
            set(key, config.getString(config_elem + "." + key));
    }
    catch (Exception & e)
    {
        if (e.code() == ErrorCodes::UNKNOWN_SETTING)
            throw Exception(e.message() + " in MergeTree config", ErrorCodes::INVALID_CONFIG_PARAMETER);
        else
            e.rethrow();
    }
}

void MergeTreeSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        try
        {
            applyChanges(storage_def.settings->changes);
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_SETTING)
                throw Exception(e.message() + " for storage " + storage_def.engine->name, ErrorCodes::BAD_ARGUMENTS);
            else
                e.rethrow();
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }

    SettingsChanges & changes = storage_def.settings->changes;

#define ADD_IF_ABSENT(NAME)                                                                                   \
    if (std::find_if(changes.begin(), changes.end(),                                                          \
                  [](const SettingChange & c) { return c.name == #NAME; })                                    \
            == changes.end())                                                                                 \
        changes.push_back(SettingChange{#NAME, (NAME).value});

    APPLY_FOR_IMMUTABLE_MERGE_TREE_SETTINGS(ADD_IF_ABSENT)
#undef ADD_IF_ABSENT
}

}
