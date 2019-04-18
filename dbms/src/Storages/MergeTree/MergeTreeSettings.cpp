#include <Storages/MergeTree/MergeTreeSettings.h>
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
}

void MergeTreeSettings::loadFromConfig(const String & config_elem, const Poco::Util::AbstractConfiguration & config)
{
    if (!config.has(config_elem))
        return;

    Poco::Util::AbstractConfiguration::Keys config_keys;
    config.keys(config_elem, config_keys);

    for (const String & key : config_keys)
    {
        String value = config.getString(config_elem + "." + key);

#define SET(TYPE, NAME, DEFAULT) \
        else if (key == #NAME) NAME.set(value);

        if (false) {}
        APPLY_FOR_MERGE_TREE_SETTINGS(SET)
        else
            throw Exception("Unknown MergeTree setting " + key + " in config", ErrorCodes::INVALID_CONFIG_PARAMETER);
#undef SET
    }
}

void MergeTreeSettings::loadFromQuery(ASTStorage & storage_def)
{
    if (storage_def.settings)
    {
        for (const ASTSetQuery::Change & setting : storage_def.settings->changes)
        {
#define SET(TYPE, NAME, DEFAULT) \
            else if (setting.name == #NAME) NAME.set(setting.value);

            if (false) {}
            APPLY_FOR_MERGE_TREE_SETTINGS(SET)
            else
                throw Exception(
                    "Unknown setting " + setting.name + " for storage " + storage_def.engine->name,
                    ErrorCodes::BAD_ARGUMENTS);
#undef SET
        }
    }
    else
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        storage_def.set(storage_def.settings, settings_ast);
    }

    ASTSetQuery::Changes & changes = storage_def.settings->changes;


    /// Here we are checking immutable storage settings.
    /// They are mutually exclusive so in first 'if' we check, that user specified only one of them.
    String applied_setting_name;
#define CHECK_DUPLICATES(NAME)                                                                                    \
    if (std::find_if(changes.begin(), changes.end(),                                                              \
                [](const auto & c) { return c.name == #NAME; }) != changes.end())                                 \
    {                                                                                                             \
        if (!applied_setting_name.empty())                                                                        \
            throw Exception(                                                                                      \
                std::string{"Mutually exclusive settings are specified '"} + #NAME                                \
                        + "' and '" + applied_setting_name + "'",                                                 \
                ErrorCodes::BAD_ARGUMENTS);                                                                       \
        else                                                          \
            applied_setting_name = #NAME;                                                                         \
    }                                                                                                             \

    APPLY_FOR_IMMUTABLE_MERGE_TREE_SETTINGS(CHECK_DUPLICATES)
#undef CHECK_DUPLICATES

    /// If user doesn't specify any of them, then we choose first, which is not zero.
    /// For rest of them we set zero.
#define ADD_ONE_IF_ABSENT(NAME)                                           \
    if (applied_setting_name.empty())                                     \
    {                                                                     \
        if (NAME.value != 0)                                              \
        {                                                                 \
            applied_setting_name = #NAME;                                 \
            changes.push_back(ASTSetQuery::Change{#NAME, NAME.value});    \
        }                                                                 \
    }                                                                     \
    else if (#NAME != applied_setting_name)                             \
        NAME.set(0);                                                      \

    APPLY_FOR_IMMUTABLE_MERGE_TREE_SETTINGS(ADD_ONE_IF_ABSENT)
#undef ADD_ONE_IF_ABSENT

}

}
