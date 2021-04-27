#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTFunction.h>
#include <Common/Exception.h>
#include <Core/Settings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
    extern const int BAD_ARGUMENTS;
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
            e.addMessage("in MergeTree config");
        throw;
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

    SettingsChanges & changes = storage_def.settings->changes;

#define ADD_IF_ABSENT(NAME)                                                                                   \
    if (std::find_if(changes.begin(), changes.end(),                                                          \
                  [](const SettingChange & c) { return c.name == #NAME; })                                    \
            == changes.end())                                                                                 \
        changes.push_back(SettingChange{#NAME, (NAME).value});

    APPLY_FOR_IMMUTABLE_MERGE_TREE_SETTINGS(ADD_IF_ABSENT)
#undef ADD_IF_ABSENT
}

void MergeTreeSettings::sanityCheck(const Settings & query_settings) const
{
    if (number_of_free_entries_in_pool_to_execute_mutation > query_settings.background_pool_size)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of 'number_of_free_entries_in_pool_to_execute_mutation' setting"
            " ({}) (default values are defined in <merge_tree> section of config.xml"
            " or the value can be specified per table in SETTINGS section of CREATE TABLE query)"
            " is greater than the value of 'background_pool_size'"
            " ({}) (the value is defined in users.xml for default profile)."
            " This indicates incorrect configuration because mutations cannot work with these settings.",
            number_of_free_entries_in_pool_to_execute_mutation,
            query_settings.background_pool_size);
    }

    if (number_of_free_entries_in_pool_to_lower_max_size_of_merge > query_settings.background_pool_size)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of 'number_of_free_entries_in_pool_to_lower_max_size_of_merge' setting"
            " ({}) (default values are defined in <merge_tree> section of config.xml"
            " or the value can be specified per table in SETTINGS section of CREATE TABLE query)"
            " is greater than the value of 'background_pool_size'"
            " ({}) (the value is defined in users.xml for default profile)."
            " This indicates incorrect configuration because the maximum size of merge will be always lowered.",
            number_of_free_entries_in_pool_to_lower_max_size_of_merge,
            query_settings.background_pool_size);
    }

    // The min_index_granularity_bytes value is 1024 b and index_granularity_bytes is 10 mb by default.
    // If index_granularity_bytes is not disabled i.e > 0 b, then always ensure that it's greater than
    // min_index_granularity_bytes. This is mainly a safeguard against accidents whereby a really low
    // index_granularity_bytes SETTING of 1b can create really large parts with large marks.
    if (index_granularity_bytes > 0 && index_granularity_bytes < min_index_granularity_bytes)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "index_granularity_bytes: {} is lower than specified min_index_granularity_bytes: {}",
            index_granularity_bytes,
            min_index_granularity_bytes);
    }

    // If min_bytes_to_rebalance_partition_over_jbod is not disabled i.e > 0 b, then always ensure that
    // it's not less than min_bytes_to_rebalance_partition_over_jbod. This is a safeguard to avoid tiny
    // parts to participate JBOD balancer which will slow down the merge process.
    if (min_bytes_to_rebalance_partition_over_jbod > 0
        && min_bytes_to_rebalance_partition_over_jbod < max_bytes_to_merge_at_max_space_in_pool / 1024)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "min_bytes_to_rebalance_partition_over_jbod: {} is lower than specified max_bytes_to_merge_at_max_space_in_pool / 150: {}",
            min_bytes_to_rebalance_partition_over_jbod,
            max_bytes_to_merge_at_max_space_in_pool / 1024);
    }
}
}
