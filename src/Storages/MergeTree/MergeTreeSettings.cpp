#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Disks/DiskFomAST.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/FieldFromAST.h>
#include <Parsers/isDiskFunction.h>
#include <Core/Field.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
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

void MergeTreeSettings::loadFromQuery(ASTStorage & storage_def, ContextPtr context, bool is_attach)
{
    if (storage_def.settings)
    {
        try
        {
            bool found_disk_setting = false;
            bool found_storage_policy_setting = false;

            auto changes = storage_def.settings->changes;
            for (auto & [name, value] : changes)
            {
                CustomType custom;
                if (name == "disk")
                {
                    ASTPtr value_as_custom_ast = nullptr;
                    if (value.tryGet<CustomType>(custom) && 0 == strcmp(custom.getTypeName(), "AST"))
                        value_as_custom_ast = dynamic_cast<const FieldFromASTImpl &>(custom.getImpl()).ast;

                    if (value_as_custom_ast && isDiskFunction(value_as_custom_ast))
                    {
                        auto disk_name = DiskFomAST::createCustomDisk(value_as_custom_ast, context, is_attach);
                        LOG_DEBUG(getLogger("MergeTreeSettings"), "Created custom disk {}", disk_name);
                        value = disk_name;
                    }
                    else
                    {
                        DiskFomAST::ensureDiskIsNotCustom(value.safeGet<String>(), context);
                    }

                    if (has("storage_policy"))
                        resetToDefault("storage_policy");

                    found_disk_setting = true;
                }
                else if (name == "storage_policy")
                    found_storage_policy_setting = true;

                if (found_disk_setting && found_storage_policy_setting)
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "MergeTree settings `storage_policy` and `disk` cannot be specified at the same time");
                }

            }

            applyChanges(changes);
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

bool MergeTreeSettings::isReadonlySetting(const String & name)
{
    return name == "index_granularity" || name == "index_granularity_bytes" || name == "enable_mixed_granularity_parts";
}

bool MergeTreeSettings::isPartFormatSetting(const String & name)
{
    return name == "min_bytes_for_wide_part" || name == "min_rows_for_wide_part";
}

void MergeTreeSettings::sanityCheck(size_t background_pool_tasks) const
{
    if (number_of_free_entries_in_pool_to_execute_mutation > background_pool_tasks)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of 'number_of_free_entries_in_pool_to_execute_mutation' setting"
            " ({}) (default values are defined in <merge_tree> section of config.xml"
            " or the value can be specified per table in SETTINGS section of CREATE TABLE query)"
            " is greater than the value of 'background_pool_size'*'background_merges_mutations_concurrency_ratio'"
            " ({}) (the value is defined in users.xml for default profile)."
            " This indicates incorrect configuration because mutations cannot work with these settings.",
            number_of_free_entries_in_pool_to_execute_mutation,
            background_pool_tasks);
    }

    if (number_of_free_entries_in_pool_to_lower_max_size_of_merge > background_pool_tasks)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of 'number_of_free_entries_in_pool_to_lower_max_size_of_merge' setting"
            " ({}) (default values are defined in <merge_tree> section of config.xml"
            " or the value can be specified per table in SETTINGS section of CREATE TABLE query)"
            " is greater than the value of 'background_pool_size'*'background_merges_mutations_concurrency_ratio'"
            " ({}) (the value is defined in users.xml for default profile)."
            " This indicates incorrect configuration because the maximum size of merge will be always lowered.",
            number_of_free_entries_in_pool_to_lower_max_size_of_merge,
            background_pool_tasks);
    }

    if (number_of_free_entries_in_pool_to_execute_optimize_entire_partition > background_pool_tasks)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of 'number_of_free_entries_in_pool_to_execute_optimize_entire_partition' setting"
            " ({}) (default values are defined in <merge_tree> section of config.xml"
            " or the value can be specified per table in SETTINGS section of CREATE TABLE query)"
            " is greater than the value of 'background_pool_size'*'background_merges_mutations_concurrency_ratio'"
            " ({}) (the value is defined in users.xml for default profile)."
            " This indicates incorrect configuration because the maximum size of merge will be always lowered.",
            number_of_free_entries_in_pool_to_execute_optimize_entire_partition,
            background_pool_tasks);
    }

    // Zero index_granularity is nonsensical.
    if (index_granularity < 1)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "index_granularity: value {} makes no sense",
            index_granularity);
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
            "min_bytes_to_rebalance_partition_over_jbod: {} is lower than specified max_bytes_to_merge_at_max_space_in_pool / 1024: {}",
            min_bytes_to_rebalance_partition_over_jbod,
            max_bytes_to_merge_at_max_space_in_pool / 1024);
    }

    if (max_cleanup_delay_period < cleanup_delay_period)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The value of max_cleanup_delay_period setting ({}) must be greater than the value of cleanup_delay_period setting ({})",
            max_cleanup_delay_period, cleanup_delay_period);
    }

    if (max_merge_selecting_sleep_ms < merge_selecting_sleep_ms)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The value of max_merge_selecting_sleep_ms setting ({}) must be greater than the value of merge_selecting_sleep_ms setting ({})",
            max_merge_selecting_sleep_ms, merge_selecting_sleep_ms);
    }

    if (merge_selecting_sleep_slowdown_factor < 1.f)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The value of merge_selecting_sleep_slowdown_factor setting ({}) cannot be less than 1.0",
            merge_selecting_sleep_slowdown_factor);
    }
}

void MergeTreeColumnSettings::validate(const SettingsChanges & changes)
{
    static const MergeTreeSettings merge_tree_settings;
    static const std::set<String> allowed_column_level_settings =
    {
        "min_compress_block_size",
        "max_compress_block_size"
    };

    for (const auto & change : changes)
    {
        if (!allowed_column_level_settings.contains(change.name))
            throw Exception(
                ErrorCodes::UNKNOWN_SETTING,
                "Setting {} is unknown or not supported at column level, supported settings: {}",
                change.name,
                fmt::join(allowed_column_level_settings, ", "));
        MergeTreeSettings::checkCanSet(change.name, change.value);
    }
}


std::vector<String> MergeTreeSettings::getAllRegisteredNames() const
{
    std::vector<String> all_settings;
    for (const auto & setting_field : all())
        all_settings.push_back(setting_field.getName());
    return all_settings;
}

std::string_view MergeTreeSettings::resolveName(std::string_view name)
{
    return MergeTreeSettings::Traits::resolveName(name);
}
}
