#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int BAD_ARGUMENTS;
}

void MergeTreeSettings::loadFromConfig(const String & config_elem, Poco::Util::AbstractConfiguration & config)
{
#define SET(NAME, GETTER) \
    try \
    { \
        NAME = config.GETTER(config_elem + "." #NAME, NAME); \
    } \
    catch (const Poco::Exception & e) \
    { \
        throw Exception( \
                "Invalid config parameter: " + config_elem + "/" #NAME + ": " + e.message() + ".", \
                ErrorCodes::INVALID_CONFIG_PARAMETER); \
    }

    SET(index_granularity, getUInt64);
    SET(max_bytes_to_merge_at_max_space_in_pool, getUInt64);
    SET(max_bytes_to_merge_at_min_space_in_pool, getUInt64);
    SET(max_replicated_merges_in_queue, getUInt64);
    SET(number_of_free_entries_in_pool_to_lower_max_size_of_merge, getUInt64);
    SET(old_parts_lifetime, getUInt64);
    SET(temporary_directories_lifetime, getUInt64);
    SET(parts_to_delay_insert, getUInt64);
    SET(parts_to_throw_insert, getUInt64);
    SET(max_delay_to_insert, getUInt64);
    SET(replicated_deduplication_window, getUInt64);
    SET(replicated_deduplication_window_seconds, getUInt64);
    SET(replicated_logs_to_keep, getUInt64);
    SET(prefer_fetch_merged_part_time_threshold, getUInt64);
    SET(prefer_fetch_merged_part_size_threshold, getUInt64);
    SET(max_suspicious_broken_parts, getUInt64);
    SET(max_files_to_modify_in_alter_columns, getUInt64);
    SET(max_files_to_remove_in_alter_columns, getUInt64);
    SET(replicated_max_ratio_of_wrong_parts, getDouble);
    SET(replicated_max_parallel_fetches, getUInt64);
    SET(replicated_max_parallel_fetches_for_table, getUInt64);
    SET(replicated_max_parallel_sends, getUInt64);
    SET(replicated_max_parallel_sends_for_table, getUInt64);
    SET(replicated_can_become_leader, getBool);
    SET(zookeeper_session_expiration_check_period, getUInt64);
    SET(check_delay_period, getUInt64);
    SET(cleanup_delay_period, getUInt64);
    SET(min_relative_delay_to_yield_leadership, getUInt64);
    SET(min_relative_delay_to_close, getUInt64);
    SET(min_absolute_delay_to_close, getUInt64);
    SET(enable_vertical_merge_algorithm, getUInt64);
    SET(vertical_merge_algorithm_min_rows_to_activate, getUInt64);
    SET(vertical_merge_algorithm_min_columns_to_activate, getUInt64);

#undef SET
}

void MergeTreeSettings::loadFromQuery(ASTStorage & storage_def)
{
    bool index_granularity_changed = false;
    if (storage_def.settings)
    {
        for (const ASTSetQuery::Change & setting : storage_def.settings->changes)
        {
            if (setting.name == "index_granularity")
            {
                index_granularity = setting.value.safeGet<UInt64>();
                index_granularity_changed = true;
            }
            else
                throw Exception(
                    "Unknown setting " + setting.name + " for storage " + storage_def.engine->name,
                    ErrorCodes::BAD_ARGUMENTS);
        }
    }

    if (!index_granularity_changed)
    {
        if (!storage_def.settings)
        {
            auto settings_ast = std::make_shared<ASTSetQuery>();
            settings_ast->is_standalone = false;
            storage_def.set(storage_def.settings, settings_ast);
        }

        storage_def.settings->changes.push_back(
            ASTSetQuery::Change{"index_granularity", index_granularity});
    }
}

}
