#include <Core/Settings.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/SelectQueryInfo.h>

#include <Interpreters/Context.h>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 low_cardinality_max_dictionary_size;
    extern const SettingsBool low_cardinality_use_single_dictionary_for_part;
    extern const SettingsUInt64 min_compress_block_size;
    extern const SettingsUInt64 max_compress_block_size;
    extern const SettingsBool merge_tree_use_v1_object_and_dynamic_serialization;

    extern const SettingsBool checksum_on_read;
    extern const SettingsBool apply_deleted_mask;
    extern const SettingsBool allow_asynchronous_read_from_io_pool_for_merge_tree;
    extern const SettingsBool enable_multiple_prewhere_read_steps;
    extern const SettingsBool query_plan_merge_filters;
    extern const SettingsFloat max_streams_to_max_threads_ratio;
    extern const SettingsUInt64 max_streams_for_merge_tree_reading;
    extern const SettingsBool use_query_condition_cache;
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool query_condition_cache_store_conditions_as_plaintext;
    extern const SettingsBool merge_tree_use_deserialization_prefixes_cache;
    extern const SettingsBool merge_tree_use_prefixes_deserialization_thread_pool;
    extern const SettingsUInt64 filesystem_prefetches_limit;
    extern const SettingsBool secondary_indices_enable_bulk_filtering;
    extern const SettingsUInt64 merge_tree_min_bytes_for_seek;
    extern const SettingsUInt64 merge_tree_min_rows_for_seek;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool compress_primary_key;
    extern const MergeTreeSettingsBool use_adaptive_write_buffer_for_dynamic_subcolumns;
    extern const MergeTreeSettingsBool use_compact_variant_discriminators_serialization;
    extern const MergeTreeSettingsNonZeroUInt64 marks_compress_block_size;
    extern const MergeTreeSettingsString marks_compression_codec;
    extern const MergeTreeSettingsString primary_key_compression_codec;
    extern const MergeTreeSettingsUInt64 adaptive_write_buffer_initial_size;
    extern const MergeTreeSettingsUInt64 max_compress_block_size;
    extern const MergeTreeSettingsUInt64 min_compress_block_size;
    extern const MergeTreeSettingsNonZeroUInt64 primary_key_compress_block_size;
    extern const MergeTreeSettingsMergeTreeObjectSerializationVersion object_serialization_version;
    extern const MergeTreeSettingsMergeTreeObjectSharedDataSerializationVersion object_shared_data_serialization_version;
    extern const MergeTreeSettingsMergeTreeObjectSharedDataSerializationVersion object_shared_data_serialization_version_for_zero_level_parts;
    extern const MergeTreeSettingsNonZeroUInt64 object_shared_data_buckets_for_compact_part;
    extern const MergeTreeSettingsNonZeroUInt64 object_shared_data_buckets_for_wide_part;
    extern const MergeTreeSettingsMergeTreeDynamicSerializationVersion dynamic_serialization_version;
}

MergeTreeWriterSettings::MergeTreeWriterSettings(
    const Settings & global_settings,
    const WriteSettings & query_write_settings_,
    const MergeTreeSettingsPtr & storage_settings,
    const MergeTreeDataPartPtr & data_part,
    bool can_use_adaptive_granularity_,
    bool rewrite_primary_key_,
    bool save_marks_in_cache_,
    bool save_primary_index_in_memory_,
    bool blocks_are_granules_size_)
    : min_compress_block_size((*storage_settings)[MergeTreeSetting::min_compress_block_size] ? (*storage_settings)[MergeTreeSetting::min_compress_block_size] : global_settings[Setting::min_compress_block_size])
    , max_compress_block_size((*storage_settings)[MergeTreeSetting::max_compress_block_size] ? (*storage_settings)[MergeTreeSetting::max_compress_block_size] : global_settings[Setting::max_compress_block_size])
    , marks_compression_codec((*storage_settings)[MergeTreeSetting::marks_compression_codec])
    , marks_compress_block_size((*storage_settings)[MergeTreeSetting::marks_compress_block_size])
    , compress_primary_key((*storage_settings)[MergeTreeSetting::compress_primary_key])
    , primary_key_compression_codec((*storage_settings)[MergeTreeSetting::primary_key_compression_codec])
    , primary_key_compress_block_size((*storage_settings)[MergeTreeSetting::primary_key_compress_block_size])
    , can_use_adaptive_granularity(can_use_adaptive_granularity_)
    , rewrite_primary_key(rewrite_primary_key_)
    , save_marks_in_cache(save_marks_in_cache_)
    , save_primary_index_in_memory(save_primary_index_in_memory_)
    , blocks_are_granules_size(blocks_are_granules_size_)
    , query_write_settings(query_write_settings_)
    , low_cardinality_max_dictionary_size(global_settings[Setting::low_cardinality_max_dictionary_size])
    , low_cardinality_use_single_dictionary_for_part(global_settings[Setting::low_cardinality_use_single_dictionary_for_part] != 0)
    , use_compact_variant_discriminators_serialization((*storage_settings)[MergeTreeSetting::use_compact_variant_discriminators_serialization])
    , dynamic_serialization_version(global_settings[Setting::merge_tree_use_v1_object_and_dynamic_serialization] ? MergeTreeDynamicSerializationVersion::V1 : (*storage_settings)[MergeTreeSetting::dynamic_serialization_version])
    , object_serialization_version(global_settings[Setting::merge_tree_use_v1_object_and_dynamic_serialization] ? MergeTreeObjectSerializationVersion::V1 : (*storage_settings)[MergeTreeSetting::object_serialization_version])
    , object_shared_data_serialization_version(data_part->isZeroLevel() ? (*storage_settings)[MergeTreeSetting::object_shared_data_serialization_version_for_zero_level_parts] : (*storage_settings)[MergeTreeSetting::object_shared_data_serialization_version])
    , object_shared_data_buckets(isCompactPart(data_part) ? (*storage_settings)[MergeTreeSetting::object_shared_data_buckets_for_compact_part] : (*storage_settings)[MergeTreeSetting::object_shared_data_buckets_for_wide_part])
    , use_adaptive_write_buffer_for_dynamic_subcolumns((*storage_settings)[MergeTreeSetting::use_adaptive_write_buffer_for_dynamic_subcolumns])
    , adaptive_write_buffer_initial_size((*storage_settings)[MergeTreeSetting::adaptive_write_buffer_initial_size])
{
}

MergeTreeReaderSettings MergeTreeReaderSettings::create(const ContextPtr & context, const MergeTreeSettings & /*storage_settings*/, const SelectQueryInfo & query_info)
{
    const auto & settings = context->getSettingsRef();
    return {
        .read_settings = context->getReadSettings(),
        .save_marks_in_cache = true,
        .checksum_on_read = settings[Setting::checksum_on_read],
        .read_in_order = query_info.input_order_info != nullptr,
        .apply_deleted_mask = settings[Setting::apply_deleted_mask],
        .use_asynchronous_read_from_pool = settings[Setting::allow_asynchronous_read_from_io_pool_for_merge_tree]
            && (settings[Setting::max_streams_to_max_threads_ratio] > 1 || settings[Setting::max_streams_for_merge_tree_reading] > 1),
        .enable_multiple_prewhere_read_steps = settings[Setting::enable_multiple_prewhere_read_steps],
        .force_short_circuit_execution = settings[Setting::query_plan_merge_filters],
        .use_query_condition_cache = settings[Setting::use_query_condition_cache] && settings[Setting::allow_experimental_analyzer],
        .query_condition_cache_store_conditions_as_plaintext = settings[Setting::query_condition_cache_store_conditions_as_plaintext],
        .use_deserialization_prefixes_cache = settings[Setting::merge_tree_use_deserialization_prefixes_cache],
        .use_prefixes_deserialization_thread_pool = settings[Setting::merge_tree_use_prefixes_deserialization_thread_pool],
        .secondary_indices_enable_bulk_filtering = settings[Setting::secondary_indices_enable_bulk_filtering],
        .merge_tree_min_bytes_for_seek = settings[Setting::merge_tree_min_bytes_for_seek],
        .merge_tree_min_rows_for_seek = settings[Setting::merge_tree_min_rows_for_seek],
        .filesystem_prefetches_limit = settings[Setting::filesystem_prefetches_limit],
        .enable_analyzer = settings[Setting::allow_experimental_analyzer],
    };
}

}
