#include <Core/Settings.h>
#include <DataTypes/ObjectUtils.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 low_cardinality_max_dictionary_size;
    extern const SettingsBool low_cardinality_use_single_dictionary_for_part;
    extern const SettingsUInt64 min_compress_block_size;
    extern const SettingsUInt64 max_compress_block_size;
}

MergeTreeWriterSettings::MergeTreeWriterSettings(
    const Settings & global_settings,
    const WriteSettings & query_write_settings_,
    const MergeTreeSettingsPtr & storage_settings,
    bool can_use_adaptive_granularity_,
    bool rewrite_primary_key_,
    bool blocks_are_granules_size_)
    : min_compress_block_size(
          storage_settings->min_compress_block_size ? storage_settings->min_compress_block_size : global_settings[Setting::min_compress_block_size])
    , max_compress_block_size(
          storage_settings->max_compress_block_size ? storage_settings->max_compress_block_size : global_settings[Setting::max_compress_block_size])
    , marks_compression_codec(storage_settings->marks_compression_codec)
    , marks_compress_block_size(storage_settings->marks_compress_block_size)
    , compress_primary_key(storage_settings->compress_primary_key)
    , primary_key_compression_codec(storage_settings->primary_key_compression_codec)
    , primary_key_compress_block_size(storage_settings->primary_key_compress_block_size)
    , can_use_adaptive_granularity(can_use_adaptive_granularity_)
    , rewrite_primary_key(rewrite_primary_key_)
    , blocks_are_granules_size(blocks_are_granules_size_)
    , query_write_settings(query_write_settings_)
    , low_cardinality_max_dictionary_size(global_settings[Setting::low_cardinality_max_dictionary_size])
    , low_cardinality_use_single_dictionary_for_part(global_settings[Setting::low_cardinality_use_single_dictionary_for_part] != 0)
    , use_compact_variant_discriminators_serialization(storage_settings->use_compact_variant_discriminators_serialization)
    , use_adaptive_write_buffer_for_dynamic_subcolumns(storage_settings->use_adaptive_write_buffer_for_dynamic_subcolumns)
    , adaptive_write_buffer_initial_size(storage_settings->adaptive_write_buffer_initial_size)
{
}

}
