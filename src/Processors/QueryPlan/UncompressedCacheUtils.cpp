#include <Processors/QueryPlan/UncompressedCacheUtils.h>
#include <Processors/QueryPlan/PartsRemoteFSUtils.h>

#include <Core/Settings.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool enable_automatic_use_uncompressed_cache;
    extern const SettingsBool use_uncompressed_cache;
    extern const SettingsUInt64 merge_tree_max_bytes_to_use_cache;
    extern const SettingsUInt64 merge_tree_max_rows_to_use_cache;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 index_granularity;
    extern const MergeTreeSettingsUInt64 index_granularity_bytes;
}

bool resolveUseUncompressedCacheForMergeTreeRead(
    const RangesInDataParts & parts,
    size_t sum_marks,
    const Settings & settings,
    const MergeTreeSettings & data_settings,
    bool has_uncompressed_cache,
    bool extra_parts_on_remote_disk)
{
    size_t adaptive_parts = 0;
    for (const auto & part : parts)
    {
        if (part.data_part->index_granularity_info.mark_type.adaptive)
            ++adaptive_parts;
    }

    size_t index_granularity_bytes = 0;
    if (adaptive_parts > parts.size() / 2)
        index_granularity_bytes = data_settings[MergeTreeSetting::index_granularity_bytes];

    const size_t max_marks_to_use_cache = MergeTreeDataSelectExecutor::roundRowsOrBytesToMarks(
        settings[Setting::merge_tree_max_rows_to_use_cache],
        settings[Setting::merge_tree_max_bytes_to_use_cache],
        data_settings[MergeTreeSetting::index_granularity],
        index_granularity_bytes);

    const bool any_parts_on_remote_disk
        = extra_parts_on_remote_disk || analyzePartsOnRemoteFS(parts).any_parts_on_remote_disk;

    /// The uncompressed cache setting is applied to the whole read pool. When the user did not
    /// explicitly override it, keep any query touching remote/object-storage parts opt-in by
    /// default because those parts already have other cache layers.
    const bool auto_enable_supported =
        settings[Setting::enable_automatic_use_uncompressed_cache]
        && canAutoEnableUncompressedCacheForMergeTreeRead(any_parts_on_remote_disk, has_uncompressed_cache);

    return shouldUseUncompressedCacheForMergeTreeRead(
        settings[Setting::use_uncompressed_cache].changed,
        settings[Setting::use_uncompressed_cache],
        sum_marks <= max_marks_to_use_cache,
        auto_enable_supported);
}

bool automaticUncompressedCacheIsOverriddenByOptOut(const Settings & settings)
{
    return settings[Setting::enable_automatic_use_uncompressed_cache]
        && settings[Setting::use_uncompressed_cache].changed
        && !settings[Setting::use_uncompressed_cache];
}

}
