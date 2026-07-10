#pragma once

#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB
{

struct Settings;
struct MergeTreeSettings;

constexpr bool canAutoEnableUncompressedCacheForMergeTreeRead(
    bool any_parts_on_remote_disk,
    bool has_uncompressed_cache)
{
    return !any_parts_on_remote_disk && has_uncompressed_cache;
}

constexpr bool shouldUseUncompressedCacheForMergeTreeRead(
    bool setting_changed,
    bool setting_value,
    bool query_fits_cache_thresholds,
    bool auto_enable_supported)
{
    if (!query_fits_cache_thresholds)
        return false;

    if (setting_changed)
        return setting_value;

    return auto_enable_supported;
}

/// Resolve whether a MergeTree read of `parts` should use the uncompressed cache.
///
/// The automatic decision is evaluated against the ranges that will actually be read
/// (`parts` / `sum_marks`). For the lazy-materialization second phase this must be called
/// with the second-phase ranges, not the pre-limit scan, so that a large first pass does not
/// disable caching for a small repeated payload read. An explicit `use_uncompressed_cache`
/// override always wins.
bool resolveUseUncompressedCacheForMergeTreeRead(
    const RangesInDataParts & parts,
    size_t sum_marks,
    const Settings & settings,
    const MergeTreeSettings & data_settings,
    bool has_uncompressed_cache);

}
