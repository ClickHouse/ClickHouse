#pragma once

namespace DB
{

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

}
