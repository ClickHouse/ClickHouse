#pragma once

#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB
{

struct Settings;
struct MergeTreeSettings;
class Context;

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
///
/// `extra_parts_on_remote_disk` reports remote parts that are not (or no longer) in `parts` but were
/// still touched by the same query - for lazy materialization the first-phase scan can read remote
/// parts whose rows do not survive the limit, and such a mixed read must stay opt-in.
bool resolveUseUncompressedCacheForMergeTreeRead(
    const RangesInDataParts & parts,
    size_t sum_marks,
    const Settings & settings,
    const MergeTreeSettings & data_settings,
    bool has_uncompressed_cache,
    bool extra_parts_on_remote_disk = false);

/// Whether the automatic mode is enabled but an explicit `use_uncompressed_cache = 0` opts out of it.
///
/// The opt-out is carried only by the `changed` flag of a setting whose value equals the default, and that
/// flag does not survive a secondary query: the leaf server turns the forwarded settings back into
/// `SettingsChanges` and clamps them to its constraints, which drops every change whose value already equals
/// the leaf's current value. So the initiator has to resolve the opt-out itself and switch
/// `enable_automatic_use_uncompressed_cache` off in the settings it sends to the shards or replicas
/// (`ClusterProxy::resolveAutomaticUncompressedCacheOptOut`), and bake an explicit `use_uncompressed_cache = 0`
/// into the forwarded query text, whose own `SETTINGS` clause the leaf replays after that clamping
/// (`ClusterProxy::resolveAutomaticUncompressedCacheOptOutInQuery`).
bool automaticUncompressedCacheIsOverriddenByOptOut(const Settings & settings);

/// Keep an explicit `use_uncompressed_cache = 0` of the caller when a query context is rebuilt from the global
/// context for another user (a `SQL SECURITY DEFINER` view, `EXECUTE AS`). Such a rebuild clamps the caller's
/// `SettingsChanges` to the new user's constraints, which drops every change whose value equals the current one,
/// so the default-valued opt-out is lost while `enable_automatic_use_uncompressed_cache = 1` survives.
///
/// `caller_opted_out` is `automaticUncompressedCacheIsOverriddenByOptOut` of the caller's settings, taken before
/// the rebuild. The setting is marked as explicitly set only when the rebuilt context still holds the unchanged
/// default `0`: its value does not change, so the new user's constraints are respected, and a value set by the
/// new user's profile, or a `1` forced by its constraints, is left alone.
void preserveUncompressedCacheOptOut(Context & context, bool caller_opted_out);

}
