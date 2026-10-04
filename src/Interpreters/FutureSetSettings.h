#pragma once

#include <QueryPipeline/SizeLimits.h>
#include <base/types.h>

namespace DB
{

struct Settings;

/// The query settings that define a set of `IN`: its size limits, its handling of `NULL`s, the
/// values that it keeps for index analysis and, for a set built from a subquery, its spilling to
/// disk while it is filled (see `Set::setSpillSettings`).
struct FutureSetSettings
{
    /// `max_rows_in_set`, `max_bytes_in_set` and `set_overflow_mode`.
    SizeLimits size_limits;
    bool transform_null_in = false;
    /// The most values that the set keeps for index analysis; 0 keeps all of them.
    size_t max_size_for_index = 0;

    /// Both thresholds of 0 keep the set in memory.
    size_t max_bytes_before_external_set = 0;
    double max_bytes_ratio_before_external_set = 0;
    size_t max_block_size = 0;
    size_t min_free_disk_space = 0;
    String temporary_files_codec;
    size_t temporary_files_buffer_size = 0;

    /// No size limits, and the set stays in memory, for sets of internal operations that do not use query
    /// settings.
    FutureSetSettings() = default;

    explicit FutureSetSettings(const Settings & settings);
};

}
