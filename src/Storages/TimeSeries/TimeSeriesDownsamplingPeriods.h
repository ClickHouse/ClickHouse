#pragma once

#include <base/types.h>

#include <string_view>
#include <vector>


namespace DB
{

/// One downsampling level of the `samples_downsampling_periods` setting:
/// a bucket older than `offset_seconds` keeps the last sample of each `interval_seconds` long interval.
struct TimeSeriesDownsamplingLevel
{
    UInt64 offset_seconds = 0;
    UInt64 interval_seconds = 0;
};

/// The parsed `samples_downsampling_periods` setting: `offset:interval[,offset:interval...]`, the levels are ordered by offset.
struct TimeSeriesDownsamplingPeriods
{
    std::vector<TimeSeriesDownsamplingLevel> levels;

    /// Parses the setting; throws INVALID_SETTING_VALUE if the text is malformed or the levels are inconsistent.
    static TimeSeriesDownsamplingPeriods parse(std::string_view text);

    /// Checks the levels against the bucket step of the samples table and the retention of the recent samples table.
    void validateForTable(UInt64 bucket_step_seconds, UInt64 min_offset_seconds) const;
};

}
