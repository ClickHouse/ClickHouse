#pragma once

#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/Statistics/Statistics.h>

namespace DB
{

namespace MergeTreeSetting
{
extern const MergeTreeSettingsBool auto_statistics_assume_floats_distinct;
extern const MergeTreeSettingsBool auto_statistics_assume_long_strings_distinct;
extern const MergeTreeSettingsUInt64 auto_statistics_long_string_distinct_min_length;
extern const MergeTreeSettingsUInt64 auto_statistics_long_string_distinct_probe_rows;
}

inline StatisticsBuildOptions getStatisticsBuildOptions(const MergeTreeSettings & settings)
{
    return {
        .assume_floats_distinct = settings[MergeTreeSetting::auto_statistics_assume_floats_distinct],
        .assume_long_strings_distinct = settings[MergeTreeSetting::auto_statistics_assume_long_strings_distinct],
        .long_string_distinct_min_length = settings[MergeTreeSetting::auto_statistics_long_string_distinct_min_length],
        .long_string_distinct_probe_rows = settings[MergeTreeSetting::auto_statistics_long_string_distinct_probe_rows]};
}

}
