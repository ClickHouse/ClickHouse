#include <Interpreters/FutureSetSettings.h>

#include <Core/Settings.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool make_distributed_plan;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsUInt64 max_bytes_before_external_set;
    extern const SettingsUInt64 max_bytes_in_set;
    extern const SettingsDouble max_bytes_ratio_before_external_set;
    extern const SettingsUInt64 max_rows_in_set;
    extern const SettingsUInt64 min_free_disk_space_for_temporary_data;
    extern const SettingsOverflowMode set_overflow_mode;
    extern const SettingsString temporary_files_codec;
    extern const SettingsNonZeroUInt64 temporary_files_buffer_size;
    extern const SettingsBool transform_null_in;
    extern const SettingsUInt64 use_index_for_in_with_subqueries_max_values;
}

FutureSetSettings::FutureSetSettings(const Settings & settings)
    : size_limits(settings[Setting::max_rows_in_set], settings[Setting::max_bytes_in_set], settings[Setting::set_overflow_mode])
    , transform_null_in(settings[Setting::transform_null_in])
    , max_block_size(settings[Setting::max_block_size])
    , min_free_disk_space(settings[Setting::min_free_disk_space_for_temporary_data])
    , temporary_files_codec(settings[Setting::temporary_files_codec])
    , temporary_files_buffer_size(settings[Setting::temporary_files_buffer_size])
{
    /// The sets of a distributed plan do not spill to disk yet. The plan ships the values of each set to
    /// its worker tasks, and a set on disk does not keep its values. For the same reason,
    /// `use_index_for_in_with_subqueries_max_values` does not limit the values that these sets keep.
    if (settings[Setting::make_distributed_plan])
        return;

    max_size_for_index = settings[Setting::use_index_for_in_with_subqueries_max_values];
    max_bytes_before_external_set = settings[Setting::max_bytes_before_external_set];
    max_bytes_ratio_before_external_set = settings[Setting::max_bytes_ratio_before_external_set];
}

}
