#pragma once

#include <Core/SettingsEnums.h>
#include <Interpreters/Context_fwd.h>
#include <Common/DateLUTImpl.h>

namespace DB
{

/// The `week_functions_starting_day`, `week_functions_range` and `week_functions_first_week_of_year` settings.
/// They apply to week functions called without an explicit `mode` argument: each setting that is not `'auto'`
/// replaces its part of the function's default mode, and the parts left at `'auto'` keep the values of that mode.
struct WeekFunctionsSettings
{
    WeekFunctionsStartingDay starting_day;
    WeekFunctionsRange range;
    WeekFunctionsFirstWeekOfYear first_week_of_year;

    explicit WeekFunctionsSettings(const ContextPtr & context);

    /// The first day of the week, 1 = Monday ... 7 = Sunday: `week_functions_starting_day`, or `default_first_weekday`
    /// if it is `'auto'`.
    UInt8 firstWeekday(UInt8 default_first_weekday) const;

    /// `spec` with the settings that are not `'auto'` applied.
    WeekSpec apply(WeekSpec spec) const;

    /// `spec` with `week_functions_starting_day` applied, if it is not `'auto'`. The other two settings number weeks,
    /// not days, so they don't apply to `toDayOfWeek`.
    WeekDaySpec apply(WeekDaySpec spec) const;
};

}
