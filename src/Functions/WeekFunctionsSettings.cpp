#include <Functions/WeekFunctionsSettings.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace Setting
{
    extern const SettingsWeekFunctionsStartingDay week_functions_starting_day;
    extern const SettingsWeekFunctionsRange week_functions_range;
    extern const SettingsWeekFunctionsFirstWeekOfYear week_functions_first_week_of_year;
}

/// The day values of the setting number the days as `WeekSpec` and `WeekDaySpec` do.
static_assert(static_cast<UInt8>(WeekFunctionsStartingDay::MONDAY) == 1);
static_assert(static_cast<UInt8>(WeekFunctionsStartingDay::SUNDAY) == 7);

WeekFunctionsSettings::WeekFunctionsSettings(const ContextPtr & context)
    : starting_day(context->getSettingsRef()[Setting::week_functions_starting_day])
    , range(context->getSettingsRef()[Setting::week_functions_range])
    , first_week_of_year(context->getSettingsRef()[Setting::week_functions_first_week_of_year])
{
}

UInt8 WeekFunctionsSettings::firstWeekday(UInt8 default_first_weekday) const
{
    return starting_day == WeekFunctionsStartingDay::AUTO ? default_first_weekday : static_cast<UInt8>(starting_day);
}

WeekSpec WeekFunctionsSettings::apply(WeekSpec spec) const
{
    spec.first_weekday = firstWeekday(spec.first_weekday);

    switch (range)
    {
        case WeekFunctionsRange::AUTO:
            break;
        case WeekFunctionsRange::ZERO_TO_53:
            spec.week_year = false;
            break;
        case WeekFunctionsRange::ONE_TO_53:
            spec.week_year = true;
            break;
    }

    switch (first_week_of_year)
    {
        case WeekFunctionsFirstWeekOfYear::AUTO:
            break;
        case WeekFunctionsFirstWeekOfYear::FIRST_FULL_WEEK:
            spec.first_week_rule = FirstWeekRule::FirstFullWeek;
            break;
        case WeekFunctionsFirstWeekOfYear::FOUR_OR_MORE_DAYS:
            spec.first_week_rule = FirstWeekRule::FourOrMoreDays;
            break;
        case WeekFunctionsFirstWeekOfYear::CONTAINS_JANUARY_1:
            spec.first_week_rule = FirstWeekRule::ContainsJanuary1;
            break;
    }

    return spec;
}

WeekDaySpec WeekFunctionsSettings::apply(WeekDaySpec spec) const
{
    spec.first_weekday = firstWeekday(spec.first_weekday);
    return spec;
}

}
