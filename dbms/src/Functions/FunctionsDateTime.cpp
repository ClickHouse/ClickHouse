#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsDateTime.h>

namespace DB
{

void registerFunctionsDateTime(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToYear>();
    factory.registerFunction<FunctionToMonth>();
    factory.registerFunction<FunctionToDayOfMonth>();
    factory.registerFunction<FunctionToDayOfWeek>();
    factory.registerFunction<FunctionToHour>();
    factory.registerFunction<FunctionToMinute>();
    factory.registerFunction<FunctionToSecond>();
    factory.registerFunction<FunctionToStartOfDay>();
    factory.registerFunction<FunctionToMonday>();
    factory.registerFunction<FunctionToStartOfMonth>();
    factory.registerFunction<FunctionToStartOfQuarter>();
    factory.registerFunction<FunctionToStartOfYear>();
    factory.registerFunction<FunctionToStartOfMinute>();
    factory.registerFunction<FunctionToStartOfFiveMinute>();
    factory.registerFunction<FunctionToStartOfHour>();
    factory.registerFunction<FunctionToRelativeYearNum>();
    factory.registerFunction<FunctionToRelativeMonthNum>();
    factory.registerFunction<FunctionToRelativeWeekNum>();
    factory.registerFunction<FunctionToRelativeDayNum>();
    factory.registerFunction<FunctionToRelativeHourNum>();
    factory.registerFunction<FunctionToRelativeMinuteNum>();
    factory.registerFunction<FunctionToRelativeSecondNum>();
    factory.registerFunction<FunctionToTime>();
    factory.registerFunction<FunctionNow>();
    factory.registerFunction<FunctionToday>();
    factory.registerFunction<FunctionYesterday>();
    factory.registerFunction<FunctionTimeSlot>();
    factory.registerFunction<FunctionTimeSlots>();
    factory.registerFunction<FunctionToYYYYMM>();
    factory.registerFunction<FunctionToYYYYMMDD>();
    factory.registerFunction<FunctionToYYYYMMDDhhmmss>();

    factory.registerFunction<FunctionAddSeconds>();
    factory.registerFunction<FunctionAddMinutes>();
    factory.registerFunction<FunctionAddHours>();
    factory.registerFunction<FunctionAddDays>();
    factory.registerFunction<FunctionAddWeeks>();
    factory.registerFunction<FunctionAddMonths>();
    factory.registerFunction<FunctionAddYears>();

    factory.registerFunction<FunctionSubtractSeconds>();
    factory.registerFunction<FunctionSubtractMinutes>();
    factory.registerFunction<FunctionSubtractHours>();
    factory.registerFunction<FunctionSubtractDays>();
    factory.registerFunction<FunctionSubtractWeeks>();
    factory.registerFunction<FunctionSubtractMonths>();
    factory.registerFunction<FunctionSubtractYears>();
}

}
