#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsDateTime.h>

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
}

}
