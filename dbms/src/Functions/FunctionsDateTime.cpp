#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsDateTime.h>

namespace DB
{

void registerFunctionsDateTime(FunctionFactory & factory)
{
	#define F [](const Context & context) -> IFunction*

	factory.registerFunction("toYear", 				F { return new FunctionToYear; });
	factory.registerFunction("toMonth",				F { return new FunctionToMonth; });
	factory.registerFunction("toDayOfMonth", 		F { return new FunctionToDayOfMonth; });
	factory.registerFunction("toDayOfWeek", 		F { return new FunctionToDayOfWeek; });
	factory.registerFunction("toHour", 				F { return new FunctionToHour; });
	factory.registerFunction("toMinute", 			F { return new FunctionToMinute; });
	factory.registerFunction("toSecond", 			F { return new FunctionToSecond; });
	factory.registerFunction("toMonday", 			F { return new FunctionToMonday; });
	factory.registerFunction("toStartOfMonth", 		F { return new FunctionToStartOfMonth; });
	factory.registerFunction("toStartOfQuarter", 	F { return new FunctionToStartOfQuarter; });
	factory.registerFunction("toStartOfYear", 		F { return new FunctionToStartOfYear; });
	factory.registerFunction("toStartOfMinute", 	F { return new FunctionToStartOfMinute; });
	factory.registerFunction("toStartOfHour", 		F { return new FunctionToStartOfHour; });
	factory.registerFunction("toRelativeYearNum", 	F { return new FunctionToRelativeYearNum; });
	factory.registerFunction("toRelativeMonthNum", 	F { return new FunctionToRelativeMonthNum; });
	factory.registerFunction("toRelativeWeekNum", 	F { return new FunctionToRelativeWeekNum; });
	factory.registerFunction("toRelativeDayNum", 	F { return new FunctionToRelativeDayNum; });
	factory.registerFunction("toRelativeHourNum", 	F { return new FunctionToRelativeHourNum; });
	factory.registerFunction("toRelativeMinuteNum", F { return new FunctionToRelativeMinuteNum; });
	factory.registerFunction("toRelativeSecondNum", F { return new FunctionToRelativeSecondNum; });
	factory.registerFunction("toTime", 				F { return new FunctionToTime; });
	factory.registerFunction("now", 				F { return new FunctionNow; });
	factory.registerFunction("timeSlot", 			F { return new FunctionTimeSlot; });
	factory.registerFunction("timeSlots", 			F { return new FunctionTimeSlots; });

	#undef F
}

}
