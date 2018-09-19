#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsDateTime.h>

namespace DB
{


static std::string extractTimeZoneNameFromColumn(const IColumn & column)
{
    const ColumnConst * time_zone_column = checkAndGetColumnConst<ColumnString>(&column);

    if (!time_zone_column)
        throw Exception("Illegal column " + column.getName()
            + " of time zone argument of function, must be constant string",
            ErrorCodes::ILLEGAL_COLUMN);

    return time_zone_column->getValue<String>();
}


std::string extractTimeZoneNameFromFunctionArguments(const ColumnsWithTypeAndName & arguments, size_t time_zone_arg_num, size_t datetime_arg_num)
{
    /// Explicit time zone may be passed in last argument.
    if (arguments.size() == time_zone_arg_num + 1 && arguments[time_zone_arg_num].column)
    {
        return extractTimeZoneNameFromColumn(*arguments[time_zone_arg_num].column);
    }
    else
    {
        if (!arguments.size())
            return {};

        /// If time zone is attached to an argument of type DateTime.
        if (const DataTypeDateTime * type = checkAndGetDataType<DataTypeDateTime>(arguments[datetime_arg_num].type.get()))
            return type->getTimeZone().getTimeZone();

        return {};
    }
}

const DateLUTImpl & extractTimeZoneFromFunctionArguments(Block & block, const ColumnNumbers & arguments, size_t time_zone_arg_num, size_t datetime_arg_num)
{
    if (arguments.size() == time_zone_arg_num + 1)
        return DateLUT::instance(extractTimeZoneNameFromColumn(*block.getByPosition(arguments[time_zone_arg_num]).column));
    else
    {
        if (!arguments.size())
            return DateLUT::instance();

        /// If time zone is attached to an argument of type DateTime.
        if (const DataTypeDateTime * type = checkAndGetDataType<DataTypeDateTime>(block.getByPosition(arguments[datetime_arg_num]).type.get()))
            return type->getTimeZone();

        return DateLUT::instance();
    }
}

void registerFunctionsDateTime(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToYear>();
    factory.registerFunction<FunctionToQuarter>();
    factory.registerFunction<FunctionToMonth>();
    factory.registerFunction<FunctionToDayOfMonth>();
    factory.registerFunction<FunctionToDayOfWeek>();
    factory.registerFunction<FunctionToDayOfYear>();
    factory.registerFunction<FunctionToHour>();
    factory.registerFunction<FunctionToMinute>();
    factory.registerFunction<FunctionToSecond>();
    factory.registerFunction<FunctionToStartOfDay>();
    factory.registerFunction<FunctionToMonday>();
    factory.registerFunction<FunctionToISOWeek>();
    factory.registerFunction<FunctionToISOYear>();
    factory.registerFunction<FunctionToStartOfMonth>();
    factory.registerFunction<FunctionToStartOfQuarter>();
    factory.registerFunction<FunctionToStartOfYear>();
    factory.registerFunction<FunctionToStartOfMinute>();
    factory.registerFunction<FunctionToStartOfFiveMinute>();
    factory.registerFunction<FunctionToStartOfFifteenMinutes>();
    factory.registerFunction<FunctionToStartOfHour>();
    factory.registerFunction<FunctionToStartOfISOYear>();
    factory.registerFunction<FunctionToRelativeYearNum>();
    factory.registerFunction<FunctionToRelativeQuarterNum>();
    factory.registerFunction<FunctionToRelativeMonthNum>();
    factory.registerFunction<FunctionToRelativeWeekNum>();
    factory.registerFunction<FunctionToRelativeDayNum>();
    factory.registerFunction<FunctionToRelativeHourNum>();
    factory.registerFunction<FunctionToRelativeMinuteNum>();
    factory.registerFunction<FunctionToRelativeSecondNum>();
    factory.registerFunction<FunctionToTime>();
    factory.registerFunction<FunctionNow>(FunctionFactory::CaseInsensitive);
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

    factory.registerFunction<FunctionDateDiff>(FunctionFactory::CaseInsensitive);

    factory.registerFunction<FunctionToTimeZone>();


    factory.registerFunction<FunctionFormatDateTime>();
}

}
