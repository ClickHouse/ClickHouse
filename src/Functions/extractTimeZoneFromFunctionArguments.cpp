#include <Columns/ColumnString.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


std::string extractTimeZoneNameFromColumn(const IColumn * column, const String & column_name)
{
    const ColumnConst * time_zone_column = checkAndGetColumnConst<ColumnString>(column);

    if (!time_zone_column)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                        "Illegal column {} of time zone argument of function, must be a constant string",
                        column_name);

    return time_zone_column->getValue<String>();
}


std::string extractTimeZoneNameFromFunctionArguments(const ColumnsWithTypeAndName & arguments, size_t time_zone_arg_num, size_t datetime_arg_num, bool allow_nonconst_timezone_arguments)
{
    /// Explicit time zone may be passed in last argument.
    if ((arguments.size() == time_zone_arg_num + 1)
       && (!allow_nonconst_timezone_arguments || arguments[time_zone_arg_num].column))
    {
        return extractTimeZoneNameFromColumn(arguments[time_zone_arg_num].column.get(), arguments[time_zone_arg_num].name);
    }

    if (arguments.size() <= datetime_arg_num)
        return {};

    const auto & dt_arg = arguments[datetime_arg_num].type.get();
    /// If time zone is attached to an argument of type DateTime.
    if (const auto * type = checkAndGetDataType<DataTypeDateTime>(dt_arg))
        return type->hasExplicitTimeZone() ? type->getTimeZone().getTimeZone() : std::string();
    if (const auto * type = checkAndGetDataType<DataTypeDateTime64>(dt_arg))
        return type->hasExplicitTimeZone() ? type->getTimeZone().getTimeZone() : std::string();

    return {};
}

const DateLUTImpl & extractTimeZoneFromFunctionArguments(const ColumnsWithTypeAndName & arguments, size_t time_zone_arg_num, size_t datetime_arg_num)
{
    if (arguments.size() == time_zone_arg_num + 1)
    {
        std::string time_zone = extractTimeZoneNameFromColumn(arguments[time_zone_arg_num].column.get(), arguments[time_zone_arg_num].name);
        if (time_zone.empty())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Provided time zone must be non-empty and be a valid time zone");
        return DateLUT::instance(time_zone);
    }

    if (arguments.size() <= datetime_arg_num)
        return DateLUT::instance();

    const auto & dt_arg = arguments[datetime_arg_num].type.get();
    /// If time zone is attached to an argument of type DateTime.
    if (const auto * type = checkAndGetDataType<DataTypeDateTime>(dt_arg))
        return type->getTimeZone();
    if (const auto * type = checkAndGetDataType<DataTypeDateTime64>(dt_arg))
        return type->getTimeZone();

    return DateLUT::instance();
}

}

