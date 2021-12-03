#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/FunctionHelpers.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <Columns/ColumnString.h>
#include <common/DateLUT.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


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
        if (arguments.empty())
            return {};

        /// If time zone is attached to an argument of type DateTime.
        if (const auto * type = checkAndGetDataType<DataTypeDateTime>(arguments[datetime_arg_num].type.get()))
            return type->getTimeZone().getTimeZone();
        if (const auto * type = checkAndGetDataType<DataTypeDateTime64>(arguments[datetime_arg_num].type.get()))
            return type->getTimeZone().getTimeZone();

        return {};
    }
}

const DateLUTImpl & extractTimeZoneFromFunctionArguments(const ColumnsWithTypeAndName & arguments, size_t time_zone_arg_num, size_t datetime_arg_num)
{
    if (arguments.size() == time_zone_arg_num + 1)
    {
        std::string time_zone = extractTimeZoneNameFromColumn(*arguments[time_zone_arg_num].column);
        if (time_zone.empty())
            throw Exception("Provided time zone must be non-empty and be a valid time zone", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return DateLUT::instance(time_zone);
    }
    else
    {
        if (arguments.empty())
            return DateLUT::instance();

        /// If time zone is attached to an argument of type DateTime.
        if (const auto * type = checkAndGetDataType<DataTypeDateTime>(arguments[datetime_arg_num].type.get()))
            return type->getTimeZone();
        if (const auto * type = checkAndGetDataType<DataTypeDateTime64>(arguments[datetime_arg_num].type.get()))
            return type->getTimeZone();

        return DateLUT::instance();
    }
}

}

