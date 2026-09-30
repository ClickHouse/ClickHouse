#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/IFunctionDateOrDateTime.h>
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
    /// The name can arrive wrapped, e.g. from `now(toLowCardinality('UTC'))`. The column is absent
    /// when the argument is not a constant, and the check below reports that.
    const auto full_column = column ? column->convertToFullColumnIfLowCardinality() : nullptr;
    /// The callers that validate this argument - `now`, `now64`, `toTimezone` and the shared
    /// date/time helpers - accept `String` or `FixedString`, so accept both here as well.
    const ColumnConst * time_zone_column = full_column ? checkAndGetColumnConstStringOrFixedString(full_column.get()) : nullptr;

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

DataTypePtr getArgumentTypeWithTimeZone(const IFunctionBase & function, const IDataType & argument_type, const IColumn * time_zone_column)
{
    const auto * adaptor = typeid_cast<const FunctionToFunctionBaseAdaptor *>(&function);
    const bool takes_time_zone = (adaptor && dynamic_cast<const FunctionDateOrDateTimeBase *>(adaptor->getFunction().get()))
        || function.getName() == "toString";
    if (!takes_time_zone || !time_zone_column)
        return nullptr;

    const auto full_column = time_zone_column->convertToFullColumnIfLowCardinality();
    const auto * time_zone_const = checkAndGetColumnConstStringOrFixedString(full_column.get());
    if (!time_zone_const)
        return nullptr;
    const String time_zone = time_zone_const->getValue<String>();

    const IDataType * type = &argument_type;
    if (const auto * low_cardinality_type = typeid_cast<const DataTypeLowCardinality *>(type))
        type = low_cardinality_type->getDictionaryType().get();
    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type))
        type = nullable_type->getNestedType().get();

    if (typeid_cast<const DataTypeDateTime *>(type))
        return std::make_shared<DataTypeDateTime>(time_zone);
    if (const auto * date_time64 = typeid_cast<const DataTypeDateTime64 *>(type))
        return std::make_shared<DataTypeDateTime64>(date_time64->getScale(), time_zone);
    return nullptr;
}

}
