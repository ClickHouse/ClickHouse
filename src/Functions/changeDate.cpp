#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/castColumn.h>
#include "Common/DateLUTImpl.h"
#include "Common/Exception.h"
#include <Common/DateLUT.h>
#include <Common/typeid_cast.h>
#include "Columns/IColumn.h"
#include "DataTypes/IDataType.h"

#include <array>
#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

enum class ChangeDateFunctionsNames
{
    CHANGE_YEAR = 0,
    CHANGE_MONTH = 1,
    CHANGE_DAY = 2,
    CHANGE_HOUR = 3,
    CHANGE_MINUTE = 4,
    CHANGE_SECOND = 5
};

constexpr bool isTimeChange(const ChangeDateFunctionsNames & type)
{
    return type == ChangeDateFunctionsNames::CHANGE_HOUR ||
           type == ChangeDateFunctionsNames::CHANGE_MINUTE ||
           type == ChangeDateFunctionsNames::CHANGE_SECOND;
}

template <typename DataType>
constexpr bool isDate()
{
    return DataType::type_id == TypeIndex::Date;
}

template <typename DataType>
constexpr bool isDate32()
{
    return DataType::type_id == TypeIndex::Date32;
}

template <typename DataType>
constexpr bool isDateTime()
{
    return DataType::type_id == TypeIndex::DateTime;
}

template <typename DataType>
constexpr bool isDateTime64()
{
    return DataType::type_id == TypeIndex::DateTime64;
}


template <typename Traits>
class FunctionChangeDate : public IFunction
{
public:
    static constexpr auto name = Traits::Name;

    static constexpr std::array mandatory_argument_names = {"date", "new_value"};

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return mandatory_argument_names.size(); }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionChangeDate>(); }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires 2 parameters: date, new_value. Passed {}.", getName(), arguments.size());

        if (!isDateOrDate32OrDateTimeOrDateTime64(*arguments[0].type) || !isNumber(*arguments[1].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be Date(32) or DateTime(64), second - numeric", getName());

        if constexpr (isTimeChange(Traits::EnumName))
        {
            if (isDate(arguments[0].type))
                return std::make_shared<DataTypeDateTime>();
            if (isDate32(arguments[0].type))
                return std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale);
        }

        return arguments[0].type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & input_type = arguments[0].type;
        if (isDate(input_type))
        {
            if constexpr (isTimeChange(Traits::EnumName))
                return execute<DataTypeDate, DataTypeDateTime>(arguments, input_type, result_type, input_rows_count);
            return execute<DataTypeDate, DataTypeDate>(arguments, input_type, result_type, input_rows_count);
        }
        if (isDate32(input_type))
        {
            if constexpr (isTimeChange(Traits::EnumName))
                return execute<DataTypeDate32, DataTypeDateTime64>(arguments, input_type, result_type, input_rows_count);
            return execute<DataTypeDate32, DataTypeDate32>(arguments, input_type, result_type, input_rows_count);
        }
        if (isDateTime(input_type))
            return execute<DataTypeDateTime, DataTypeDateTime>(arguments, input_type, result_type, input_rows_count);
        return execute<DataTypeDateTime64, DataTypeDateTime64>(arguments, input_type, result_type, input_rows_count);
    }

    template <typename DataType, typename ResultDataType>
    ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & input_type, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        bool is_const = (isColumnConst(*arguments[0].column) && isColumnConst(*arguments[1].column));
        size_t result_rows_count = (is_const ? 1 : input_rows_count);

        typename ResultDataType::ColumnType::MutablePtr result_column;
        if constexpr (isDateTime64<ResultDataType>())
        {
            auto scale = DataTypeDateTime64::default_scale;
            if constexpr (isDateTime64<DataType>())
                scale = typeid_cast<const DataTypeDateTime64 &>(*result_type).getScale();
            result_column = ResultDataType::ColumnType::create(result_rows_count, scale);
        }
        else
            result_column = ResultDataType::ColumnType::create(result_rows_count);

        auto & result_data = result_column->getData();

        auto input_column = arguments[0].column->convertToFullIfNeeded();
        const auto & input_column_data = typeid_cast<const typename DataType::ColumnType &>(*input_column).getData();

        auto new_value_column = castColumn(arguments[1], std::make_shared<DataTypeFloat64>());
        new_value_column = new_value_column->convertToFullIfNeeded();
        const auto & new_value_column_data = typeid_cast<const ColumnFloat64 &>(*new_value_column).getData();

        for (size_t i = 0; i < result_rows_count; ++i)
        {
            if constexpr (isDateTime64<DataType>())
            {
                const auto scale = typeid_cast<const DataTypeDateTime64 &>(*result_type).getScale();
                const auto & date_lut = typeid_cast<const DataTypeDateTime64 &>(*result_type).getTimeZone();
                Int64 deg = 1;
                for (size_t j = 0; j < scale; ++j)
                    deg *= 10;

                Int64 time = date_lut.toNumYYYYMMDDhhmmss(input_column_data[i] / deg);
                Int64 fraction = input_column_data[i] % deg;

                result_data[i] = getChangedDate(time, new_value_column_data[i], result_type, date_lut, scale, fraction);
            }
            else if constexpr (isDate32<DataType>() && isDateTime64<ResultDataType>())
            {
                const auto & date_lut = typeid_cast<const DataTypeDateTime64 &>(*result_type).getTimeZone();
                Int64 time = static_cast<Int64>(date_lut.toNumYYYYMMDD(ExtendedDayNum(input_column_data[i]))) * 1'000'000;

                result_data[i] = getChangedDate(time, new_value_column_data[i], result_type, date_lut, 3, 0);
            }
            else if constexpr (isDate<DataType>() && isDateTime<ResultDataType>())
            {
                const auto & date_lut = typeid_cast<const DataTypeDateTime &>(*result_type).getTimeZone();
                Int64 time = static_cast<Int64>(date_lut.toNumYYYYMMDD(ExtendedDayNum(input_column_data[i]))) * 1'000'000;

                result_data[i] = static_cast<UInt32>(getChangedDate(time, new_value_column_data[i], result_type, date_lut));
            }
            else if constexpr (isDateTime<DataType>())
            {
                const auto & date_lut = typeid_cast<const DataTypeDateTime &>(*result_type).getTimeZone();
                Int64 time = date_lut.toNumYYYYMMDDhhmmss(input_column_data[i]);

                result_data[i] = static_cast<UInt32>(getChangedDate(time, new_value_column_data[i], result_type, date_lut));
            }
            else
            {
                const auto & date_lut = DateLUT::instance();
                Int64 time;
                if (isDate(input_type))
                    time = static_cast<Int64>(date_lut.toNumYYYYMMDD(DayNum(input_column_data[i]))) * 1'000'000;
                else
                    time = static_cast<Int64>(date_lut.toNumYYYYMMDD(ExtendedDayNum(input_column_data[i]))) * 1'000'000;

                if (isDate(result_type))
                    result_data[i] = static_cast<UInt16>(getChangedDate(time, new_value_column_data[i], result_type, date_lut));
                else
                    result_data[i] = static_cast<Int32>(getChangedDate(time, new_value_column_data[i], result_type, date_lut));
            }
        }

        if (is_const)
            return ColumnConst::create(std::move(result_column), input_rows_count);

        return result_column;
    }

    Int64 getChangedDate(Int64 time, Float64 new_value, const DataTypePtr & result_type, const DateLUTImpl & date_lut, Int64 scale = 0, Int64 fraction = 0) const
    {
        auto year = time / 10'000'000'000;
        auto month = (time % 10'000'000'000) / 100'000'000;
        auto day = (time % 100'000'000) / 1'000'000;
        auto hours = (time % 1'000'000) / 10'000;
        auto minutes = (time % 10'000) / 100;
        auto seconds = time % 100;

        Int64 min_date, max_date;
        Int16 min_year, max_year;
        if (isDate(result_type))
        {
            min_date = date_lut.makeDayNum(1970, 1, 1);
            max_date = date_lut.makeDayNum(2149, 6, 6);
            min_year = 1970;
            max_year = 2149;
        }
        else if (isDate32(result_type))
        {
            min_date = date_lut.makeDayNum(1900, 1, 1);
            max_date = date_lut.makeDayNum(2299, 12, 31);
            min_year = 1900;
            max_year = 2299;
        }
        else if (isDateTime(result_type))
        {
            min_date = 0;
            max_date = 0x0ffffffffll;
            min_year = 1970;
            max_year = 2106;
        }
        else
        {
            min_date = DecimalUtils::decimalFromComponents<DateTime64>(
                date_lut.makeDateTime(1900, 1, 1, 0, 0, 0),
                static_cast<Int64>(0),
                static_cast<UInt32>(scale));
            Int64 deg = 1;
            for (Int64 j = 0; j < scale; ++j)
                deg *= 10;
            max_date = DecimalUtils::decimalFromComponents<DateTime64>(
                date_lut.makeDateTime(2299, 12, 31, 23, 59, 59),
                static_cast<Int64>(deg - 1),
                static_cast<UInt32>(scale));
            min_year = 1900;
            max_year = 2299;
        }

        switch (Traits::EnumName)
        {
            case ChangeDateFunctionsNames::CHANGE_YEAR:
                if (new_value < min_year)
                    return min_date;
                else if (new_value > max_year)
                    return max_date;
                year = static_cast<Int16>(new_value);
                break;
            case ChangeDateFunctionsNames::CHANGE_MONTH:
                if (new_value < 1 || new_value > 12)
                    return min_date;
                month = static_cast<UInt8>(new_value);
                break;
            case ChangeDateFunctionsNames::CHANGE_DAY:
                if (new_value < 1 || new_value > 31)
                    return min_date;
                day = static_cast<UInt8>(new_value);
                break;
            case ChangeDateFunctionsNames::CHANGE_HOUR:
                if (new_value < 0 || new_value > 23)
                    return min_date;
                hours = static_cast<UInt8>(new_value);
                break;
            case ChangeDateFunctionsNames::CHANGE_MINUTE:
                if (new_value < 0 || new_value > 59)
                    return min_date;
                minutes = static_cast<UInt8>(new_value);
                break;
            case ChangeDateFunctionsNames::CHANGE_SECOND:
                if (new_value < 0 || new_value > 59)
                    return min_date;
                seconds = static_cast<UInt8>(new_value);
                break;
        }

        Int64 result;
        if (isDateOrDate32(result_type))
            result = date_lut.makeDayNum(year, month, day);
        else if (isDateTime(result_type))
            result = date_lut.makeDateTime(year, month, day, hours, minutes, seconds);
        else
            result = DecimalUtils::decimalFromComponents<DateTime64>(
                date_lut.makeDateTime(year, month, day, hours, minutes, seconds),
                static_cast<Int64>(fraction),
                static_cast<UInt32>(scale));

        if (result > max_date)
            return max_date;

        return result;
    }
};


struct ChangeYearTraits
{
    static constexpr auto Name = "changeYear";
    static constexpr auto EnumName = ChangeDateFunctionsNames::CHANGE_YEAR;
};

struct ChangeMonthTraits
{
    static constexpr auto Name = "changeMonth";
    static constexpr auto EnumName = ChangeDateFunctionsNames::CHANGE_MONTH;
};

struct ChangeDayTraits
{
    static constexpr auto Name = "changeDay";
    static constexpr auto EnumName = ChangeDateFunctionsNames::CHANGE_DAY;
};

struct ChangeHourTraits
{
    static constexpr auto Name = "changeHour";
    static constexpr auto EnumName = ChangeDateFunctionsNames::CHANGE_HOUR;
};

struct ChangeMinuteTraits
{
    static constexpr auto Name = "changeMinute";
    static constexpr auto EnumName = ChangeDateFunctionsNames::CHANGE_MINUTE;
};

struct ChangeSecondTraits
{
    static constexpr auto Name = "changeSecond";
    static constexpr auto EnumName = ChangeDateFunctionsNames::CHANGE_SECOND;
};

}

REGISTER_FUNCTION(ChangeDate)
{
    factory.registerFunction<FunctionChangeDate<ChangeYearTraits>>(
        FunctionDocumentation{
            .description = R"(
Changes the year of the given Date(32) or DateTime(64).
Returns the same type as the input data.
)",
            .categories{"Dates and Times"}
        }
    );
    factory.registerFunction<FunctionChangeDate<ChangeMonthTraits>>(
        FunctionDocumentation{
            .description = R"(
Same as changeYear function, but changes month of the date.
)",
            .categories{"Dates and Times"}
        }
    );
    factory.registerFunction<FunctionChangeDate<ChangeDayTraits>>(
        FunctionDocumentation{
            .description = R"(
Same as changeYear function, but changes day_of_month of the date.
)",
            .categories{"Dates and Times"}
        }
    );
    factory.registerFunction<FunctionChangeDate<ChangeHourTraits>>(
        FunctionDocumentation{
            .description = R"(
Changes the hour of the given Date(32) or DateTime(64).
If the input data is Date, return DateTime;
if the input data is Date32, return DateTime64;
In other cases returns the same type as the input data.
)",
            .categories{"Dates and Times"}
        }
    );
    factory.registerFunction<FunctionChangeDate<ChangeMinuteTraits>>(
        FunctionDocumentation{
            .description = R"(
Same as changeHour function, but changes minute of the date.
)",
            .categories{"Dates and Times"}
        }
    );
    factory.registerFunction<FunctionChangeDate<ChangeSecondTraits>>(
        FunctionDocumentation{
            .description = R"(
Same as changeHour function, but changes seconds of the date.
)",
            .categories{"Dates and Times"}
        }
    );
}

}
