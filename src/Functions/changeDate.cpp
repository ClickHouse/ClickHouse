#include "Common/DateLUTImpl.h"
#include "Common/Exception.h"
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Common/DateLUT.h>
#include <Common/typeid_cast.h>
#include <Core/DecimalFunctions.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/castColumn.h>

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

enum class Component
{
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second
};

}

template <typename Traits>
class FunctionChangeDate : public IFunction
{
public:
    static constexpr auto name = Traits::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionChangeDate>(); }
    String getName() const override { return Traits::name; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"date_or_datetime", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isDateOrDate32OrDateTimeOrDateTime64), nullptr, "Date or date with time"},
            {"value", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNativeInteger), nullptr, "Integer"}
        };
        validateFunctionArguments(*this, arguments, args);

        const auto & input_type = arguments[0].type;

        if constexpr (Traits::component == Component::Hour || Traits::component == Component::Minute || Traits::component == Component::Second)
        {
            if (isDate(input_type))
                return std::make_shared<DataTypeDateTime>();
            if (isDate32(input_type))
                return std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale);
        }

        return input_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & input_type = arguments[0].type;
        if (isDate(input_type))
        {
            if constexpr (Traits::component == Component::Hour || Traits::component == Component::Minute || Traits::component == Component::Second)
                return execute<DataTypeDate, DataTypeDateTime>(arguments, input_type, result_type, input_rows_count);
            return execute<DataTypeDate, DataTypeDate>(arguments, input_type, result_type, input_rows_count);
        }
        if (isDate32(input_type))
        {
            if constexpr (Traits::component == Component::Hour || Traits::component == Component::Minute || Traits::component == Component::Second)
                return execute<DataTypeDate32, DataTypeDateTime64>(arguments, input_type, result_type, input_rows_count);
            return execute<DataTypeDate32, DataTypeDate32>(arguments, input_type, result_type, input_rows_count);
        }
        if (isDateTime(input_type))
            return execute<DataTypeDateTime, DataTypeDateTime>(arguments, input_type, result_type, input_rows_count);
        if (isDateTime64(input_type))
            return execute<DataTypeDateTime64, DataTypeDateTime64>(arguments, input_type, result_type, input_rows_count);

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid input type");
    }

    template <typename InputDataType, typename ResultDataType>
    ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & input_type, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        typename ResultDataType::ColumnType::MutablePtr result_col;
        if constexpr (std::is_same_v<ResultDataType, DataTypeDateTime64>)
        {
            auto scale = DataTypeDateTime64::default_scale;
            if constexpr (std::is_same_v<InputDataType, DateTime64>)
                scale = typeid_cast<const DataTypeDateTime64 &>(*result_type).getScale();
            result_col = ResultDataType::ColumnType::create(input_rows_count, scale);
        }
        else
            result_col = ResultDataType::ColumnType::create(input_rows_count);

        auto date_time_col = arguments[0].column->convertToFullIfNeeded();
        const auto & date_time_col_data = typeid_cast<const typename InputDataType::ColumnType &>(*date_time_col).getData();

        auto value_col = castColumn(arguments[1], std::make_shared<DataTypeFloat64>());
        value_col = value_col->convertToFullIfNeeded();
        const auto & value_col_data = typeid_cast<const ColumnFloat64 &>(*value_col).getData();

        auto & result_col_data = result_col->getData();

        if constexpr (std::is_same_v<InputDataType, DataTypeDateTime64>)
        {
            const auto scale = typeid_cast<const DataTypeDateTime64 &>(*result_type).getScale();
            const auto & date_lut = typeid_cast<const DataTypeDateTime64 &>(*result_type).getTimeZone();

            Int64 deg = 1;
            for (size_t j = 0; j < scale; ++j)
                deg *= 10;

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                Int64 time = date_lut.toNumYYYYMMDDhhmmss(date_time_col_data[i] / deg);
                Int64 fraction = date_time_col_data[i] % deg;

                result_col_data[i] = getChangedDate(time, value_col_data[i], result_type, date_lut, scale, fraction);
            }
        }
        else if constexpr (std::is_same_v<InputDataType, DataTypeDate32> && std::is_same_v<ResultDataType, DataTypeDateTime64>)
        {
            const auto & date_lut = typeid_cast<const DataTypeDateTime64 &>(*result_type).getTimeZone();
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                Int64 time = static_cast<Int64>(date_lut.toNumYYYYMMDD(ExtendedDayNum(date_time_col_data[i]))) * 1'000'000;
                result_col_data[i] = getChangedDate(time, value_col_data[i], result_type, date_lut, 3, 0);
            }
        }
        else if constexpr (std::is_same_v<InputDataType, DataTypeDate> && std::is_same_v<ResultDataType, DataTypeDateTime>)
        {
            const auto & date_lut = typeid_cast<const DataTypeDateTime &>(*result_type).getTimeZone();
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                Int64 time = static_cast<Int64>(date_lut.toNumYYYYMMDD(ExtendedDayNum(date_time_col_data[i]))) * 1'000'000;
                result_col_data[i] = static_cast<UInt32>(getChangedDate(time, value_col_data[i], result_type, date_lut));
            }
        }
        else if constexpr (std::is_same_v<InputDataType, DataTypeDateTime>)
        {
            const auto & date_lut = typeid_cast<const DataTypeDateTime &>(*result_type).getTimeZone();
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                Int64 time = date_lut.toNumYYYYMMDDhhmmss(date_time_col_data[i]);
                result_col_data[i] = static_cast<UInt32>(getChangedDate(time, value_col_data[i], result_type, date_lut));
            }
        }
        else
        {
            const auto & date_lut = DateLUT::instance();
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                Int64 time;
                if (isDate(input_type))
                    time = static_cast<Int64>(date_lut.toNumYYYYMMDD(DayNum(date_time_col_data[i]))) * 1'000'000;
                else
                    time = static_cast<Int64>(date_lut.toNumYYYYMMDD(ExtendedDayNum(date_time_col_data[i]))) * 1'000'000;

                if (isDate(result_type))
                    result_col_data[i] = static_cast<UInt16>(getChangedDate(time, value_col_data[i], result_type, date_lut));
                else
                    result_col_data[i] = static_cast<Int32>(getChangedDate(time, value_col_data[i], result_type, date_lut));
            }
        }

        return result_col;
    }

    Int64 getChangedDate(Int64 time, Float64 new_value, const DataTypePtr & result_type, const DateLUTImpl & date_lut, Int64 scale = 0, Int64 fraction = 0) const
    {
        auto year = time / 10'000'000'000;
        auto month = (time % 10'000'000'000) / 100'000'000;
        auto day = (time % 100'000'000) / 1'000'000;
        auto hours = (time % 1'000'000) / 10'000;
        auto minutes = (time % 10'000) / 100;
        auto seconds = time % 100;

        Int64 min_date = 0;
        Int64 max_date = 0;
        Int16 min_year;
        Int16 max_year;
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
            max_date = 0x0FFFFFFFFLL;
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

        switch (Traits::component)
        {
            case Component::Year:
                if (new_value < min_year)
                    return min_date;
                else if (new_value > max_year)
                    return max_date;
                year = static_cast<Int16>(new_value);
                break;
            case Component::Month:
                if (new_value < 1 || new_value > 12)
                    return min_date;
                month = static_cast<UInt8>(new_value);
                break;
            case Component::Day:
                if (new_value < 1 || new_value > 31)
                    return min_date;
                day = static_cast<UInt8>(new_value);
                break;
            case Component::Hour:
                if (new_value < 0 || new_value > 23)
                    return min_date;
                hours = static_cast<UInt8>(new_value);
                break;
            case Component::Minute:
                if (new_value < 0 || new_value > 59)
                    return min_date;
                minutes = static_cast<UInt8>(new_value);
                break;
            case Component::Second:
                if (new_value < 0 || new_value > 59)
                    return min_date;
                seconds = static_cast<UInt8>(new_value);
                break;
        }

        Int64 result;
        if (isDate(result_type) || isDate32(result_type))
            result = date_lut.makeDayNum(year, month, day);
        else if (isDateTime(result_type))
            result = date_lut.makeDateTime(year, month, day, hours, minutes, seconds);
        else
#ifndef __clang_analyzer__
            /// ^^ This looks funny. It is the least terrible suppression of a false positive reported by clang-analyzer (a sub-class
            /// of clang-tidy checks) deep down in 'decimalFromComponents'. Usual suppressions of the form NOLINT* don't work here (they
            /// would only affect code in _this_ file), and suppressing the issue in 'decimalFromComponents' may suppress true positives.
            result = DecimalUtils::decimalFromComponents<DateTime64>(
                date_lut.makeDateTime(year, month, day, hours, minutes, seconds),
                fraction,
                static_cast<UInt32>(scale));
#else
        {
            UNUSED(fraction);
            result = 0;
        }
#endif

        if (result < min_date)
            return min_date;

        if (result > max_date)
            return max_date;

        return result;
    }
};


struct ChangeYearTraits
{
    static constexpr auto name = "changeYear";
    static constexpr auto component = Component::Year;
};

struct ChangeMonthTraits
{
    static constexpr auto name = "changeMonth";
    static constexpr auto component = Component::Month;
};

struct ChangeDayTraits
{
    static constexpr auto name = "changeDay";
    static constexpr auto component = Component::Day;
};

struct ChangeHourTraits
{
    static constexpr auto name = "changeHour";
    static constexpr auto component = Component::Hour;
};

struct ChangeMinuteTraits
{
    static constexpr auto name = "changeMinute";
    static constexpr auto component = Component::Minute;
};

struct ChangeSecondTraits
{
    static constexpr auto name = "changeSecond";
    static constexpr auto component = Component::Second;
};

REGISTER_FUNCTION(ChangeDate)
{
    {
        FunctionDocumentation::Description description = "Changes the year component of a date or date time.";
        FunctionDocumentation::Syntax syntax = "changeYear(date_or_datetime, value);";
        FunctionDocumentation::Arguments arguments = {
            {"date_or_datetime", "The value to change.", {"Date", "Date32", "DateTime", "DateTime64"}},
            {"value", "The new value.", {"(U)Int*"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a value of the same type as `date_or_datetime` with modified year component.", {"Date", "Date32", "DateTime", "DateTime64"}};
        FunctionDocumentation::Examples example = {
            {"Usage example", "SELECT changeYear('2024-01-01'::DateTime, 2023)", "2023-01-01 00:00:00"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {24, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
        FunctionDocumentation function_documentation = {description, syntax, arguments, returned_value, example, introduced_in, category};
        factory.registerFunction<FunctionChangeDate<ChangeYearTraits>>(function_documentation);
    }
    {
        FunctionDocumentation::Description description = "Changes the month component of a date or date time.";
        FunctionDocumentation::Syntax syntax = "changeMonth(date_or_datetime, value);";
        FunctionDocumentation::Arguments arguments = {
            {"date_or_datetime", "The value to change.", {"Date", "Date32", "DateTime", "DateTime64"}},
            {"value", "The new value.", {"(U)Int*"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a value of the same type as `date_or_datetime` with modified month component.", {"Date", "Date32", "DateTime", "DateTime64"}};
        FunctionDocumentation::Examples example = {
            {"Usage example", "SELECT changeMonth('2024-01-01'::DateTime, 12)", "2024-12-01 00:00:00"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {24, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
        FunctionDocumentation function_documentation = {description, syntax, arguments, returned_value, example, introduced_in, category};
        factory.registerFunction<FunctionChangeDate<ChangeMonthTraits>>(function_documentation);
    }
    {
        FunctionDocumentation::Description description = "Changes the day component of a date or date time.";
        FunctionDocumentation::Syntax syntax = "changeDay(date_or_datetime, value);";
        FunctionDocumentation::Arguments arguments = {
            {"date_or_datetime", "The value to change.", {"Date", "Date32", "DateTime", "DateTime64"}},
            {"value", "The new value.", {"(U)Int*"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a value of the same type as `date_or_datetime` with modified day component.", {"Date", "Date32", "DateTime", "DateTime64"}};
        FunctionDocumentation::Examples example = {
            {"Usage example", "SELECT changeDay('2024-01-31'::DateTime, 15)", "2024-01-15 00:00:00"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {24, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
        FunctionDocumentation function_documentation = {description, syntax, arguments, returned_value, example, introduced_in, category};
        factory.registerFunction<FunctionChangeDate<ChangeDayTraits>>(function_documentation);
    }
    {
        FunctionDocumentation::Description description = "Changes the hour component of a date or date time.";
        FunctionDocumentation::Syntax syntax = "changeHour(date_or_datetime, value);";
        FunctionDocumentation::Arguments arguments = {
            {"date_or_datetime", "The value to change.", {"Date", "Date32", "DateTime", "DateTime64"}},
            {"value", "The new value.", {"(U)Int*"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a value of the same type as `date_or_datetime` with modified hour component.", {"DateTime", "DateTime64"}};
        FunctionDocumentation::Examples example = {
            {"Usage example", "SELECT changeHour('2024-01-01 12:00:00'::DateTime, 5)", "2024-01-01 05:00:00"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {24, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
        FunctionDocumentation function_documentation = {description, syntax, arguments, returned_value, example, introduced_in, category};
        factory.registerFunction<FunctionChangeDate<ChangeHourTraits>>(function_documentation);
    }
    {
        FunctionDocumentation::Description description = "Changes the minute component of a `date or date time`.";
        FunctionDocumentation::Syntax syntax = "changeMinute(date_or_datetime, value);";
        FunctionDocumentation::Arguments arguments = {
            {"date_or_datetime", "The value to change.", {"Date", "Date32", "DateTime", "DateTime64"}},
            {"value", "The new value.", {"(U)Int*"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a value of the same type as `date_or_datetime` with modified minute component.", {"DateTime", "DateTime64"}};
        FunctionDocumentation::Examples example = {
            {"Usage example", "SELECT changeMinute('2024-01-01 12:30:00'::DateTime, 45)", "2024-01-01 12:45:00"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {24, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
        FunctionDocumentation function_documentation = {description, syntax, arguments, returned_value, example, introduced_in, category};
        factory.registerFunction<FunctionChangeDate<ChangeMinuteTraits>>(function_documentation);
    }
    {
        FunctionDocumentation::Description description = "Changes the second component of a date or date time.";
        FunctionDocumentation::Syntax syntax = "changeSecond(date_or_datetime, value);";
        FunctionDocumentation::Arguments arguments = {
            {"date_or_datetime", "The value to change.", {"Date", "Date32", "DateTime", "DateTime64"}},
            {"value", "The new value.", {"(U)Int*"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"Returns a value of the same type as `date_or_datetime` with modified seconds component.", {"DateTime", "DateTime64"}};
        FunctionDocumentation::Examples example = {
            {"Usage example", "SELECT changeSecond('2024-01-01 12:30:45'::DateTime, 15)", "2024-01-01 12:30:15"}
        };
        FunctionDocumentation::IntroducedIn introduced_in = {24, 7};
        FunctionDocumentation::Category category = FunctionDocumentation::Category::DateAndTime;
        FunctionDocumentation function_documentation = {description, syntax, arguments, returned_value, example, introduced_in, category};
        factory.registerFunction<FunctionChangeDate<ChangeSecondTraits>>(function_documentation);
    }
}

}
