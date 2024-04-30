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
#include <Columns/ColumnVector.h>
#include <Interpreters/castColumn.h>

#include "Common/DateLUTImpl.h"
#include <Common/DateLUT.h>
#include <Common/typeid_cast.h>
#include "Columns/IColumn.h"
#include "DataTypes/IDataType.h"
#include "base/DayNum.h"

#include <array>
#include <memory>

namespace DB
{

namespace
{

enum ChangeDateFunctionsNames
{
    CHANGE_YEAR = 0,
    CHANGE_MONTH = 1,
    CHANGE_DAY = 2,
    CHANGE_HOUR = 3,
    CHANGE_MINUTE = 4,
    CHANGE_SECOND = 5
};

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
        FunctionArgumentDescriptors args{
                {mandatory_argument_names[0], &isDateOrDate32OrDateTimeOrDateTime64<IDataType>, nullptr, "Date"},
                {mandatory_argument_names[1], &isNumber<IDataType>, nullptr, "Number"}
            };
        validateFunctionArgumentTypes(*this, arguments, args);

        if (Traits::EnumName >= 3)
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
            if (Traits::EnumName >= 3)
                return execute<DataTypeDate, DataTypeDateTime>(arguments, input_type, result_type, input_rows_count);
            return execute<DataTypeDate, DataTypeDate>(arguments, input_type, result_type, input_rows_count);
        }
        if (isDate32(input_type))
        {
            if (Traits::EnumName >= 3)
                return executeDate32ToDateTime64(arguments, result_type, input_rows_count);
            return execute<DataTypeDate32, DataTypeDate32>(arguments, input_type, result_type, input_rows_count);
        }
        if (isDateTime(input_type))
            return execute<DataTypeDateTime, DataTypeDateTime>(arguments, input_type, result_type, input_rows_count);
        return executeDateTime64(arguments, result_type, input_rows_count);
    }


    template <typename DataType, typename ResultDataType>
    ColumnPtr execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & input_type, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        const auto & date_lut = DateLUT::instance();

        auto result_column = ResultDataType::ColumnType::create(input_rows_count);
        auto & result_data = result_column->getData();

        auto input_column = castColumn(arguments[0], std::make_shared<DataType>());
        input_column = input_column->convertToFullColumnIfConst();
        const auto & input_column_data = typeid_cast<const typename DataType::ColumnType &>(*input_column).getData();

        auto new_value_column = castColumn(arguments[1], std::make_shared<DataTypeFloat32>());
        new_value_column = new_value_column->convertToFullColumnIfConst();
        const auto & new_value_column_data = typeid_cast<const ColumnFloat32 &>(*new_value_column).getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            Int64 time;
            if (isDateOrDate32(input_type))
                time = static_cast<Int64>(date_lut.toNumYYYYMMDD(DayNum(input_column_data[i]))) * 1000'000;
            else
                time = date_lut.toNumYYYYMMDDhhmmss(input_column_data[i]);

            if (isDateOrDate32(result_type))
                result_data[i] = static_cast<Int32>(getChangedDate(time, new_value_column_data[i], result_type, date_lut));
            else
                result_data[i] = static_cast<UInt32>(getChangedDate(time, new_value_column_data[i], result_type, date_lut));
        }

        return result_column;
    }

    ColumnPtr executeDate32ToDateTime64(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        const auto & date_lut = DateLUT::instance();

        auto result_column = ColumnDateTime64::create(input_rows_count, DataTypeDateTime64::default_scale);
        auto & result_data = result_column->getData();

        auto input_column = arguments[0].column->convertToFullColumnIfConst();
        const auto & input_column_data = typeid_cast<const ColumnDate32 &>(*input_column).getData();

        auto new_value_column = castColumn(arguments[1], std::make_shared<DataTypeFloat32>());
        new_value_column = new_value_column->convertToFullColumnIfConst();
        const auto & new_value_column_data = typeid_cast<const ColumnFloat32 &>(*new_value_column).getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            Int64 time = static_cast<Int64>(date_lut.toNumYYYYMMDD(DayNum(input_column_data[i]))) * 1000'000;

            result_data[i] = getChangedDate(time, new_value_column_data[i], result_type, date_lut, 1000, 0);
        }

        return result_column;
    }

    ColumnPtr executeDateTime64(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
    {
        auto result_column = ColumnDateTime64::create(input_rows_count, DataTypeDateTime64::default_scale);
        auto & result_data = result_column->getData();

        auto input_column = arguments[0].column->convertToFullColumnIfConst();
        const auto & input_column_data = typeid_cast<const ColumnDateTime64 &>(*input_column).getData();

        auto new_value_column = castColumn(arguments[1], std::make_shared<DataTypeFloat32>());
        new_value_column = new_value_column->convertToFullColumnIfConst();
        const auto & new_value_column_data = typeid_cast<const ColumnFloat32 &>(*new_value_column).getData();

        const auto scale = typeid_cast<const DataTypeDateTime64 &>(*result_type).getScale();
        const auto & date_lut = typeid_cast<const DataTypeDateTime64 &>(*result_type).getTimeZone();
        Int64 deg = 1;
        for (size_t i = 0; i < scale; ++i)
        {
            deg *= 10;
        }

        for (size_t i = 0; i < input_rows_count; ++i)
        {            
            Int64 time = date_lut.toNumYYYYMMDDhhmmss(input_column_data[i] / deg);
            Int64 fraction = input_column_data[i] % deg;

            result_data[i] = getChangedDate(time, new_value_column_data[i], result_type, date_lut, deg, fraction);
        }

        return result_column;
    }

    Int64 getChangedDate(Int64 time, Float32 new_value, const DataTypePtr & result_type, const DateLUTImpl & date_lut, Int64 deg = 0, Int64 fraction = 0) const
    {
        auto year = time / 10'000'000'000;
        auto month = (time % 10'000'000'000) / 100'000'000;
        auto day = (time % 100'000'000) / 1000'000;
        auto hours = (time % 1000'000) / 10'000;
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
            min_date = date_lut.makeDateTime(1900, 1, 1, 0,0 , 0) * deg;
            max_date = date_lut.makeDateTime(2299, 12, 31, 23, 59, 59) * deg + (deg - 1);
            min_year = 1900;
            max_year = 2299;
        }

        Int8 fl = 0;

        switch (Traits::EnumName)
        {
            case CHANGE_YEAR:
                if (new_value < min_year)
                    fl = 1;
                else if (new_value > max_year)
                    fl = 2;
                year = static_cast<Int16>(new_value);
                break;
            case CHANGE_MONTH:
                if (new_value < 1 || new_value > 12)
                    fl = 1;
                month = static_cast<UInt8>(new_value);
                break;
            case CHANGE_DAY:
                if (new_value < 1 || new_value > 31)
                    fl = 1;
                day = static_cast<UInt8>(new_value);
                break;
            case CHANGE_HOUR:
                if (new_value < 0 || new_value > 23)
                    fl = 1;
                hours = static_cast<UInt8>(new_value);
                break;
            case CHANGE_MINUTE:
                if (new_value < 0 || new_value > 59)
                    fl = 1;
                minutes = static_cast<UInt8>(new_value);
                break;
            case CHANGE_SECOND:
                if (new_value < 0 || new_value > 59)
                    fl = 1;
                seconds = static_cast<UInt8>(new_value);
                break;
        }

        if (fl == 1)
            return min_date;

        if (fl == 2)
            return max_date;

        Int64 result;
        if (isDateOrDate32(result_type))
            result = date_lut.makeDayNum(year, month, day);
        else
        {
            if (isDateTime(result_type))
                result = date_lut.makeDateTime(year, month, day, hours, minutes, seconds);
            else
                result = date_lut.makeDateTime(year, month, day, hours, minutes, seconds) * deg + fraction;
        }


        if (result > max_date)
            return max_date;

        return result;
    }
};


struct ChangeYearTraits
{
    static constexpr auto Name = "changeYear";
    static constexpr auto EnumName = CHANGE_YEAR;
};

struct ChangeMonthTraits
{
    static constexpr auto Name = "changeMonth";
    static constexpr auto EnumName = CHANGE_MONTH;
};

struct ChangeDayTraits
{
    static constexpr auto Name = "changeDay";
    static constexpr auto EnumName = CHANGE_DAY;
};

struct ChangeHourTraits
{
    static constexpr auto Name = "changeHour";
    static constexpr auto EnumName = CHANGE_HOUR;
};

struct ChangeMinuteTraits
{
    static constexpr auto Name = "changeMinute";
    static constexpr auto EnumName = CHANGE_MINUTE;
};

struct ChangeSecondTraits
{
    static constexpr auto Name = "changeSecond";
    static constexpr auto EnumName = CHANGE_SECOND;
};


}

REGISTER_FUNCTION(ChangeDate)
{
    factory.registerFunction<FunctionChangeDate<ChangeYearTraits>>();
    factory.registerFunction<FunctionChangeDate<ChangeMonthTraits>>();
    factory.registerFunction<FunctionChangeDate<ChangeDayTraits>>();
    factory.registerFunction<FunctionChangeDate<ChangeHourTraits>>();
    factory.registerFunction<FunctionChangeDate<ChangeMinuteTraits>>();
    factory.registerFunction<FunctionChangeDate<ChangeSecondTraits>>();
}

}
