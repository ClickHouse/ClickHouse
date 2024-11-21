#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/IntervalKind.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>
#include <Formats/FormatSettings.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/extractTimeZoneFromFunctionArguments.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/TransformDateTime64.h>

#include <IO/WriteHelpers.h>

#include <base/find_symbols.h>

#include <type_traits>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

namespace
{

template <bool is_diff>
class DateDiffImpl
{
public:
    using ColumnDateTime64 = ColumnDecimal<DateTime64>;

    explicit DateDiffImpl(const String & name_) : name(name_) {}

    template <typename Transform>
    void dispatchForColumns(
        const IColumn & col_x, const IColumn & col_y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        size_t input_rows_count,
        ColumnInt64::Container & result) const
    {
        if (const auto * x_vec_16 = checkAndGetColumn<ColumnDate>(&col_x))
            dispatchForSecondColumn<Transform>(*x_vec_16, col_y, timezone_x, timezone_y, input_rows_count, result);
        else if (const auto * x_vec_32 = checkAndGetColumn<ColumnDateTime>(&col_x))
            dispatchForSecondColumn<Transform>(*x_vec_32, col_y, timezone_x, timezone_y, input_rows_count, result);
        else if (const auto * x_vec_32_s = checkAndGetColumn<ColumnDate32>(&col_x))
            dispatchForSecondColumn<Transform>(*x_vec_32_s, col_y, timezone_x, timezone_y, input_rows_count, result);
        else if (const auto * x_vec_64 = checkAndGetColumn<ColumnDateTime64>(&col_x))
            dispatchForSecondColumn<Transform>(*x_vec_64, col_y, timezone_x, timezone_y, input_rows_count, result);
        else if (const auto * x_const_16 = checkAndGetColumnConst<ColumnDate>(&col_x))
            dispatchConstForSecondColumn<Transform>(x_const_16->getValue<UInt16>(), col_y, timezone_x, timezone_y, input_rows_count, result);
        else if (const auto * x_const_32 = checkAndGetColumnConst<ColumnDateTime>(&col_x))
            dispatchConstForSecondColumn<Transform>(x_const_32->getValue<UInt32>(), col_y, timezone_x, timezone_y, input_rows_count, result);
        else if (const auto * x_const_32_s = checkAndGetColumnConst<ColumnDate32>(&col_x))
            dispatchConstForSecondColumn<Transform>(x_const_32_s->getValue<Int32>(), col_y, timezone_x, timezone_y, input_rows_count, result);
        else if (const auto * x_const_64 = checkAndGetColumnConst<ColumnDateTime64>(&col_x))
            dispatchConstForSecondColumn<Transform>(x_const_64->getValue<DecimalField<DateTime64>>(), col_y, timezone_x, timezone_y, input_rows_count, result);
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column for first argument of function {}, must be Date, Date32, DateTime or DateTime64", name);
    }

    template <typename Transform, typename LeftColumnType>
    void dispatchForSecondColumn(
        const LeftColumnType & x, const IColumn & col_y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        size_t input_rows_count,
        ColumnInt64::Container & result) const
    {
        if (const auto * y_vec_16 = checkAndGetColumn<ColumnDate>(&col_y))
            vectorVector<Transform>(x, *y_vec_16, timezone_x, timezone_y, input_rows_count, result);
        else if (const auto * y_vec_32 = checkAndGetColumn<ColumnDateTime>(&col_y))
            vectorVector<Transform>(x, *y_vec_32, timezone_x, timezone_y, input_rows_count, result);
        else if (const auto * y_vec_32_s = checkAndGetColumn<ColumnDate32>(&col_y))
            vectorVector<Transform>(x, *y_vec_32_s, timezone_x, timezone_y, input_rows_count, result);
        else if (const auto * y_vec_64 = checkAndGetColumn<ColumnDateTime64>(&col_y))
            vectorVector<Transform>(x, *y_vec_64, timezone_x, timezone_y, input_rows_count, result);
        else if (const auto * y_const_16 = checkAndGetColumnConst<ColumnDate>(&col_y))
            vectorConstant<Transform>(x, y_const_16->getValue<UInt16>(), timezone_x, timezone_y, input_rows_count, result);
        else if (const auto * y_const_32 = checkAndGetColumnConst<ColumnDateTime>(&col_y))
            vectorConstant<Transform>(x, y_const_32->getValue<UInt32>(), timezone_x, timezone_y, input_rows_count, result);
        else if (const auto * y_const_32_s = checkAndGetColumnConst<ColumnDate32>(&col_y))
            vectorConstant<Transform>(x, y_const_32_s->getValue<Int32>(), timezone_x, timezone_y, input_rows_count, result);
        else if (const auto * y_const_64 = checkAndGetColumnConst<ColumnDateTime64>(&col_y))
            vectorConstant<Transform>(x, y_const_64->getValue<DecimalField<DateTime64>>(), timezone_x, timezone_y, input_rows_count, result);
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column for second argument of function {}, must be Date, Date32, DateTime or DateTime64", name);
    }

    template <typename Transform, typename T1>
    void dispatchConstForSecondColumn(
        T1 x, const IColumn & col_y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        size_t input_rows_count,
        ColumnInt64::Container & result) const
    {
        if (const auto * y_vec_16 = checkAndGetColumn<ColumnDate>(&col_y))
            constantVector<Transform>(x, *y_vec_16, timezone_x, timezone_y, input_rows_count, result);
        else if (const auto * y_vec_32 = checkAndGetColumn<ColumnDateTime>(&col_y))
            constantVector<Transform>(x, *y_vec_32, timezone_x, timezone_y, input_rows_count, result);
        else if (const auto * y_vec_32_s = checkAndGetColumn<ColumnDate32>(&col_y))
            constantVector<Transform>(x, *y_vec_32_s, timezone_x, timezone_y, input_rows_count, result);
        else if (const auto * y_vec_64 = checkAndGetColumn<ColumnDateTime64>(&col_y))
            constantVector<Transform>(x, *y_vec_64, timezone_x, timezone_y, input_rows_count, result);
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column for second argument of function {}, must be Date, Date32, DateTime or DateTime64", name);
    }

    template <typename Transform, typename LeftColumnType, typename RightColumnType>
    void vectorVector(
        const LeftColumnType & x, const RightColumnType & y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        size_t input_rows_count,
        ColumnInt64::Container & result) const
    {
        const auto & x_data = x.getData();
        const auto & y_data = y.getData();

        const auto transform_x = TransformDateTime64<Transform>(getScale(x));
        const auto transform_y = TransformDateTime64<Transform>(getScale(y));
        for (size_t i = 0; i < input_rows_count; ++i)
            result[i] = calculate(transform_x, transform_y, x_data[i], y_data[i], timezone_x, timezone_y);
    }

    template <typename Transform, typename LeftColumnType, typename T2>
    void vectorConstant(
        const LeftColumnType & x, T2 y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        size_t input_rows_count,
        ColumnInt64::Container & result) const
    {
        const auto & x_data = x.getData();
        const auto transform_x = TransformDateTime64<Transform>(getScale(x));
        const auto transform_y = TransformDateTime64<Transform>(getScale(y));
        const auto y_value = stripDecimalFieldValue(y);

        for (size_t i = 0; i < input_rows_count; ++i)
            result[i] = calculate(transform_x, transform_y, x_data[i], y_value, timezone_x, timezone_y);
    }

    template <typename Transform, typename T1, typename RightColumnType>
    void constantVector(
        T1 x, const RightColumnType & y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        size_t input_rows_count,
        ColumnInt64::Container & result) const
    {
        const auto & y_data = y.getData();
        const auto transform_x = TransformDateTime64<Transform>(getScale(x));
        const auto transform_y = TransformDateTime64<Transform>(getScale(y));
        const auto x_value = stripDecimalFieldValue(x);

        for (size_t i = 0; i < input_rows_count; ++i)
            result[i] = calculate(transform_x, transform_y, x_value, y_data[i], timezone_x, timezone_y);
    }

    template <typename TransformX, typename TransformY, typename T1, typename T2>
    Int64 calculate(const TransformX & transform_x, const TransformY & transform_y, T1 x, T2 y, const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y) const
    {
        auto res =  static_cast<Int64>(transform_y.execute(y, timezone_y)) - static_cast<Int64>(transform_x.execute(x, timezone_x));

        if constexpr (is_diff)
        {
            return res;
        }
        else
        {
            /// Adjust res:
            DateTimeComponentsWithFractionalPart a_comp;
            DateTimeComponentsWithFractionalPart b_comp;
            Int64 adjust_value;
            auto x_nanoseconds = TransformDateTime64<ToRelativeSubsecondNumImpl<nanosecond_multiplier>>(transform_x.getScaleMultiplier()).execute(x, timezone_x);
            auto y_nanoseconds = TransformDateTime64<ToRelativeSubsecondNumImpl<nanosecond_multiplier>>(transform_y.getScaleMultiplier()).execute(y, timezone_y);

            if (x_nanoseconds <= y_nanoseconds)
            {
                a_comp = TransformDateTime64<ToDateTimeComponentsImpl>(transform_x.getScaleMultiplier()).execute(x, timezone_x);
                b_comp = TransformDateTime64<ToDateTimeComponentsImpl>(transform_y.getScaleMultiplier()).execute(y, timezone_y);
                adjust_value = -1;
            }
            else
            {
                a_comp = TransformDateTime64<ToDateTimeComponentsImpl>(transform_y.getScaleMultiplier()).execute(y, timezone_y);
                b_comp = TransformDateTime64<ToDateTimeComponentsImpl>(transform_x.getScaleMultiplier()).execute(x, timezone_x);
                adjust_value = 1;
            }

            if constexpr (std::is_same_v<TransformX, TransformDateTime64<ToRelativeYearNumImpl<ResultPrecision::Extended>>>)
            {
                if ((a_comp.date.month > b_comp.date.month)
                    || ((a_comp.date.month == b_comp.date.month) && ((a_comp.date.day > b_comp.date.day)
                    || ((a_comp.date.day == b_comp.date.day) && ((a_comp.time.hour > b_comp.time.hour)
                    || ((a_comp.time.hour == b_comp.time.hour) && ((a_comp.time.minute > b_comp.time.minute)
                    || ((a_comp.time.minute == b_comp.time.minute) && ((a_comp.time.second > b_comp.time.second)
                    || ((a_comp.time.second == b_comp.time.second) && ((a_comp.millisecond > b_comp.millisecond)
                    || ((a_comp.millisecond == b_comp.millisecond) && ((a_comp.microsecond > b_comp.microsecond)
                    || ((a_comp.microsecond == b_comp.microsecond) && (a_comp.nanosecond > b_comp.nanosecond)))))))))))))))
                    res += adjust_value;
            }
            else if constexpr (std::is_same_v<TransformX, TransformDateTime64<ToRelativeQuarterNumImpl<ResultPrecision::Extended>>>)
            {
                auto x_month_in_quarter = (a_comp.date.month - 1) % 3;
                auto y_month_in_quarter = (b_comp.date.month - 1) % 3;
                if ((x_month_in_quarter > y_month_in_quarter)
                    || ((x_month_in_quarter == y_month_in_quarter) && ((a_comp.date.day > b_comp.date.day)
                    || ((a_comp.date.day == b_comp.date.day) && ((a_comp.time.hour > b_comp.time.hour)
                    || ((a_comp.time.hour == b_comp.time.hour) && ((a_comp.time.minute > b_comp.time.minute)
                    || ((a_comp.time.minute == b_comp.time.minute) && ((a_comp.time.second > b_comp.time.second)
                    || ((a_comp.time.second == b_comp.time.second) && ((a_comp.millisecond > b_comp.millisecond)
                    || ((a_comp.millisecond == b_comp.millisecond) && ((a_comp.microsecond > b_comp.microsecond)
                    || ((a_comp.microsecond == b_comp.microsecond) && (a_comp.nanosecond > b_comp.nanosecond)))))))))))))))
                    res += adjust_value;
            }
            else if constexpr (std::is_same_v<TransformX, TransformDateTime64<ToRelativeMonthNumImpl<ResultPrecision::Extended>>>)
            {
                if ((a_comp.date.day > b_comp.date.day)
                    || ((a_comp.date.day == b_comp.date.day) && ((a_comp.time.hour > b_comp.time.hour)
                    || ((a_comp.time.hour == b_comp.time.hour) && ((a_comp.time.minute > b_comp.time.minute)
                    || ((a_comp.time.minute == b_comp.time.minute) && ((a_comp.time.second > b_comp.time.second)
                    || ((a_comp.time.second == b_comp.time.second) && ((a_comp.millisecond > b_comp.millisecond)
                    || ((a_comp.millisecond == b_comp.millisecond) && ((a_comp.microsecond > b_comp.microsecond)
                    || ((a_comp.microsecond == b_comp.microsecond) && (a_comp.nanosecond > b_comp.nanosecond)))))))))))))
                    res += adjust_value;
            }
            else if constexpr (std::is_same_v<TransformX, TransformDateTime64<ToRelativeWeekNumImpl<ResultPrecision::Extended>>>)
            {
                auto x_day_of_week = TransformDateTime64<ToDayOfWeekImpl>(transform_x.getScaleMultiplier()).execute(x, 0, timezone_x);
                auto y_day_of_week = TransformDateTime64<ToDayOfWeekImpl>(transform_y.getScaleMultiplier()).execute(y, 0, timezone_y);
                if ((x_day_of_week > y_day_of_week)
                    || ((x_day_of_week == y_day_of_week) && (a_comp.time.hour > b_comp.time.hour))
                    || ((a_comp.time.hour == b_comp.time.hour) && ((a_comp.time.minute > b_comp.time.minute)
                    || ((a_comp.time.minute == b_comp.time.minute) && ((a_comp.time.second > b_comp.time.second)
                    || ((a_comp.time.second == b_comp.time.second) && ((a_comp.millisecond > b_comp.millisecond)
                    || ((a_comp.millisecond == b_comp.millisecond) && ((a_comp.microsecond > b_comp.microsecond)
                    || ((a_comp.microsecond == b_comp.microsecond) && (a_comp.nanosecond > b_comp.nanosecond)))))))))))
                    res += adjust_value;
            }
            else if constexpr (std::is_same_v<TransformX, TransformDateTime64<ToRelativeDayNumImpl<ResultPrecision::Extended>>>)
            {
                if ((a_comp.time.hour > b_comp.time.hour)
                    || ((a_comp.time.hour == b_comp.time.hour) && ((a_comp.time.minute > b_comp.time.minute)
                    || ((a_comp.time.minute == b_comp.time.minute) && ((a_comp.time.second > b_comp.time.second)
                    || ((a_comp.time.second == b_comp.time.second) && ((a_comp.millisecond > b_comp.millisecond)
                    || ((a_comp.millisecond == b_comp.millisecond) && ((a_comp.microsecond > b_comp.microsecond)
                    || ((a_comp.microsecond == b_comp.microsecond) && (a_comp.nanosecond > b_comp.nanosecond)))))))))))
                    res += adjust_value;
            }
            else if constexpr (std::is_same_v<TransformX, TransformDateTime64<ToRelativeHourNumImpl<ResultPrecision::Extended>>>)
            {
                if ((a_comp.time.minute > b_comp.time.minute)
                    || ((a_comp.time.minute == b_comp.time.minute) && ((a_comp.time.second > b_comp.time.second)
                    || ((a_comp.time.second == b_comp.time.second) && ((a_comp.millisecond > b_comp.millisecond)
                    || ((a_comp.millisecond == b_comp.millisecond) && ((a_comp.microsecond > b_comp.microsecond)
                    || ((a_comp.microsecond == b_comp.microsecond) && (a_comp.nanosecond > b_comp.nanosecond)))))))))
                    res += adjust_value;
            }
            else if constexpr (std::is_same_v<TransformX, TransformDateTime64<ToRelativeMinuteNumImpl<ResultPrecision::Extended>>>)
            {
                if ((a_comp.time.second > b_comp.time.second)
                    || ((a_comp.time.second == b_comp.time.second) && ((a_comp.millisecond > b_comp.millisecond)
                    || ((a_comp.millisecond == b_comp.millisecond) && ((a_comp.microsecond > b_comp.microsecond)
                    || ((a_comp.microsecond == b_comp.microsecond) && (a_comp.nanosecond > b_comp.nanosecond)))))))
                    res += adjust_value;
            }
            else if constexpr (std::is_same_v<TransformX, TransformDateTime64<ToRelativeSecondNumImpl<ResultPrecision::Extended>>>)
            {
                if ((a_comp.millisecond > b_comp.millisecond)
                    || ((a_comp.millisecond == b_comp.millisecond) && ((a_comp.microsecond > b_comp.microsecond)
                    || ((a_comp.microsecond == b_comp.microsecond) && (a_comp.nanosecond > b_comp.nanosecond)))))
                    res += adjust_value;
            }
            else if constexpr (std::is_same_v<TransformX, TransformDateTime64<ToRelativeSubsecondNumImpl<millisecond_multiplier>>>)
            {
                if ((a_comp.microsecond > b_comp.microsecond)
                    || ((a_comp.microsecond == b_comp.microsecond) && (a_comp.nanosecond > b_comp.nanosecond)))
                    res += adjust_value;
            }
            else if constexpr (std::is_same_v<TransformX, TransformDateTime64<ToRelativeSubsecondNumImpl<microsecond_multiplier>>>)
            {
                if (a_comp.nanosecond > b_comp.nanosecond)
                    res += adjust_value;
            }
            return res;
        }
    }

    template <typename T>
    static UInt32 getScale(const T & v)
    {
        if constexpr (std::is_same_v<T, ColumnDateTime64>)
            return v.getScale();
        else if constexpr (std::is_same_v<T, DecimalField<DateTime64>>)
            return v.getScale();

        return 0;
    }
    template <typename T>
    static auto stripDecimalFieldValue(T && v)
    {
        if constexpr (std::is_same_v<std::decay_t<T>, DecimalField<DateTime64>>)
            return v.getValue();
        else
            return v;
    }
private:
    String name;
};


/** dateDiff('unit', t1, t2, [timezone])
  * age('unit', t1, t2, [timezone])
  * t1 and t2 can be Date, Date32, DateTime or DateTime64
  *
  * If timezone is specified, it applied to both arguments.
  * If not, timezones from datatypes t1 and t2 are used.
  * If that timezones are not the same, the result is unspecified.
  *
  * Timezone matters because days can have different length.
  */
template <bool is_relative>
class FunctionDateDiff : public IFunction
{
public:
    static constexpr auto name = is_relative ? "dateDiff" : "age";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDateDiff>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 3}; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors mandatory_args{
            {"unit", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
            {"startdate", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isDateOrDate32OrDateTimeOrDateTime64), nullptr, "Date[32] or DateTime[64]"},
            {"enddate", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isDateOrDate32OrDateTimeOrDateTime64), nullptr, "Date[32] or DateTime[64]"},
        };

        FunctionArgumentDescriptors optional_args{
            {"timezone", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isString), nullptr, "String"},
        };

        validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

        return std::make_shared<DataTypeInt64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_unit = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        if (!col_unit)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument for function {} must be constant String", getName());

        String unit = Poco::toLower(col_unit->getValue<String>());

        const IColumn & col_x = *arguments[1].column;
        const IColumn & col_y = *arguments[2].column;

        auto col_res = ColumnInt64::create(input_rows_count);

        const auto & timezone_x = extractTimeZoneFromFunctionArguments(arguments, 3, 1);
        const auto & timezone_y = extractTimeZoneFromFunctionArguments(arguments, 3, 2);

        if (unit == "year" || unit == "years" || unit == "yy" || unit == "yyyy")
            impl.template dispatchForColumns<ToRelativeYearNumImpl<ResultPrecision::Extended>>(col_x, col_y, timezone_x, timezone_y, input_rows_count, col_res->getData());
        else if (unit == "quarter" || unit == "quarters" || unit == "qq" || unit == "q")
            impl.template dispatchForColumns<ToRelativeQuarterNumImpl<ResultPrecision::Extended>>(col_x, col_y, timezone_x, timezone_y, input_rows_count, col_res->getData());
        else if (unit == "month" || unit == "months" || unit == "mm" || unit == "m")
            impl.template dispatchForColumns<ToRelativeMonthNumImpl<ResultPrecision::Extended>>(col_x, col_y, timezone_x, timezone_y, input_rows_count, col_res->getData());
        else if (unit == "week" || unit == "weeks" || unit == "wk" || unit == "ww")
            impl.template dispatchForColumns<ToRelativeWeekNumImpl<ResultPrecision::Extended>>(col_x, col_y, timezone_x, timezone_y, input_rows_count, col_res->getData());
        else if (unit == "day" || unit == "days" || unit == "dd" || unit == "d")
            impl.template dispatchForColumns<ToRelativeDayNumImpl<ResultPrecision::Extended>>(col_x, col_y, timezone_x, timezone_y, input_rows_count, col_res->getData());
        else if (unit == "hour" || unit == "hours" || unit == "hh" || unit == "h")
            impl.template dispatchForColumns<ToRelativeHourNumImpl<ResultPrecision::Extended>>(col_x, col_y, timezone_x, timezone_y, input_rows_count, col_res->getData());
        else if (unit == "minute" || unit == "minutes" || unit == "mi" || unit == "n")
            impl.template dispatchForColumns<ToRelativeMinuteNumImpl<ResultPrecision::Extended>>(col_x, col_y, timezone_x, timezone_y, input_rows_count, col_res->getData());
        else if (unit == "second" || unit == "seconds" || unit == "ss" || unit == "s")
            impl.template dispatchForColumns<ToRelativeSecondNumImpl<ResultPrecision::Extended>>(col_x, col_y, timezone_x, timezone_y, input_rows_count, col_res->getData());
        else if (unit == "millisecond" || unit == "milliseconds" || unit == "ms")
            impl.template dispatchForColumns<ToRelativeSubsecondNumImpl<millisecond_multiplier>>(col_x, col_y, timezone_x, timezone_y, input_rows_count, col_res->getData());
        else if (unit == "microsecond" || unit == "microseconds" || unit == "us" || unit == "u")
            impl.template dispatchForColumns<ToRelativeSubsecondNumImpl<microsecond_multiplier>>(col_x, col_y, timezone_x, timezone_y, input_rows_count, col_res->getData());
        else if (unit == "nanosecond" || unit == "nanoseconds" || unit == "ns")
            impl.template dispatchForColumns<ToRelativeSubsecondNumImpl<nanosecond_multiplier>>(col_x, col_y, timezone_x, timezone_y, input_rows_count, col_res->getData());
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function {} does not support '{}' unit", getName(), unit);

        return col_res;
    }
private:
    DateDiffImpl<is_relative> impl{name};
};


/** timeDiff(t1, t2)
  * t1 and t2 can be Date or DateTime
  */
class FunctionTimeDiff : public IFunction
{
    using ColumnDateTime64 = ColumnDecimal<DateTime64>;
public:
    static constexpr auto name = "timeDiff";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeDiff>(); }

    String getName() const override { return name; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }
    bool isVariadic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"first_datetime", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isDateOrDate32OrDateTimeOrDateTime64), nullptr, "Date[32] or DateTime[64]"},
            {"second_datetime", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isDateOrDate32OrDateTimeOrDateTime64), nullptr, "Date[32] or DateTime[64]"},
        };

        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeInt64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeInt64>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn & col_x = *arguments[0].column;
        const IColumn & col_y = *arguments[1].column;

        auto col_res = ColumnInt64::create(input_rows_count);

        impl.dispatchForColumns<ToRelativeSecondNumImpl<ResultPrecision::Extended>>(col_x, col_y, DateLUT::instance(), DateLUT::instance(), input_rows_count, col_res->getData());

        return col_res;
    }
private:
    DateDiffImpl<true> impl{name};
};

}

REGISTER_FUNCTION(DateDiff)
{
    factory.registerFunction<FunctionDateDiff<true>>({}, FunctionFactory::Case::Insensitive);
    factory.registerAlias("date_diff", FunctionDateDiff<true>::name);
    factory.registerAlias("DATE_DIFF", FunctionDateDiff<true>::name);
    factory.registerAlias("timestampDiff", FunctionDateDiff<true>::name);
    factory.registerAlias("timestamp_diff", FunctionDateDiff<true>::name);
    factory.registerAlias("TIMESTAMP_DIFF", FunctionDateDiff<true>::name);
}

REGISTER_FUNCTION(TimeDiff)
{
    factory.registerFunction<FunctionTimeDiff>(FunctionDocumentation{.description=R"(
Returns the difference between two dates or dates with time values. The difference is calculated in seconds units (see toRelativeSecondNum).
It is same as `dateDiff` and was added only for MySQL support. `dateDiff` is preferred.

Example:
[example:typical]
)",
    .examples{
        {"typical", "SELECT timeDiff(UTCTimestamp(), now());", ""}},
    .categories{"Dates and Times"}}, FunctionFactory::Case::Insensitive);
}

REGISTER_FUNCTION(Age)
{
    factory.registerFunction<FunctionDateDiff<false>>({}, FunctionFactory::Case::Insensitive);
}

}
