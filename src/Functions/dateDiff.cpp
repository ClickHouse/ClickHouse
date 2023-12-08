#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/IntervalKind.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnDecimal.h>

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
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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
        const IColumn & x, const IColumn & y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result) const
    {
        if (const auto * x_vec_16 = checkAndGetColumn<ColumnDate>(&x))
            dispatchForSecondColumn<Transform>(*x_vec_16, y, timezone_x, timezone_y, result);
        else if (const auto * x_vec_32 = checkAndGetColumn<ColumnDateTime>(&x))
            dispatchForSecondColumn<Transform>(*x_vec_32, y, timezone_x, timezone_y, result);
        else if (const auto * x_vec_32_s = checkAndGetColumn<ColumnDate32>(&x))
            dispatchForSecondColumn<Transform>(*x_vec_32_s, y, timezone_x, timezone_y, result);
        else if (const auto * x_vec_64 = checkAndGetColumn<ColumnDateTime64>(&x))
            dispatchForSecondColumn<Transform>(*x_vec_64, y, timezone_x, timezone_y, result);
        else if (const auto * x_const_16 = checkAndGetColumnConst<ColumnDate>(&x))
            dispatchConstForSecondColumn<Transform>(x_const_16->getValue<UInt16>(), y, timezone_x, timezone_y, result);
        else if (const auto * x_const_32 = checkAndGetColumnConst<ColumnDateTime>(&x))
            dispatchConstForSecondColumn<Transform>(x_const_32->getValue<UInt32>(), y, timezone_x, timezone_y, result);
        else if (const auto * x_const_32_s = checkAndGetColumnConst<ColumnDate32>(&x))
            dispatchConstForSecondColumn<Transform>(x_const_32_s->getValue<Int32>(), y, timezone_x, timezone_y, result);
        else if (const auto * x_const_64 = checkAndGetColumnConst<ColumnDateTime64>(&x))
            dispatchConstForSecondColumn<Transform>(x_const_64->getValue<DecimalField<DateTime64>>(), y, timezone_x, timezone_y, result);
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column for first argument of function {}, must be Date, Date32, DateTime or DateTime64",
                name);
    }

    template <typename Transform, typename LeftColumnType>
    void dispatchForSecondColumn(
        const LeftColumnType & x, const IColumn & y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result) const
    {
        if (const auto * y_vec_16 = checkAndGetColumn<ColumnDate>(&y))
            vectorVector<Transform>(x, *y_vec_16, timezone_x, timezone_y, result);
        else if (const auto * y_vec_32 = checkAndGetColumn<ColumnDateTime>(&y))
            vectorVector<Transform>(x, *y_vec_32, timezone_x, timezone_y, result);
        else if (const auto * y_vec_32_s = checkAndGetColumn<ColumnDate32>(&y))
            vectorVector<Transform>(x, *y_vec_32_s, timezone_x, timezone_y, result);
        else if (const auto * y_vec_64 = checkAndGetColumn<ColumnDateTime64>(&y))
            vectorVector<Transform>(x, *y_vec_64, timezone_x, timezone_y, result);
        else if (const auto * y_const_16 = checkAndGetColumnConst<ColumnDate>(&y))
            vectorConstant<Transform>(x, y_const_16->getValue<UInt16>(), timezone_x, timezone_y, result);
        else if (const auto * y_const_32 = checkAndGetColumnConst<ColumnDateTime>(&y))
            vectorConstant<Transform>(x, y_const_32->getValue<UInt32>(), timezone_x, timezone_y, result);
        else if (const auto * y_const_32_s = checkAndGetColumnConst<ColumnDate32>(&y))
            vectorConstant<Transform>(x, y_const_32_s->getValue<Int32>(), timezone_x, timezone_y, result);
        else if (const auto * y_const_64 = checkAndGetColumnConst<ColumnDateTime64>(&y))
            vectorConstant<Transform>(x, y_const_64->getValue<DecimalField<DateTime64>>(), timezone_x, timezone_y, result);
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column for second argument of function {}, must be Date, Date32, DateTime or DateTime64",
                name);
    }

    template <typename Transform, typename T1>
    void dispatchConstForSecondColumn(
        T1 x, const IColumn & y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result) const
    {
        if (const auto * y_vec_16 = checkAndGetColumn<ColumnDate>(&y))
            constantVector<Transform>(x, *y_vec_16, timezone_x, timezone_y, result);
        else if (const auto * y_vec_32 = checkAndGetColumn<ColumnDateTime>(&y))
            constantVector<Transform>(x, *y_vec_32, timezone_x, timezone_y, result);
        else if (const auto * y_vec_32_s = checkAndGetColumn<ColumnDate32>(&y))
            constantVector<Transform>(x, *y_vec_32_s, timezone_x, timezone_y, result);
        else if (const auto * y_vec_64 = checkAndGetColumn<ColumnDateTime64>(&y))
            constantVector<Transform>(x, *y_vec_64, timezone_x, timezone_y, result);
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column for second argument of function {}, must be Date, Date32, DateTime or DateTime64",
                name);
    }

    template <typename Transform, typename LeftColumnType, typename RightColumnType>
    void vectorVector(
        const LeftColumnType & x, const RightColumnType & y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result) const
    {
        const auto & x_data = x.getData();
        const auto & y_data = y.getData();

        const auto transform_x = TransformDateTime64<Transform>(getScale(x));
        const auto transform_y = TransformDateTime64<Transform>(getScale(y));
        for (size_t i = 0, size = x.size(); i < size; ++i)
                result[i] = calculate(transform_x, transform_y, x_data[i], y_data[i], timezone_x, timezone_y);
    }

    template <typename Transform, typename LeftColumnType, typename T2>
    void vectorConstant(
        const LeftColumnType & x, T2 y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result) const
    {
        const auto & x_data = x.getData();
        const auto transform_x = TransformDateTime64<Transform>(getScale(x));
        const auto transform_y = TransformDateTime64<Transform>(getScale(y));
        const auto y_value = stripDecimalFieldValue(y);

        for (size_t i = 0, size = x.size(); i < size; ++i)
            result[i] = calculate(transform_x, transform_y, x_data[i], y_value, timezone_x, timezone_y);
    }

    template <typename Transform, typename T1, typename RightColumnType>
    void constantVector(
        T1 x, const RightColumnType & y,
        const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y,
        ColumnInt64::Container & result) const
    {
        const auto & y_data = y.getData();
        const auto transform_x = TransformDateTime64<Transform>(getScale(x));
        const auto transform_y = TransformDateTime64<Transform>(getScale(y));
        const auto x_value = stripDecimalFieldValue(x);

        for (size_t i = 0, size = y.size(); i < size; ++i)
            result[i] = calculate(transform_x, transform_y, x_value, y_data[i], timezone_x, timezone_y);
    }

    template <typename TransformX, typename TransformY, typename T1, typename T2>
    Int64 calculate(const TransformX & transform_x, const TransformY & transform_y, T1 x, T2 y, const DateLUTImpl & timezone_x, const DateLUTImpl & timezone_y) const
    {
        if constexpr (is_diff)
            return static_cast<Int64>(transform_y.execute(y, timezone_y))
                - static_cast<Int64>(transform_x.execute(x, timezone_x));
        else
        {
            auto res = static_cast<Int64>(transform_y.execute(y, timezone_y))
                - static_cast<Int64>(transform_x.execute(x, timezone_x));
            DateTimeComponentsWithFractionalPart a_comp;
            DateTimeComponentsWithFractionalPart b_comp;
            Int64 adjust_value;
            auto x_microseconds = TransformDateTime64<ToRelativeSubsecondNumImpl<microsecond_multiplier>>(transform_x.getScaleMultiplier()).execute(x, timezone_x);
            auto y_microseconds = TransformDateTime64<ToRelativeSubsecondNumImpl<microsecond_multiplier>>(transform_y.getScaleMultiplier()).execute(y, timezone_y);

            if (x_microseconds <= y_microseconds)
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
                    || ((a_comp.millisecond == b_comp.millisecond) && (a_comp.microsecond > b_comp.microsecond)))))))))))))
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
                    || ((a_comp.millisecond == b_comp.millisecond) && (a_comp.microsecond > b_comp.microsecond)))))))))))))
                    res += adjust_value;
            }
            else if constexpr (std::is_same_v<TransformX, TransformDateTime64<ToRelativeMonthNumImpl<ResultPrecision::Extended>>>)
            {
                if ((a_comp.date.day > b_comp.date.day)
                    || ((a_comp.date.day == b_comp.date.day) && ((a_comp.time.hour > b_comp.time.hour)
                    || ((a_comp.time.hour == b_comp.time.hour) && ((a_comp.time.minute > b_comp.time.minute)
                    || ((a_comp.time.minute == b_comp.time.minute) && ((a_comp.time.second > b_comp.time.second)
                    || ((a_comp.time.second == b_comp.time.second) && ((a_comp.millisecond > b_comp.millisecond)
                    || ((a_comp.millisecond == b_comp.millisecond) && (a_comp.microsecond > b_comp.microsecond)))))))))))
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
                    || ((a_comp.millisecond == b_comp.millisecond) && (a_comp.microsecond > b_comp.microsecond)))))))))
                    res += adjust_value;
            }
            else if constexpr (std::is_same_v<TransformX, TransformDateTime64<ToRelativeDayNumImpl<ResultPrecision::Extended>>>)
            {
                if ((a_comp.time.hour > b_comp.time.hour)
                    || ((a_comp.time.hour == b_comp.time.hour) && ((a_comp.time.minute > b_comp.time.minute)
                    || ((a_comp.time.minute == b_comp.time.minute) && ((a_comp.time.second > b_comp.time.second)
                    || ((a_comp.time.second == b_comp.time.second) && ((a_comp.millisecond > b_comp.millisecond)
                    || ((a_comp.millisecond == b_comp.millisecond) && (a_comp.microsecond > b_comp.microsecond)))))))))
                    res += adjust_value;
            }
            else if constexpr (std::is_same_v<TransformX, TransformDateTime64<ToRelativeHourNumImpl<ResultPrecision::Extended>>>)
            {
                if ((a_comp.time.minute > b_comp.time.minute)
                    || ((a_comp.time.minute == b_comp.time.minute) && ((a_comp.time.second > b_comp.time.second)
                    || ((a_comp.time.second == b_comp.time.second) && ((a_comp.millisecond > b_comp.millisecond)
                    || ((a_comp.millisecond == b_comp.millisecond) && (a_comp.microsecond > b_comp.microsecond)))))))
                    res += adjust_value;
            }
            else if constexpr (std::is_same_v<TransformX, TransformDateTime64<ToRelativeMinuteNumImpl<ResultPrecision::Extended>>>)
            {
                if ((a_comp.time.second > b_comp.time.second)
                    || ((a_comp.time.second == b_comp.time.second) && ((a_comp.millisecond > b_comp.millisecond)
                    || ((a_comp.millisecond == b_comp.millisecond) && (a_comp.microsecond > b_comp.microsecond)))))
                    res += adjust_value;
            }
            else if constexpr (std::is_same_v<TransformX, TransformDateTime64<ToRelativeSecondNumImpl<ResultPrecision::Extended>>>)
            {
                if ((a_comp.millisecond > b_comp.millisecond)
                    || ((a_comp.millisecond == b_comp.millisecond) && (a_comp.microsecond > b_comp.microsecond)))
                    res += adjust_value;
            }
            else if constexpr (std::is_same_v<TransformX, TransformDateTime64<ToRelativeSubsecondNumImpl<1000>>>)
            {
                if (a_comp.microsecond > b_comp.microsecond)
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

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 3 && arguments.size() != 4)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 3 or 4",
                getName(), arguments.size());

        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} (unit) must be String",
                getName());

        if (!isDate(arguments[1]) && !isDate32(arguments[1]) && !isDateTime(arguments[1]) && !isDateTime64(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument for function {} must be Date, Date32, DateTime or DateTime64",
                getName());

        if (!isDate(arguments[2]) && !isDate32(arguments[2]) && !isDateTime(arguments[2]) && !isDateTime64(arguments[2]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Third argument for function {} must be Date, Date32, DateTime or DateTime64",
                getName()
                );

        if (arguments.size() == 4 && !isString(arguments[3]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Fourth argument for function {} (timezone) must be String",
                getName());

        return std::make_shared<DataTypeInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 3}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * unit_column = checkAndGetColumnConst<ColumnString>(arguments[0].column.get());
        if (!unit_column)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "First argument for function {} must be constant String",
                getName());

        String unit = Poco::toLower(unit_column->getValue<String>());

        const IColumn & x = *arguments[1].column;
        const IColumn & y = *arguments[2].column;

        size_t rows = input_rows_count;
        auto res = ColumnInt64::create(rows);

        const auto & timezone_x = extractTimeZoneFromFunctionArguments(arguments, 3, 1);
        const auto & timezone_y = extractTimeZoneFromFunctionArguments(arguments, 3, 2);

        if (unit == "year" || unit == "yy" || unit == "yyyy")
            impl.template dispatchForColumns<ToRelativeYearNumImpl<ResultPrecision::Extended>>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "quarter" || unit == "qq" || unit == "q")
            impl.template dispatchForColumns<ToRelativeQuarterNumImpl<ResultPrecision::Extended>>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "month" || unit == "mm" || unit == "m")
            impl.template dispatchForColumns<ToRelativeMonthNumImpl<ResultPrecision::Extended>>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "week" || unit == "wk" || unit == "ww")
            impl.template dispatchForColumns<ToRelativeWeekNumImpl<ResultPrecision::Extended>>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "day" || unit == "dd" || unit == "d")
            impl.template dispatchForColumns<ToRelativeDayNumImpl<ResultPrecision::Extended>>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "hour" || unit == "hh" || unit == "h")
            impl.template dispatchForColumns<ToRelativeHourNumImpl<ResultPrecision::Extended>>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "minute" || unit == "mi" || unit == "n")
            impl.template dispatchForColumns<ToRelativeMinuteNumImpl<ResultPrecision::Extended>>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "second" || unit == "ss" || unit == "s")
            impl.template dispatchForColumns<ToRelativeSecondNumImpl<ResultPrecision::Extended>>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "millisecond" || unit == "ms")
            impl.template dispatchForColumns<ToRelativeSubsecondNumImpl<millisecond_multiplier>>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "microsecond" || unit == "us" || unit == "u")
            impl.template dispatchForColumns<ToRelativeSubsecondNumImpl<microsecond_multiplier>>(x, y, timezone_x, timezone_y, res->getData());
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Function {} does not support '{}' unit", getName(), unit);

        return res;
    }
private:
    DateDiffImpl<is_relative> impl{name};
};


/** TimeDiff(t1, t2)
  * t1 and t2 can be Date or DateTime
  */
class FunctionTimeDiff : public IFunction
{
    using ColumnDateTime64 = ColumnDecimal<DateTime64>;
public:
    static constexpr auto name = "TimeDiff";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTimeDiff>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments for function {} doesn't match: passed {}, should be 2",
                getName(), arguments.size());

        if (!isDate(arguments[0]) && !isDate32(arguments[0]) && !isDateTime(arguments[0]) && !isDateTime64(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be Date, Date32, DateTime or DateTime64",
                getName());

        if (!isDate(arguments[1]) && !isDate32(arguments[1]) && !isDateTime(arguments[1]) && !isDateTime64(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument for function {} must be Date, Date32, DateTime or DateTime64",
                getName()
                );

        return std::make_shared<DataTypeInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const IColumn & x = *arguments[0].column;
        const IColumn & y = *arguments[1].column;

        size_t rows = input_rows_count;
        auto res = ColumnInt64::create(rows);

        impl.dispatchForColumns<ToRelativeSecondNumImpl<ResultPrecision::Extended>>(x, y, DateLUT::instance(), DateLUT::instance(), res->getData());

        return res;
    }
private:
    DateDiffImpl<true> impl{name};
};

}

REGISTER_FUNCTION(DateDiff)
{
    factory.registerFunction<FunctionDateDiff<true>>({}, FunctionFactory::CaseInsensitive);
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
    .categories{"Dates and Times"}}, FunctionFactory::CaseInsensitive);
}

REGISTER_FUNCTION(Age)
{
    factory.registerFunction<FunctionDateDiff<false>>({}, FunctionFactory::CaseInsensitive);
}

}
