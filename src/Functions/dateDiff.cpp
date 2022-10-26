#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypesNumber.h>
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

/** dateDiff('unit', t1, t2, [timezone])
  * t1 and t2 can be Date or DateTime
  *
  * If timezone is specified, it applied to both arguments.
  * If not, timezones from datatypes t1 and t2 are used.
  * If that timezones are not the same, the result is unspecified.
  *
  * Timezone matters because days can have different length.
  */
class FunctionDateDiff : public IFunction
{
public:
    static constexpr auto name = "dateDiff";
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
            dispatchForColumns<ToRelativeYearNumImpl<ResultPrecision::Extended>>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "quarter" || unit == "qq" || unit == "q")
            dispatchForColumns<ToRelativeQuarterNumImpl<ResultPrecision::Extended>>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "month" || unit == "mm" || unit == "m")
            dispatchForColumns<ToRelativeMonthNumImpl<ResultPrecision::Extended>>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "week" || unit == "wk" || unit == "ww")
            dispatchForColumns<ToRelativeWeekNumImpl<ResultPrecision::Extended>>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "day" || unit == "dd" || unit == "d")
            dispatchForColumns<ToRelativeDayNumImpl<ResultPrecision::Extended>>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "hour" || unit == "hh" || unit == "h")
            dispatchForColumns<ToRelativeHourNumImpl<ResultPrecision::Extended>>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "minute" || unit == "mi" || unit == "n")
            dispatchForColumns<ToRelativeMinuteNumImpl<ResultPrecision::Extended>>(x, y, timezone_x, timezone_y, res->getData());
        else if (unit == "second" || unit == "ss" || unit == "s")
            dispatchForColumns<ToRelativeSecondNumImpl<ResultPrecision::Extended>>(x, y, timezone_x, timezone_y, res->getData());
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Function {} does not support '{}' unit", getName(), unit);

        return res;
    }

private:
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
                getName());
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
                getName());
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
                getName());
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
        return static_cast<Int64>(transform_y.execute(y, timezone_y))
             - static_cast<Int64>(transform_x.execute(x, timezone_x));
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
};

}

REGISTER_FUNCTION(DateDiff)
{
    factory.registerFunction<FunctionDateDiff>({}, FunctionFactory::CaseInsensitive);
}

}
