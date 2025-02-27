#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Common/NaNUtils.h>
#include <DataTypes/DataTypeString.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>
#include <IO/DoubleConverter.h>


namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int BAD_ARGUMENTS;
}

namespace
{

/** Prints amount of seconds in form of:
  * "1 year, 2 months, 12 days, 3 hours, 1 minute and 33 seconds".
  * Maximum unit can be specified as a second argument: for example, you can specify "days",
  * and it will avoid using years and months.
  *
  * The length of years and months (and even days in presence of time adjustments) are rough:
  * year is just 365 days, month is 30.5 days, day is 86400 seconds.
  *
  * You may think that the choice of constants and the whole purpose of this function is very ignorant...
  * And you're right. But actually it's made similar to a random Python library from the internet:
  * https://github.com/jmoiron/humanize/blob/b37dc30ba61c2446eecb1a9d3e9ac8c9adf00f03/src/humanize/time.py#L462
  */
class FunctionFormatReadableTimeDelta : public IFunction
{
public:
    static constexpr auto name = "formatReadableTimeDelta";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFormatReadableTimeDelta>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Number of arguments for function {} doesn't match: passed {}, should be at least 1.",
                            getName(), arguments.size());

        if (arguments.size() > 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Number of arguments for function {} doesn't match: passed {}, should be 1, 2 or 3.",
                            getName(), arguments.size());

        const IDataType & type = *arguments[0];

        if (!isNativeNumber(type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Cannot format {} as time delta", type.getName());

        if (arguments.size() >= 2)
        {
            const auto * maximum_unit_arg = arguments[1].get();
            if (!isStringOrFixedString(maximum_unit_arg))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument maximum_unit of function {}",
                                maximum_unit_arg->getName(), getName());

            if (arguments.size() == 3)
            {
                const auto * minimum_unit_arg = arguments[2].get();
                if (!isStringOrFixedString(minimum_unit_arg))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument minimum_unit of function {}",
                                    minimum_unit_arg->getName(), getName());
            }
        }

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

    bool useDefaultImplementationForConstants() const override { return true; }

    enum Unit
    {
        Nanoseconds = 1,
        Microseconds = 2,
        Milliseconds = 3,
        Seconds = 4,
        Minutes = 5,
        Hours = 6,
        Days = 7,
        Months = 8,
        Years = 9
    };

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        std::string_view maximum_unit_str, minimum_unit_str;
        if (arguments.size() >= 2)
        {
            const ColumnPtr & maximum_unit_column = arguments[1].column;
            const ColumnConst * maximum_unit_const_col = checkAndGetColumnConstStringOrFixedString(maximum_unit_column.get());
            if (maximum_unit_const_col)
                maximum_unit_str = maximum_unit_const_col->getDataColumn().getDataAt(0).toView();

            if (arguments.size() == 3)
            {
                const ColumnPtr & minimum_unit_column = arguments[2].column;
                const ColumnConst * minimum_unit_const_col = checkAndGetColumnConstStringOrFixedString(minimum_unit_column.get());
                if (minimum_unit_const_col)
                    minimum_unit_str = minimum_unit_const_col->getDataColumn().getDataAt(0).toView();
            }
        }
        /// Default means "use all available whole units".
        Unit max_unit = dispatchUnit(maximum_unit_str, Years, "maximum");
        /// Set seconds as min_unit by default not to ruin old use cases
        Unit min_unit = dispatchUnit(minimum_unit_str, Seconds, "minimum");

        if (min_unit > max_unit)
        {
            if (minimum_unit_str.empty())
                min_unit = Nanoseconds;   /// User wants sub-second max_unit. Show him all sub-second units unless other specified.
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                                "Explicitly specified value of minimum unit argument ({}) for function {} "
                                "must not be greater than maximum unit value ({}).",
                                minimum_unit_str, getName(), maximum_unit_str);
        }

        auto col_to = ColumnString::create();

        ColumnString::Chars & data_to = col_to->getChars();
        ColumnString::Offsets & offsets_to = col_to->getOffsets();
        offsets_to.resize(input_rows_count);

        WriteBufferFromVector<ColumnString::Chars> buf_to(data_to);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            /// Virtual call is Ok (negligible comparing to the rest of calculations).
            Float64 value = arguments[0].column->getFloat64(i);

            if (!isFinite(value))
            {
                /// Cannot decide what unit it is (years, month), just simply write inf or nan.
                writeFloatText(value, buf_to);
            }
            else
            {
                bool is_negative = value < 0;
                if (is_negative)
                {
                    writeChar('-', buf_to);
                    value = -value;
                }

                /// To output separators between parts: ", " and " and ".
                bool has_output = false;

                Float64 whole_part;
                std::string fractional_str = getFractionalString(std::modf(value, &whole_part));

                switch (max_unit) /// A kind of Duff Device.
                {
                    case Years:
                        processUnit(365 * 24 * 3600, 0, " year", 5, whole_part, fractional_str, buf_to, has_output, min_unit, min_unit == Years);
                        if (min_unit == Years)
                            break;
                        [[fallthrough]];

                    case Months:
                        processUnit(static_cast<UInt64>(30.5 * 24 * 3600), 0, " month", 6, whole_part, fractional_str, buf_to, has_output, min_unit, min_unit == Months);
                        if (min_unit == Months)
                            break;
                        [[fallthrough]];

                    case Days:
                        processUnit(24 * 3600, 0, " day", 4, whole_part, fractional_str, buf_to, has_output, min_unit, min_unit == Days);
                        if (min_unit == Days)
                            break;
                        [[fallthrough]];

                    case Hours:
                        processUnit(3600, 0, " hour", 5, whole_part, fractional_str, buf_to, has_output, min_unit, min_unit == Hours);
                        if (min_unit == Hours)
                            break;
                        [[fallthrough]];

                    case Minutes:
                        processUnit(60, 0, " minute", 7, whole_part, fractional_str, buf_to, has_output, min_unit, min_unit == Minutes);
                        if (min_unit == Minutes)
                            break;
                        [[fallthrough]];

                    case Seconds:
                        processUnit(1, 0, " second", 7, whole_part, fractional_str, buf_to, has_output, min_unit, min_unit == Seconds);
                        if (min_unit == Seconds)
                            break;
                        [[fallthrough]];

                    case Milliseconds:
                        processUnit(1, 3, " millisecond", 12, whole_part, fractional_str, buf_to, has_output, min_unit, min_unit == Milliseconds);
                        if (min_unit == Milliseconds)
                            break;
                        [[fallthrough]];

                    case Microseconds:
                        processUnit(1, 6, " microsecond", 12, whole_part, fractional_str, buf_to, has_output, min_unit, min_unit == Microseconds);
                        if (min_unit == Microseconds)
                            break;
                        [[fallthrough]];

                    case Nanoseconds:
                        processUnit(1, 9, " nanosecond", 11, whole_part, fractional_str, buf_to, has_output, min_unit, true);
                }
            }

            writeChar(0, buf_to);
            offsets_to[i] = buf_to.count();
        }

        buf_to.finalize();
        return col_to;
    }

    static void processUnit(
        UInt64 unit_multiplier, UInt32 unit_scale, const char * unit_name, size_t unit_name_size, Float64 & whole_part,
        String & fractional_str, WriteBuffer & buf_to, bool & has_output,  Unit min_unit, bool is_minimum_unit)
    {
        if (unlikely(whole_part + 1.0 == whole_part))
        {
            /// The case when value is too large so exact representation for subsequent smaller units is not possible.
            writeText(std::floor(whole_part * DecimalUtils::scaleMultiplier<Int64>(unit_scale) / unit_multiplier), buf_to);
            buf_to.write(unit_name, unit_name_size);
            writeChar('s', buf_to);
            has_output = true;
            whole_part = 0;
            return;
        }
        UInt64 num_units = 0;
        if (unit_scale == 0)  /// dealing with whole number of seconds
        {
            num_units = static_cast<UInt64>(std::floor(whole_part / unit_multiplier));

            if (!num_units)
            {
                /// Zero units, no need to print. But if it's the last (seconds) and the only unit, print "0 seconds" nevertheless.
                if (unit_multiplier != 1 || has_output)
                    return;
            }

            /// Remaining value to print on next iteration.
            whole_part -= num_units * unit_multiplier;
        }
        else   /// dealing with sub-seconds, a bit more peculiar to avoid more precision issues
        {
            if (whole_part >= 1)  /// There were no whole units printed
            {
                num_units += static_cast<UInt64>(whole_part) * DecimalUtils::scaleMultiplier<Int64>(unit_scale);
                whole_part = 0;
            }

            for (UInt32 i = 0; i < unit_scale; ++i)
            {
                num_units += (fractional_str[i] - '0') * DecimalUtils::scaleMultiplier<Int64>(unit_scale - i - 1);
                fractional_str[i] = '0';
            }

            if (!num_units)
            {
                /// Zero units, no need to print. But if it's the last (nanoseconds) and the only unit, print "0 nanoseconds" nevertheless.
                if (!is_minimum_unit || has_output)
                    return;
            }
        }

        /// Write number of units
        if (has_output)
        {
            /// Need delimiter between values. The last delimiter is " and ", all previous are comma.
            if (is_minimum_unit || (whole_part < 1 && fractional_str.substr(0, (4 - min_unit) * 3) == std::string((4 - min_unit) * 3, '0')))
                writeCString(" and ", buf_to);
            else
                writeCString(", ", buf_to);
        }

        writeText(num_units, buf_to);

        buf_to.write(unit_name, unit_name_size); /// If we just leave strlen(unit_name) here, clang-11 fails to make it compile-time.

        /// How to pronounce: unit vs. units.
        if (num_units != 1)
            writeChar('s', buf_to);

        has_output = true;
    }

private:
    static std::string getFractionalString(const Float64 & fractional_part)
    {
        DB::DoubleConverter<true>::BufferType buffer;
        double_conversion::StringBuilder builder{buffer, sizeof(buffer)};

        if (!DB::DoubleConverter<false>::instance().ToFixed(fractional_part, 9, &builder))
            throw DB::Exception(DB::ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER, "Cannot print double number: {}", fractional_part);

        return std::string(buffer, builder.position()).substr(2);   /// do not return `0.` -- we don't need it
    }

    Unit dispatchUnit(const std::string_view & unit_str, const Unit default_unit, const std::string & bound_name) const
    {
        if (unit_str.empty())
            return default_unit;
        if (unit_str == "years")
            return Years;
        if (unit_str == "months")
            return Months;
        if (unit_str == "days")
            return Days;
        if (unit_str == "hours")
            return Hours;
        if (unit_str == "minutes")
            return Minutes;
        if (unit_str == "seconds")
            return Seconds;
        if (unit_str == "milliseconds")
            return Milliseconds;
        if (unit_str == "microseconds")
            return Microseconds;
        if (unit_str == "nanoseconds")
            return Nanoseconds;
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unexpected value of {} unit argument ({}) for function {}, the only allowed values are:"
            " 'nanoseconds', 'microseconds', 'nanoseconds', 'seconds', 'minutes', 'hours', 'days', 'months', 'years'.",
            bound_name,
            unit_str,
            getName());
    }
};

}

REGISTER_FUNCTION(FormatReadableTimeDelta)
{
    factory.registerFunction<FunctionFormatReadableTimeDelta>();
}

}
