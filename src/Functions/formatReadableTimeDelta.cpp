#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Common/NaNUtils.h>
#include <DataTypes/DataTypeString.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
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
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be at least 1.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments.size() > 2)
            throw Exception(
                "Number of arguments for function " + getName() + " doesn't match: passed " + toString(arguments.size())
                    + ", should be at most 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const IDataType & type = *arguments[0];

        if (!isNativeNumber(type))
            throw Exception("Cannot format " + type.getName() + " as time delta", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 2)
        {
            const auto * maximum_unit_arg = arguments[1].get();
            if (!isStringOrFixedString(maximum_unit_arg))
                throw Exception("Illegal type " + maximum_unit_arg->getName() + " of argument maximum_unit of function "
                                + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return std::make_shared<DataTypeString>();
    }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool useDefaultImplementationForConstants() const override { return true; }

    enum Unit
    {
        Seconds,
        Minutes,
        Hours,
        Days,
        Months,
        Years
    };

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        std::string_view maximum_unit_str;
        if (arguments.size() == 2)
        {
            const ColumnPtr & maximum_unit_column = arguments[1].column;
            const ColumnConst * maximum_unit_const_col = checkAndGetColumnConstStringOrFixedString(maximum_unit_column.get());
            if (maximum_unit_const_col)
                maximum_unit_str = maximum_unit_const_col->getDataColumn().getDataAt(0).toView();
        }

        Unit max_unit;

        /// Default means "use all available units".
        if (maximum_unit_str.empty() || maximum_unit_str == "years")
            max_unit = Years;
        else if (maximum_unit_str == "months")
            max_unit = Months;
        else if (maximum_unit_str == "days")
            max_unit = Days;
        else if (maximum_unit_str == "hours")
            max_unit = Hours;
        else if (maximum_unit_str == "minutes")
            max_unit = Minutes;
        else if (maximum_unit_str == "seconds")
            max_unit = Seconds;
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Unexpected value of maximum unit argument ({}) for function {}, the only allowed values are:"
                " 'seconds', 'minutes', 'hours', 'days', 'months', 'years'.",
                maximum_unit_str, getName());

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

                switch (max_unit) /// A kind of Duff Device.
                {
                    case Years:     processUnit(365 * 24 * 3600, " year", 5, value, buf_to, has_output); [[fallthrough]];
                    case Months:    processUnit(30.5 * 24 * 3600, " month", 6, value, buf_to, has_output); [[fallthrough]];
                    case Days:      processUnit(24 * 3600, " day", 4, value, buf_to, has_output); [[fallthrough]];
                    case Hours:     processUnit(3600, " hour", 5, value, buf_to, has_output); [[fallthrough]];
                    case Minutes:   processUnit(60, " minute", 7, value, buf_to, has_output); [[fallthrough]];
                    case Seconds:   processUnit(1, " second", 7, value, buf_to, has_output);
                }
            }

            writeChar(0, buf_to);
            offsets_to[i] = buf_to.count();
        }

        buf_to.finalize();
        return col_to;
    }

    static void processUnit(
        UInt64 unit_size, const char * unit_name, size_t unit_name_size,
        Float64 & value, WriteBuffer & buf_to, bool & has_output)
    {
        if (unlikely(value + 1.0 == value))
        {
            /// The case when value is too large so exact representation for subsequent smaller units is not possible.
            writeText(std::floor(value / unit_size), buf_to);
            buf_to.write(unit_name, unit_name_size);
            writeChar('s', buf_to);
            has_output = true;
            value = 0;
            return;
        }

        UInt64 num_units = value / unit_size;

        if (!num_units)
        {
            /// Zero units, no need to print. But if it's the last (seconds) and the only unit, print "0 seconds" nevertheless.
            if (unit_size > 1 || has_output)
                return;
        }

        /// Remaining value to print on next iteration.
        value -= num_units * unit_size;

        if (has_output)
        {
            /// Need delimiter between values. The last delimiter is " and ", all previous are comma.
            if (value < 1)
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
};

}

REGISTER_FUNCTION(FormatReadableTimeDelta)
{
    factory.registerFunction<FunctionFormatReadableTimeDelta>();
}

}

