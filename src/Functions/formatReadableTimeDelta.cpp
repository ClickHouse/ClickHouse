#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
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

class FunctionFormatReadableTimeDelta : public IFunction
{
public:
    static constexpr auto name = "formatReadableTimeDelta";
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionFormatReadableTimeDelta>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 1)
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

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) const override
    {
        StringRef maximum_unit_str;
        if (arguments.size() == 2)
        {
            const ColumnPtr & maximum_unit_column = block.getByPosition(arguments[1]).column;
            const ColumnConst * maximum_unit_const_col = checkAndGetColumnConstStringOrFixedString(maximum_unit_column.get());
            if (maximum_unit_const_col)
                maximum_unit_str = maximum_unit_const_col->getDataColumn().getDataAt(0);
        }

        Unit max_unit;
        if (maximum_unit_str.size == 0 || maximum_unit_str == "years")
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
                maximum_unit_str.toString(), getName());

        auto col_to = ColumnString::create();

        ColumnString::Chars & data_to = col_to->getChars();
        ColumnString::Offsets & offsets_to = col_to->getOffsets();
        offsets_to.resize(input_rows_count);

        WriteBufferFromVector<ColumnString::Chars> buf_to(data_to);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            Float64 value = block.getByPosition(arguments[0]).column->getFloat64(i);
            bool is_negative = value < 0;
            if (is_negative)
            {
                writeChar('-', buf_to);
                value = -value;
            }

            bool has_output = false;

            switch (max_unit)
            {
                case Years:     processUnit(365 * 24 * 3600, " year", 5, value, buf_to, has_output); [[fallthrough]];
                case Months:    processUnit(30.5 * 24 * 3600, " month", 6, value, buf_to, has_output); [[fallthrough]];
                case Days:      processUnit(24 * 3600, " day", 4, value, buf_to, has_output); [[fallthrough]];
                case Hours:     processUnit(3600, " hour", 5, value, buf_to, has_output); [[fallthrough]];
                case Minutes:   processUnit(60, " minute", 7, value, buf_to, has_output); [[fallthrough]];
                case Seconds:   processUnit(1, " second", 7, value, buf_to, has_output);
            }

            writeChar(0, buf_to);
            offsets_to[i] = buf_to.count();
        }

        buf_to.finalize();
        block.getByPosition(result).column = std::move(col_to);
    }

    static void processUnit(
        UInt64 unit_size, const char * unit_name, size_t unit_name_size,
        Float64 & value, WriteBuffer & buf_to, bool & has_output)
    {
        if (unlikely(value + 1.0 == value))
        {
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
            if (unit_size > 1 || has_output)
                return;
        }

        value -= num_units * unit_size;

        if (has_output)
        {
            if (value < 1)
                writeCString(" and ", buf_to);
            else
                writeCString(", ", buf_to);
        }

        writeText(num_units, buf_to);
        buf_to.write(unit_name, unit_name_size);
        if (num_units != 1)
            writeChar('s', buf_to);

        has_output = true;
    }
};

}

void registerFunctionFormatReadableTimeDelta(FunctionFactory & factory)
{
    factory.registerFunction<FunctionFormatReadableTimeDelta>();
}

}

