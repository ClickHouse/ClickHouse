#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Common/UnicodeBar.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/** bar(x, min, max, width) - draws a strip from the number of characters proportional to (x - min) and equal to width for x == max.
  * Returns a string with nice Unicode-art bar with resolution of 1/8 part of symbol.
  */
class FunctionBar : public IFunction
{
public:
    static constexpr auto name = "bar";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionBar>();
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override
    {
        return true;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 3 && arguments.size() != 4)
            throw Exception("Function " + getName()
                    + " requires from 3 or 4 parameters: value, min_value, max_value, [max_width_of_bar = 80]. Passed "
                    + toString(arguments.size())
                    + ".",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isNumber(arguments[0]) || !isNumber(arguments[1]) || !isNumber(arguments[2])
            || (arguments.size() == 4 && !isNumber(arguments[3])))
            throw Exception("All arguments for function " + getName() + " must be numeric.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {3}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        /// The maximum width of the bar in characters.
        Float64 max_width = 80; /// Motivated by old-school terminal size.

        if (arguments.size() == 4)
        {
            const auto & max_width_column = *arguments[3].column;

            if (!isColumnConst(max_width_column))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Fourth argument for function {} must be constant", getName());

            max_width = max_width_column.getFloat64(0);
        }

        if (isNaN(max_width))
            throw Exception("Argument 'max_width' must not be NaN", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (max_width < 1)
            throw Exception("Argument 'max_width' must be >= 1", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        if (max_width > 1000)
            throw Exception("Argument 'max_width' must be <= 1000", ErrorCodes::ARGUMENT_OUT_OF_BOUND);

        const auto & src = *arguments[0].column;

        size_t current_offset = 0;

        auto res_column = ColumnString::create();

        ColumnString::Chars & dst_chars = res_column->getChars();
        ColumnString::Offsets & dst_offsets = res_column->getOffsets();

        dst_offsets.resize(input_rows_count);
        dst_chars.reserve(input_rows_count * (UnicodeBar::getWidthInBytes(max_width) + 1)); /// strings are 0-terminated.

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            Float64 width = UnicodeBar::getWidth(
                src.getFloat64(i),
                arguments[1].column->getFloat64(i),
                arguments[2].column->getFloat64(i),
                max_width);

            if (!isFinite(width))
                throw Exception("Value of width must not be NaN and Inf", ErrorCodes::BAD_ARGUMENTS);

            size_t next_size = current_offset + UnicodeBar::getWidthInBytes(width) + 1;
            dst_chars.resize(next_size);
            UnicodeBar::render(width, reinterpret_cast<char *>(&dst_chars[current_offset]));
            current_offset = next_size;
            dst_offsets[i] = current_offset;
        }

        return res_column;
    }
};

}

void registerFunctionBar(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBar>();
}

}
