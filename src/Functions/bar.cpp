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

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 3 && arguments.size() != 4)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Function {} requires from 3 or 4 parameters: value, min_value, max_value, [max_width_of_bar = 80]. "
                    "Passed {}.", getName(), arguments.size());

        if (!isNumber(arguments[0]) || !isNumber(arguments[1]) || !isNumber(arguments[2])
            || (arguments.size() == 4 && !isNumber(arguments[3])))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "All arguments for function {} must be numeric.", getName());

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
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
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument 'max_width' must not be NaN");

        if (max_width < 1)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Argument 'max_width' must be >= 1");

        if (max_width > 1000)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Argument 'max_width' must be <= 1000");

        const auto & src = *arguments[0].column;

        size_t current_offset = 0;

        auto res_column = ColumnString::create();

        ColumnString::Chars & dst_chars = res_column->getChars();
        ColumnString::Offsets & dst_offsets = res_column->getOffsets();

        dst_offsets.resize(input_rows_count);
        dst_chars.reserve(input_rows_count * UnicodeBar::getWidthInBytes(max_width));

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            Float64 width = UnicodeBar::getWidth(
                src.getFloat64(i),
                arguments[1].column->getFloat64(i),
                arguments[2].column->getFloat64(i),
                max_width);

            if (!isFinite(width))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Value of width must not be NaN and Inf");

            size_t next_size = current_offset + UnicodeBar::getWidthInBytes(width);
            dst_chars.resize(next_size);
            UnicodeBar::render(width, reinterpret_cast<char *>(&dst_chars[current_offset]), reinterpret_cast<char *>(&dst_chars[next_size]));
            current_offset = next_size;
            dst_offsets[i] = current_offset;
        }

        return res_column;
    }
};

}

REGISTER_FUNCTION(Bar)
{
    FunctionDocumentation::Description description = R"(
Builds a bar chart.
Draws a band with width proportional to (x - min) and equal to width characters when x = max.
The band is drawn with accuracy to one eighth of a symbol.
)";
    FunctionDocumentation::Syntax syntax = "bar(x, min, max[, width])";
    FunctionDocumentation::Arguments arguments = {
        {"x", "Size to display.", {"(U)Int*", "Float*", "Decimal"}},
        {"min", "The minimum value.", {"const Int64"}},
        {"max", "The maximum value.", {"const Int64"}},
        {"width", "Optional. The width of the bar in characters. The default is `80`.", {"const (U)Int*", "const Float*", "const Decimal"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a unicode-art bar string.", {"String"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
SELECT
toHour(EventTime) AS h,
count() AS c,
bar(c, 0, 600000, 20) AS bar
FROM test.hits
GROUP BY h
ORDER BY h ASC
        )",
        R"(
┌──h─┬──────c─┬─bar────────────────┐
│  0 │ 292907 │ █████████▋         │
│  1 │ 180563 │ ██████             │
│  2 │ 114861 │ ███▋               │
│  3 │  85069 │ ██▋                │
│  4 │  68543 │ ██▎                │
│  5 │  78116 │ ██▌                │
│  6 │ 113474 │ ███▋               │
│  7 │ 170678 │ █████▋             │
│  8 │ 278380 │ █████████▎         │
│  9 │ 391053 │ █████████████      │
│ 10 │ 457681 │ ███████████████▎   │
│ 11 │ 493667 │ ████████████████▍  │
│ 12 │ 509641 │ ████████████████▊  │
│ 13 │ 522947 │ █████████████████▍ │
│ 14 │ 539954 │ █████████████████▊ │
│ 15 │ 528460 │ █████████████████▌ │
│ 16 │ 539201 │ █████████████████▊ │
│ 17 │ 523539 │ █████████████████▍ │
│ 18 │ 506467 │ ████████████████▊  │
│ 19 │ 520915 │ █████████████████▎ │
│ 20 │ 521665 │ █████████████████▍ │
│ 21 │ 542078 │ ██████████████████ │
│ 22 │ 493642 │ ████████████████▍  │
│ 23 │ 400397 │ █████████████▎     │
└────┴────────┴────────────────────┘
        )"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionBar>(documentation);
}

}
