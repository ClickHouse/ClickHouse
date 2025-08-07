#include <optional>
#include <Functions/FunctionFactory.h>
#include <Functions/hilbertEncode2DLUT.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}


class FunctionHilbertEncode : public FunctionSpaceFillingCurveEncode
{
public:
    static constexpr auto name = "hilbertEncode";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionHilbertEncode>();
    }

    String getName() const override { return name; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return ColumnUInt64::create();

        size_t num_dimensions = arguments.size();
        size_t vector_start_index = 0;
        const auto * const_col = typeid_cast<const ColumnConst *>(arguments[0].column.get());
        const ColumnTuple * mask;
        if (const_col)
            mask = typeid_cast<const ColumnTuple *>(const_col->getDataColumnPtr().get());
        else
            mask = typeid_cast<const ColumnTuple *>(arguments[0].column.get());
        if (mask)
        {
            num_dimensions = mask->tupleSize();
            vector_start_index = 1;
            for (size_t i = 0; i < num_dimensions; i++)
            {
                auto ratio = mask->getColumn(i).getUInt(0);
                if (ratio > 32)
                    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                    "Illegal argument {} of function {}, should be a number in range 0-32",
                                    arguments[0].column->getName(), getName());
            }
        }

        auto col_res = ColumnUInt64::create();
        ColumnUInt64::Container & vec_res = col_res->getData();
        vec_res.resize(input_rows_count);

        const auto expand = [mask](const UInt64 value, const UInt8 column_num)
        {
            if (mask)
                return value << mask->getColumn(column_num).getUInt(0);
            return value;
        };

        const ColumnPtr & col0 = arguments[0 + vector_start_index].column;
        if (num_dimensions == 1)
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                vec_res[i] = expand(col0->getUInt(i), 0);
            }
            return col_res;
        }

        const ColumnPtr & col1 = arguments[1 + vector_start_index].column;
        if (num_dimensions == 2)
        {
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                vec_res[i] = FunctionHilbertEncode2DWIthLookupTableImpl<3>::encode(
                    expand(col0->getUInt(i), 0),
                    expand(col1->getUInt(i), 1));
            }
            return col_res;
        }

        throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                        "Illegal number of UInt arguments of function {}: should be not more than 2 dimensions",
                        getName());
    }
};


REGISTER_FUNCTION(HilbertEncode)
{
    FunctionDocumentation::Description description = R"(
Calculates code for Hilbert Curve for a list of unsigned integers.

The function has two modes of operation:
- **Simple**
- **Expanded**

**Simple mode**

Accepts up to 2 unsigned integers as arguments and produces a UInt64 code.

**Expanded mode**

Accepts a range mask ([Tuple](../../sql-reference/data-types/tuple.md)) as the
first argument and up to 2 [unsigned integers](../../sql-reference/data-types/int-uint.md)
as other arguments.

Each number in the mask configures the number of bits by which the corresponding
argument will be shifted left, effectively scaling the argument within its range.
    )";
    FunctionDocumentation::Syntax syntax = R"(
-- Simplified mode
hilbertEncode(args)

-- Expanded mode
hilbertEncode(range_mask, args)
)";
    FunctionDocumentation::Arguments arguments = {
        {"args", "Up to two `UInt` values or columns of type `UInt`.", {"UInt8/16/32/64"}},
        {"range_mask", "For the expanded mode, up to two `UInt` values or columns of type `UInt`.", {"UInt8/16/32/64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a `UInt64` code.", {"UInt64"}};
    FunctionDocumentation::Examples examples = {
        {"Simple mode", "SELECT hilbertEncode(3, 4)", "31"},
        {
            "Expanded mode",
            R"(
-- Range expansion can be beneficial when you need a similar distribution for
-- arguments with wildly different ranges (or cardinality).
-- For example: 'IP Address' (0...FFFFFFFF) and 'Country code' (0...FF).
-- Note: tuple size must be equal to the number of the other arguments.
SELECT hilbertEncode((10, 6), 1024, 16)
            )",
            R"(
4031541586602
            )"
        },
        {
            "Single argument",
            R"(
-- For a single argument without a tuple, the function returns the argument
-- itself as the Hilbert index, since no dimensional mapping is needed.
SELECT hilbertEncode(1)
            )",
            "1"
        },
        {
            "Expanded single argument",
            R"(
-- If a single argument is provided with a tuple specifying bit shifts, the function
-- shifts the argument left by the specified number of bits.
SELECT hilbertEncode(tuple(2), 128)
            )",
            "512"
        },
        {
            "Column usage",
             R"(
-- First create the table and insert some data
CREATE TABLE hilbert_numbers(
    n1 UInt32,
    n2 UInt32
)
ENGINE=MergeTree()
ORDER BY n1;
insert into hilbert_numbers (*) values(1, 2);

-- Use column names instead of constants as function arguments
SELECT hilbertEncode(n1, n2) FROM hilbert_numbers;
             )",
             "13"
            }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {24, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Encoding;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHilbertEncode>(documentation);
}

}
