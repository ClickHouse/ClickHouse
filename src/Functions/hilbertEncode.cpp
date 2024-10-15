#include <limits>
#include <optional>
#include <Functions/FunctionFactory.h>
#include <Common/BitHelpers.h>
#include "hilbertEncode2DLUT.h"


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
    factory.registerFunction<FunctionHilbertEncode>(FunctionDocumentation{
        .description=R"(
Calculates code for Hilbert Curve for a list of unsigned integers.

The function has two modes of operation:
- Simple
- Expanded

Simple: accepts up to 2 unsigned integers as arguments and produces a UInt64 code.
[example:simple]
Produces: `31`

Expanded: accepts a range mask (tuple) as a first argument and up to 2 unsigned integers as other arguments.
Each number in the mask configures the number of bits by which the corresponding argument will be shifted left, effectively scaling the argument within its range.
[example:range_expanded]
Produces: `4031541586602`
Note: tuple size must be equal to the number of the other arguments

Range expansion can be beneficial when you need a similar distribution for arguments with wildly different ranges (or cardinality)
For example: 'IP Address' (0...FFFFFFFF) and 'Country code' (0...FF)

For a single argument without a tuple, the function returns the argument itself as the Hilbert index, since no dimensional mapping is needed.
[example:identity]
Produces: `1`

If a single argument is provided with a tuple specifying bit shifts, the function shifts the argument left by the specified number of bits.
[example:identity_expanded]
Produces: `512`

The function also accepts columns as arguments:
[example:from_table]

But the range tuple must still be a constant:
[example:from_table_range]

Please note that you can fit only so much bits of information into Hilbert code as UInt64 has.
Two arguments will have a range of maximum 2^32 (64/2) each
All overflow will be clamped to zero
)",
        .examples{
            {"simple", "SELECT hilbertEncode(3, 4)", ""},
            {"range_expanded", "SELECT hilbertEncode((10,6), 1024, 16)", ""},
            {"identity", "SELECT hilbertEncode(1)", ""},
            {"identity_expanded", "SELECT hilbertEncode(tuple(2), 128)", ""},
            {"from_table", "SELECT hilbertEncode(n1, n2) FROM table", ""},
            {"from_table_range", "SELECT hilbertEncode((1,2), n1, n2) FROM table", ""},
        },
        .categories {"Hilbert coding", "Hilbert Curve"}
    });
}

}
