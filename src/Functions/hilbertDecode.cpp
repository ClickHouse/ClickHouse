#include <limits>
#include <Functions/FunctionFactory.h>
#include <Common/BitHelpers.h>
#include "hilbertDecode2DLUT.h"


namespace DB
{

class FunctionHilbertDecode : public FunctionSpaceFillingCurveDecode<2, 0, 32>
{
public:
    static constexpr auto name = "hilbertDecode";
    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionHilbertDecode>();
    }

    String getName() const override { return name; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        size_t num_dimensions;
        const auto * col_const = typeid_cast<const ColumnConst *>(arguments[0].column.get());
        const auto * mask = typeid_cast<const ColumnTuple *>(col_const->getDataColumnPtr().get());
        if (mask)
            num_dimensions = mask->tupleSize();
        else
            num_dimensions = col_const->getUInt(0);
        const ColumnPtr & col_code = arguments[1].column;
        Columns tuple_columns(num_dimensions);

        const auto shrink = [mask](const UInt64 value, const UInt8 column_num)
        {
            if (mask)
                return value >> mask->getColumn(column_num).getUInt(0);
            return value;
        };

        auto col0 = ColumnUInt64::create();
        auto & vec0 = col0->getData();
        vec0.resize(input_rows_count);

        if (num_dimensions == 1)
        {
            for (size_t i = 0; i < input_rows_count; i++)
            {
                vec0[i] = shrink(col_code->getUInt(i), 0);
            }
            tuple_columns[0] = std::move(col0);
            return ColumnTuple::create(tuple_columns);
        }

        auto col1 = ColumnUInt64::create();
        auto & vec1 = col1->getData();
        vec1.resize(input_rows_count);

        if (num_dimensions == 2)
        {
            for (size_t i = 0; i < input_rows_count; i++)
            {
                const auto res = FunctionHilbertDecode2DWIthLookupTableImpl<3>::decode(col_code->getUInt(i));
                vec0[i] = shrink(std::get<0>(res), 0);
                vec1[i] = shrink(std::get<1>(res), 1);
            }
            tuple_columns[0] = std::move(col0);
            tuple_columns[1] = std::move(col1);
            return ColumnTuple::create(tuple_columns);
        }

        return ColumnTuple::create(tuple_columns);
    }
};


REGISTER_FUNCTION(HilbertDecode)
{
    factory.registerFunction<FunctionHilbertDecode>(FunctionDocumentation{
    .description=R"(
Decodes a Hilbert curve index back into a tuple of unsigned integers, representing coordinates in multi-dimensional space.

The function has two modes of operation:
- Simple
- Expanded

Simple Mode: Accepts the desired tuple size as the first argument (up to 2) and the Hilbert index as the second argument. This mode decodes the index into a tuple of the specified size.
[example:simple]
Will decode into: `(8, 0)`
The resulting tuple size cannot be more than 2

Expanded Mode: Takes a range mask (tuple) as the first argument and the Hilbert index as the second argument.
Each number in the mask specifies the number of bits by which the corresponding decoded argument will be right-shifted, effectively scaling down the output values.
[example:range_shrank]
Note: see hilbertEncode() docs on why range change might be beneficial.
Still limited to 2 numbers at most.

Hilbert code for one argument is always the argument itself (as a tuple).
[example:identity]
Produces: `(1)`

A single argument with a tuple specifying bit shifts will be right-shifted accordingly.
[example:identity_shrank]
Produces: `(128)`

The function accepts a column of codes as a second argument:
[example:from_table]

The range tuple must be a constant:
[example:from_table_range]
)",
        .examples{
            {"simple", "SELECT hilbertDecode(2, 64)", ""},
            {"range_shrank", "SELECT hilbertDecode((1,2), 1572864)", ""},
            {"identity", "SELECT hilbertDecode(1, 1)", ""},
            {"identity_shrank", "SELECT hilbertDecode(tuple(2), 512)", ""},
            {"from_table", "SELECT hilbertDecode(2, code) FROM table", ""},
            {"from_table_range", "SELECT hilbertDecode((1,2), code) FROM table", ""},
            },
        .categories {"Hilbert coding", "Hilbert Curve"}
    });
}

}
