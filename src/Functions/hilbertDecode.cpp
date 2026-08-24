#include <limits>
#include <Functions/FunctionFactory.h>
#include <Common/BitHelpers.h>
#include <Functions/hilbertDecode2DLUT.h>


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
    FunctionDocumentation::Description description = R"(
Decodes a Hilbert curve index back into a tuple of unsigned integers, representing coordinates in multi-dimensional space.

As with the `hilbertEncode` function, this function has two modes of operation:
- **Simple**
- **Expanded**

**Simple mode**

Accepts up to 2 unsigned integers as arguments and produces a `UInt64` code.

**Expanded mode**

Accepts a range mask (tuple) as a first argument and up to 2 unsigned integers as
other arguments. Each number in the mask configures the number of bits by which
the corresponding argument will be shifted left, effectively scaling the argument
within its range.

Range expansion can be beneficial when you need a similar distribution for
arguments with wildly different ranges (or cardinality) For example: 'IP Address' `(0...FFFFFFFF)`
and 'Country code' `(0...FF)`. As with the encode function, this is limited to 8
numbers at most.
    )";
    FunctionDocumentation::Syntax syntax = R"(
hilbertDecode(tuple_size, code)
)";
    FunctionDocumentation::Arguments arguments = {
        {"tuple_size", "Integer value of no more than `2`.", {"UInt8/16/32/64", "Tuple(UInt8/16/32/64)"}},
        {"code", "`UInt64` code.", {"UInt64"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns a tuple of the specified size.", {"Tuple(UInt64)"}};
    FunctionDocumentation::Examples examples = {
        {
            "Simple mode",
            "SELECT hilbertDecode(2, 31)",
            R"(["3", "4"])"
        },
        {
            "Single argument", R"(
-- Hilbert code for one argument is always the argument itself (as a tuple).
SELECT hilbertDecode(1, 1)
            )",
            R"(["1"])"
        },
        {
            "Expanded mode",
            R"(
-- A single argument with a tuple specifying bit shifts will be right-shifted accordingly.
SELECT hilbertDecode(tuple(2), 32768)
            )",
            R"(["128"])"
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
ORDER BY n1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into hilbert_numbers (*) values(1,2);

-- Use column names instead of constants as function arguments
SELECT untuple(hilbertDecode(2, hilbertEncode(n1, n2))) FROM hilbert_numbers;
            )",
            "1    2"
        }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {24, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Encoding;
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHilbertDecode>(documentation);
}

}
