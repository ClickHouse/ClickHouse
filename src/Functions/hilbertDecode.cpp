#include <Common/BitHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionSpaceFillingCurve.h>
#include <Functions/PerformanceAdaptors.h>
#include <limits>


namespace DB
{

namespace HilbertDetails
{

template <UInt8 bit_step>
class HilbertDecodeLookupTable
{
public:
    constexpr static UInt8 LOOKUP_TABLE[0] = {};
};

template <>
class HilbertDecodeLookupTable<1>
{
public:
    constexpr static UInt8 LOOKUP_TABLE[16] = {
        4, 1, 3, 10,
        0, 6, 7, 13,
        15, 9, 8, 2,
        11, 14, 12, 5
    };
};

template <>
class HilbertDecodeLookupTable<2>
{
public:
    constexpr static UInt8 LOOKUP_TABLE[64] = {
        0, 20, 21, 49, 18, 3, 7, 38,
        26, 11, 15, 46, 61, 41, 40, 12,
        16, 1, 5, 36, 8, 28, 29, 57,
        10, 30, 31, 59, 39, 54, 50, 19,
        47, 62, 58, 27, 55, 35, 34, 6,
        53, 33, 32, 4, 24, 9, 13, 44,
        63, 43, 42, 14, 45, 60, 56, 25,
        37, 52, 48, 17, 2, 22, 23, 51
    };
};

template <>
class HilbertDecodeLookupTable<3>
{
public:
    constexpr static UInt8 LOOKUP_TABLE[256] = {
        64, 1, 9, 136, 16, 88, 89, 209, 18, 90, 91, 211, 139, 202, 194, 67,
        4, 76, 77, 197, 70, 7, 15, 142, 86, 23, 31, 158, 221, 149, 148, 28,
        36, 108, 109, 229, 102, 39, 47, 174, 118, 55, 63, 190, 253, 181, 180, 60,
        187, 250, 242, 115, 235, 163, 162, 42, 233, 161, 160, 40, 112, 49, 57, 184,
        0, 72, 73, 193, 66, 3, 11, 138, 82, 19, 27, 154, 217, 145, 144, 24,
        96, 33, 41, 168, 48, 120, 121, 241, 50, 122, 123, 243, 171, 234, 226, 99,
        100, 37, 45, 172, 52, 124, 125, 245, 54, 126, 127, 247, 175, 238, 230, 103,
        223, 151, 150, 30, 157, 220, 212, 85, 141, 204, 196, 69, 6, 78, 79, 199,
        255, 183, 182, 62, 189, 252, 244, 117, 173, 236, 228, 101, 38, 110, 111, 231,
        159, 222, 214, 87, 207, 135, 134, 14, 205, 133, 132, 12, 84, 21, 29, 156,
        155, 218, 210, 83, 203, 131, 130, 10, 201, 129, 128, 8, 80, 17, 25, 152,
        32, 104, 105, 225, 98, 35, 43, 170, 114, 51, 59, 186, 249, 177, 176, 56,
        191, 254, 246, 119, 239, 167, 166, 46, 237, 165, 164, 44, 116, 53, 61, 188,
        251, 179, 178, 58, 185, 248, 240, 113, 169, 232, 224, 97, 34, 106, 107, 227,
        219, 147, 146, 26, 153, 216, 208, 81, 137, 200, 192, 65, 2, 74, 75, 195,
        68, 5, 13, 140, 20, 92, 93, 213, 22, 94, 95, 215, 143, 206, 198, 71
    };
};

}


template <UInt8 bit_step>
class FunctionHilbertDecode2DWIthLookupTableImpl
{
    static_assert(bit_step <= 3, "bit_step should not be more than 3 to fit in UInt8");
public:
    static std::tuple<UInt64, UInt64> decode(UInt64 hilbert_code)
    {
        UInt64 x = 0;
        UInt64 y = 0;
        const auto leading_zeros_count = getLeadingZeroBits(hilbert_code);
        const auto used_bits = std::numeric_limits<UInt64>::digits - leading_zeros_count;

        auto [current_shift, state] = getInitialShiftAndState(used_bits);

        while (current_shift >= 0)
        {
            const UInt8 hilbert_bits = (hilbert_code >> current_shift) & HILBERT_MASK;
            const auto [x_bits, y_bits] = getCodeAndUpdateState(hilbert_bits, state);
            x |= (x_bits << (current_shift >> 1));
            y |= (y_bits << (current_shift >> 1));
            current_shift -= getHilbertShift(bit_step);
        }

        return {x, y};
    }

private:
    // for bit_step = 3
    // LOOKUP_TABLE[SSHHHHHH] = SSXXXYYY
    // where SS - 2 bits for state, XXX - 3 bits of x, YYY - 3 bits of y
    // State is rotation of curve on every step, left/up/right/down - therefore 2 bits
    static std::pair<UInt64, UInt64> getCodeAndUpdateState(UInt8 hilbert_bits, UInt8& state)
    {
        const UInt8 table_index = state | hilbert_bits;
        const auto table_code = HilbertDetails::HilbertDecodeLookupTable<bit_step>::LOOKUP_TABLE[table_index];
        state = table_code & STATE_MASK;
        const UInt64 x_bits = (table_code & X_MASK) >> bit_step;
        const UInt64 y_bits = table_code & Y_MASK;
        return {x_bits, y_bits};
    }

    // hilbert code is double size of input values
    static constexpr UInt8 getHilbertShift(UInt8 shift)
    {
        return shift << 1;
    }

    static std::pair<Int8, UInt8> getInitialShiftAndState(UInt8 used_bits)
    {
        UInt8 iterations = used_bits / HILBERT_SHIFT;
        Int8 initial_shift = iterations * HILBERT_SHIFT;
        if (initial_shift < used_bits)
        {
            ++iterations;
        }
        else
        {
            initial_shift -= HILBERT_SHIFT;
        }
        UInt8 state = iterations % 2 == 0 ? LEFT_STATE : DEFAULT_STATE;
        return {initial_shift, state};
    }

    constexpr static UInt8 STEP_MASK = (1 << bit_step) - 1;
    constexpr static UInt8 HILBERT_SHIFT = getHilbertShift(bit_step);
    constexpr static UInt8 HILBERT_MASK = (1 << HILBERT_SHIFT) - 1;
    constexpr static UInt8 STATE_MASK = 0b11 << HILBERT_SHIFT;
    constexpr static UInt8 Y_MASK = STEP_MASK;
    constexpr static UInt8 X_MASK = STEP_MASK << bit_step;
    constexpr static UInt8 LEFT_STATE = 0b01 << HILBERT_SHIFT;
    constexpr static UInt8 DEFAULT_STATE = bit_step % 2 == 0 ? LEFT_STATE : 0;
};


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
        auto non_const_arguments = arguments;
        non_const_arguments[1].column = non_const_arguments[1].column->convertToFullColumnIfConst();
        const ColumnPtr & col_code = non_const_arguments[1].column;
        Columns tuple_columns(num_dimensions);

        const auto shrink = [mask](const UInt64 value, const UInt8 column_id)
        {
            if (mask)
                return value >> mask->getColumn(column_id).getUInt(0);
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
