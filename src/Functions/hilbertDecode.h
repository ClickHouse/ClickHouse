#pragma once
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Common/BitHelpers.h>
#include <Functions/FunctionSpaceFillingCurve.h>
#include <Functions/IFunction.h>
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
class HilbertDecodeLookupTable<3>
{
public:
    constexpr static UInt8 LOOKUP_TABLE[256] = {
        64, 1, 9, 136, 16, 88, 89, 209, 18, 90, 91, 211, 139, 202, 194, 67, 4, 76, 77, 197, 70, 7,
        15, 142, 86, 23, 31, 158, 221, 149, 148, 28, 36, 108, 109, 229, 102, 39, 47, 174, 118, 55,
        63, 190, 253, 181, 180, 60, 187, 250, 242, 115, 235, 163, 162, 42, 233, 161, 160, 40, 112,
        49, 57, 184, 0, 72, 73, 193, 66, 3, 11, 138, 82, 19, 27, 154, 217, 145, 144, 24, 96, 33,
        41, 168, 48, 120, 121, 241, 50, 122, 123, 243, 171, 234, 226, 99, 100, 37, 45, 172, 52,
        124, 125, 245, 54, 126, 127, 247, 175, 238, 230, 103, 223, 151, 150, 30, 157, 220, 212, 85,
        141, 204, 196, 69, 6, 78, 79, 199, 255, 183, 182, 62, 189, 252, 244, 117, 173, 236, 228,
        101, 38, 110, 111, 231, 159, 222, 214, 87, 207, 135, 134, 14, 205, 133, 132, 12, 84, 21,
        29, 156, 155, 218, 210, 83, 203, 131, 130, 10, 201, 129, 128, 8, 80, 17, 25, 152, 32, 104,
        105, 225, 98, 35, 43, 170, 114, 51, 59, 186, 249, 177, 176, 56, 191, 254, 246, 119, 239,
        167, 166, 46, 237, 165, 164, 44, 116, 53, 61, 188, 251, 179, 178, 58, 185, 248, 240, 113,
        169, 232, 224, 97, 34, 106, 107, 227, 219, 147, 146, 26, 153, 216, 208, 81, 137, 200, 192,
        65, 2, 74, 75, 195, 68, 5, 13, 140, 20, 92, 93, 213, 22, 94, 95, 215, 143, 206, 198, 71
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
        const UInt8 hilbert_shift = getHilbertShift(bit_step);
        UInt8 iterations = used_bits / hilbert_shift;
        Int8 initial_shift = iterations * hilbert_shift;
        if (initial_shift < used_bits)
        {
            ++iterations;
        }
        else
        {
            initial_shift -= hilbert_shift;
        }
        UInt8 state = iterations % 2 == 0 ? 0b01 << hilbert_shift : 0;
        return {initial_shift, state};
    }

    constexpr static UInt8 STEP_MASK = (1 << bit_step) - 1;
    constexpr static UInt8 HILBERT_MASK = (1 << getHilbertShift(bit_step)) - 1;
    constexpr static UInt8 STATE_MASK = 0b11 << getHilbertShift(bit_step);
    constexpr static UInt8 Y_MASK = STEP_MASK;
    constexpr static UInt8 X_MASK = STEP_MASK << bit_step;
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
        size_t nd;
        const auto * col_const = typeid_cast<const ColumnConst *>(arguments[0].column.get());
        const auto * mask = typeid_cast<const ColumnTuple *>(col_const->getDataColumnPtr().get());
        if (mask)
            nd = mask->tupleSize();
        else
            nd = col_const->getUInt(0);
        auto non_const_arguments = arguments;
        non_const_arguments[1].column = non_const_arguments[1].column->convertToFullColumnIfConst();
        const ColumnPtr & col_code = non_const_arguments[1].column;
        Columns tuple_columns(nd);

        const auto shrink = [mask](const UInt64 value, const UInt8 column_id)
        {
            if (mask)
                return value >> mask->getColumn(column_id).getUInt(0);
            return value;
        };

        auto col0 = ColumnUInt64::create();
        auto & vec0 = col0->getData();
        vec0.resize(input_rows_count);

        if (nd == 1)
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

        if (nd == 2)
        {
            for (size_t i = 0; i < input_rows_count; i++)
            {
                const auto res = FunctionHilbertDecode2DWIthLookupTableImpl<3>::decode(col_code->getUInt(i));
                vec0[i] = shrink(std::get<0>(res), 0);
                vec1[i] = shrink(std::get<1>(res), 1);
            }
            tuple_columns[0] = std::move(col0);
            return ColumnTuple::create(tuple_columns);
        }

        return ColumnTuple::create(tuple_columns);
    }
};

}
