#pragma once
#include <Functions/FunctionSpaceFillingCurve.h>


namespace DB
{

namespace HilbertDetails
{

template <UInt8 bit_step>
class HilbertEncodeLookupTable
{
public:
    constexpr static UInt8 LOOKUP_TABLE[0] = {};
};

template <>
class HilbertEncodeLookupTable<1>
{
public:
    constexpr static UInt8 LOOKUP_TABLE[16] = {
        4, 1, 11, 2,
        0, 15, 5, 6,
        10, 9, 3, 12,
        14, 7, 13, 8
    };
};

template <>
class HilbertEncodeLookupTable<2>
{
public:
    constexpr static UInt8 LOOKUP_TABLE[64] = {
        0, 51, 20, 5, 17, 18, 39, 6,
        46, 45, 24, 9, 15, 60, 43, 10,
        16, 1, 62, 31, 35, 2, 61, 44,
        4, 55, 8, 59, 21, 22, 25, 26,
        42, 41, 38, 37, 11, 56, 7, 52,
        28, 13, 50, 19, 47, 14, 49, 32,
        58, 27, 12, 63, 57, 40, 29, 30,
        54, 23, 34, 33, 53, 36, 3, 48
    };
};


template <>
class HilbertEncodeLookupTable<3>
{
public:
    constexpr static UInt8 LOOKUP_TABLE[256] = {
        64, 1, 206, 79, 16, 211, 84, 21, 131, 2, 205, 140, 81, 82, 151, 22, 4,
        199, 8, 203, 158, 157, 88, 25, 69, 70, 73, 74, 31, 220, 155, 26, 186,
        185, 182, 181, 32, 227, 100, 37, 59, 248, 55, 244, 97, 98, 167, 38, 124,
        61, 242, 115, 174, 173, 104, 41, 191, 62, 241, 176, 47, 236, 171, 42, 0,
        195, 68, 5, 250, 123, 60, 255, 65, 66, 135, 6, 249, 184, 125, 126, 142,
        141, 72, 9, 246, 119, 178, 177, 15, 204, 139, 10, 245, 180, 51, 240, 80,
        17, 222, 95, 96, 33, 238, 111, 147, 18, 221, 156, 163, 34, 237, 172, 20,
        215, 24, 219, 36, 231, 40, 235, 85, 86, 89, 90, 101, 102, 105, 106, 170,
        169, 166, 165, 154, 153, 150, 149, 43, 232, 39, 228, 27, 216, 23, 212, 108,
        45, 226, 99, 92, 29, 210, 83, 175, 46, 225, 160, 159, 30, 209, 144, 48,
        243, 116, 53, 202, 75, 12, 207, 113, 114, 183, 54, 201, 136, 77, 78, 190,
        189, 120, 57, 198, 71, 130, 129, 63, 252, 187, 58, 197, 132, 3, 192, 234,
        107, 44, 239, 112, 49, 254, 127, 233, 168, 109, 110, 179, 50, 253, 188, 230,
        103, 162, 161, 52, 247, 56, 251, 229, 164, 35, 224, 117, 118, 121, 122, 218,
        91, 28, 223, 138, 137, 134, 133, 217, 152, 93, 94, 11, 200, 7, 196, 214,
        87, 146, 145, 76, 13, 194, 67, 213, 148, 19, 208, 143, 14, 193, 128,
    };
};

}

template <UInt8 bit_step>
class FunctionHilbertEncode2DWIthLookupTableImpl
{
    static_assert(bit_step <= 3, "bit_step should not be more than 3 to fit in UInt8");
public:
    static UInt64 encode(UInt64 x, UInt64 y)
    {
        UInt64 hilbert_code = 0;
        const auto leading_zeros_count = getLeadingZeroBits(x | y);
        const auto used_bits = std::numeric_limits<UInt64>::digits - leading_zeros_count;
        if (used_bits > 32)
            return 0; // hilbert code will be overflowed in this case

        auto [current_shift, state] = getInitialShiftAndState(used_bits);
        while (current_shift >= 0)
        {
            const UInt8 x_bits = (x >> current_shift) & STEP_MASK;
            const UInt8 y_bits = (y >> current_shift) & STEP_MASK;
            const auto hilbert_bits = getCodeAndUpdateState(x_bits, y_bits, state);
            hilbert_code |= (hilbert_bits << getHilbertShift(current_shift));
            current_shift -= bit_step;
        }

        return hilbert_code;
    }

private:
    // for bit_step = 3
    // LOOKUP_TABLE[SSXXXYYY] = SSHHHHHH
    // where SS - 2 bits for state, XXX - 3 bits of x, YYY - 3 bits of y
    // State is rotation of curve on every step, left/up/right/down - therefore 2 bits
    static UInt64 getCodeAndUpdateState(UInt8 x_bits, UInt8 y_bits, UInt8& state)
    {
        const UInt8 table_index = state | (x_bits << bit_step) | y_bits;
        const auto table_code = HilbertDetails::HilbertEncodeLookupTable<bit_step>::LOOKUP_TABLE[table_index];
        state = table_code & STATE_MASK;
        return table_code & HILBERT_MASK;
    }

    // hilbert code is double size of input values
    static constexpr UInt8 getHilbertShift(UInt8 shift)
    {
        return shift << 1;
    }

    static std::pair<Int8, UInt8> getInitialShiftAndState(UInt8 used_bits)
    {
        UInt8 iterations = used_bits / bit_step;
        Int8 initial_shift = iterations * bit_step;
        if (initial_shift < used_bits)
        {
            ++iterations;
        }
        else
        {
            initial_shift -= bit_step;
        }
        UInt8 state = iterations % 2 == 0 ? LEFT_STATE : DEFAULT_STATE;
        return {initial_shift, state};
    }

    constexpr static UInt8 STEP_MASK = (1 << bit_step) - 1;
    constexpr static UInt8 HILBERT_SHIFT = getHilbertShift(bit_step);
    constexpr static UInt8 HILBERT_MASK = (1 << HILBERT_SHIFT) - 1;
    constexpr static UInt8 STATE_MASK = 0b11 << HILBERT_SHIFT;
    constexpr static UInt8 LEFT_STATE = 0b01 << HILBERT_SHIFT;
    constexpr static UInt8 DEFAULT_STATE = bit_step % 2 == 0 ? LEFT_STATE : 0;
};

}
