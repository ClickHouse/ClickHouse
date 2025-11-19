#pragma once
#include <Functions/FunctionSpaceFillingCurve.h>


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

}
