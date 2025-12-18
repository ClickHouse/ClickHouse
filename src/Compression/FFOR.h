#pragma once

// This file implements a FFOR algorithm inspired by the FastLanes project:
// https://github.com/cwida/FastLanes
//
// Original work by Azim Afroozeh and the CWI Database Architectures Group, licensed under the MIT License.
//
// This implementation uses template-based C++ generator approach and adapted to the CH codebase and style.

#include <DataTypes/IDataType.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

namespace Compression::FFOR
{
namespace Details
{
template <typename T>
concept UnsignedInteger = std::is_integral_v<T> && std::is_unsigned_v<T> && !std::is_same_v<T, bool>;

template <UnsignedInteger T>
consteval UInt16 get_width()
{
    return sizeof(T) * 8;
}

template <UnsignedInteger T, UInt16 values>
consteval UInt16 get_iterations()
{
    constexpr UInt16 width = get_width<T>();
    static_assert(values % width == 0);
    return values / width;
}

template <UnsignedInteger T, UInt16 bits, UInt16 step, UInt16 values>
ALWAYS_INLINE void bitPackStep(const T * __restrict in, T * __restrict out, const T base, const UInt16 index, T & agg)
{
    constexpr UInt16 width = get_width<T>();
    static_assert(bits <= width);
    static_assert(step <= width);

    constexpr UInt16 iterations = get_iterations<T, values>();

    if constexpr (bits > 0 && bits < width)
    {
        constexpr UInt16 shift = (step * bits) % width;
        constexpr T mask = (T{1} << bits) - 1;

        if constexpr (step > 0 && shift < bits)
        {
            constexpr UInt16 out_offset = iterations * (((step - 1) * bits) / width);
            out[out_offset + index] = agg;

            if constexpr (step >= width)
                return;

            if constexpr (shift > 0)
            {
                constexpr UInt16 in_prev_offset = iterations * (step - 1);
                constexpr UInt16 prev_shit = bits - shift;
                T v = in[in_prev_offset + index] - base;
                v &= mask;
                v >>= prev_shit;
                agg = v;
            }
            else
                agg = 0;
        }
        else if constexpr (step == 0)
            agg = 0;

        if constexpr (step < width)
        {
            constexpr UInt16 in_offset = iterations * step;
            T v = in[in_offset + index] - base;
            v &= mask;
            v <<= shift;
            agg |= v;
        }
    }
    else if constexpr (bits == width && step < width)
    {
        constexpr UInt16 offset = iterations * step;
        out[offset + index] = in[offset + index] - base;
    }
}

template <UnsignedInteger T, UInt16 bits, UInt16 values>
void bitPack(const T * __restrict in, T * __restrict out, const T base)
{
    if constexpr (bits == 0)
        return;

    constexpr UInt16 width = get_width<T>();
    static_assert(bits <= width);

    constexpr UInt16 iterations = get_iterations<T, values>();

    for (UInt16 i = 0; i < iterations; ++i)
    {
        [&]<UInt16... step>(std::integer_sequence<UInt16, step ...>) ALWAYS_INLINE
        {
            T state = 0;
            ((bitPackStep<T, bits, step, values>(in, out, base, i, state)), ...);
        }(std::make_integer_sequence<UInt16, width + 1>{});
    }
}

template <UnsignedInteger T, UInt16 bits, UInt16 step, UInt16 values>
ALWAYS_INLINE void bitUnpackStep(const T * __restrict in, T * __restrict out, const T base, const UInt16 index, T & pack)
{
    constexpr UInt16 width = get_width<T>();
    static_assert(bits <= width);
    static_assert(step < width);

    constexpr UInt16 iterations = get_iterations<T, values>();
    constexpr UInt16 out_offset = iterations * step;

    if constexpr (bits == 0)
        out[index + out_offset] = base;
    else if constexpr (bits == width)
        out[index + out_offset] = in[index + out_offset] + base;
    else
    {
        constexpr UInt16 shift = (step * bits) % width;
        constexpr UInt16 bits_to_full_width = width - shift;
        constexpr UInt16 in_offset = iterations * ((step + 1) * bits / width);
        constexpr bool is_in_value_available = step < width - 1;

        T v;
        if constexpr (bits_to_full_width < bits)
        {
            constexpr T mask1 = (T{1} << bits_to_full_width) - T{1};
            constexpr T mask2 = (T{1} << (bits - bits_to_full_width)) - T{1};

            v = (pack >> shift) & mask1;
            if constexpr (is_in_value_available)
            {
                pack = in[index + in_offset];
                v |= (pack & mask2) << bits_to_full_width;
            }
        }
        else
        {
            if constexpr (shift == 0)
            {
                if constexpr (is_in_value_available)
                    pack = in[index + in_offset];
                else
                    pack = 0;
            }

            constexpr T mask = (T{1} << bits) - T{1};
            v = (pack >> shift) & mask;
        }

        out[index + out_offset] = v + base;
    }
}

template <UnsignedInteger T, UInt16 bits, UInt16 values>
void bitUnpack(const T * __restrict in, T * __restrict out, const T base)
{
    constexpr UInt16 width = get_width<T>();
    static_assert(bits <= width);

    constexpr UInt16 iterations = get_iterations<T, values>();

    for (UInt16 i = 0; i < iterations; ++i)
    {
        [&]<UInt16... step>(std::integer_sequence<UInt16, step ...>) __attribute__((always_inline))
        {
            T pack = 0;
            ((bitUnpackStep<T, bits, step, values>(in, out, base, i, pack)), ...);
        }(std::make_integer_sequence<UInt16, width>{});
    }
}
}

constexpr UInt16 DEFAULT_VALUES = 1024;

template <UInt16 values = DEFAULT_VALUES>
void bitPack(const UInt32 * __restrict in, UInt32 * __restrict out, const UInt16 bits, const UInt32 base)
{
    switch (bits)
    {
        case 0:  Details::bitPack<UInt32, 0,  values>(in, out, base); break;
        case 1:  Details::bitPack<UInt32, 1,  values>(in, out, base); break;
        case 2:  Details::bitPack<UInt32, 2,  values>(in, out, base); break;
        case 3:  Details::bitPack<UInt32, 3,  values>(in, out, base); break;
        case 4:  Details::bitPack<UInt32, 4,  values>(in, out, base); break;
        case 5:  Details::bitPack<UInt32, 5,  values>(in, out, base); break;
        case 6:  Details::bitPack<UInt32, 6,  values>(in, out, base); break;
        case 7:  Details::bitPack<UInt32, 7,  values>(in, out, base); break;
        case 8:  Details::bitPack<UInt32, 8,  values>(in, out, base); break;
        case 9:  Details::bitPack<UInt32, 9,  values>(in, out, base); break;
        case 10: Details::bitPack<UInt32, 10, values>(in, out, base); break;
        case 11: Details::bitPack<UInt32, 11, values>(in, out, base); break;
        case 12: Details::bitPack<UInt32, 12, values>(in, out, base); break;
        case 13: Details::bitPack<UInt32, 13, values>(in, out, base); break;
        case 14: Details::bitPack<UInt32, 14, values>(in, out, base); break;
        case 15: Details::bitPack<UInt32, 15, values>(in, out, base); break;
        case 16: Details::bitPack<UInt32, 16, values>(in, out, base); break;
        case 17: Details::bitPack<UInt32, 17, values>(in, out, base); break;
        case 18: Details::bitPack<UInt32, 18, values>(in, out, base); break;
        case 19: Details::bitPack<UInt32, 19, values>(in, out, base); break;
        case 20: Details::bitPack<UInt32, 20, values>(in, out, base); break;
        case 21: Details::bitPack<UInt32, 21, values>(in, out, base); break;
        case 22: Details::bitPack<UInt32, 22, values>(in, out, base); break;
        case 23: Details::bitPack<UInt32, 23, values>(in, out, base); break;
        case 24: Details::bitPack<UInt32, 24, values>(in, out, base); break;
        case 25: Details::bitPack<UInt32, 25, values>(in, out, base); break;
        case 26: Details::bitPack<UInt32, 26, values>(in, out, base); break;
        case 27: Details::bitPack<UInt32, 27, values>(in, out, base); break;
        case 28: Details::bitPack<UInt32, 28, values>(in, out, base); break;
        case 29: Details::bitPack<UInt32, 29, values>(in, out, base); break;
        case 30: Details::bitPack<UInt32, 30, values>(in, out, base); break;
        case 31: Details::bitPack<UInt32, 31, values>(in, out, base); break;
        case 32: Details::bitPack<UInt32, 32, values>(in, out, base); break;
        default: throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid bits for packing: {}", bits);
    }
}

template <UInt16 values = DEFAULT_VALUES>
void bitUnpack(const UInt32 * __restrict in, UInt32 * __restrict out, const UInt16 bits, const UInt32 base)
{
    switch (bits)
    {
        case 0:  Details::bitUnpack<UInt32, 0,  values>(in, out, base); break;
        case 1:  Details::bitUnpack<UInt32, 1,  values>(in, out, base); break;
        case 2:  Details::bitUnpack<UInt32, 2,  values>(in, out, base); break;
        case 3:  Details::bitUnpack<UInt32, 3,  values>(in, out, base); break;
        case 4:  Details::bitUnpack<UInt32, 4,  values>(in, out, base); break;
        case 5:  Details::bitUnpack<UInt32, 5,  values>(in, out, base); break;
        case 6:  Details::bitUnpack<UInt32, 6,  values>(in, out, base); break;
        case 7:  Details::bitUnpack<UInt32, 7,  values>(in, out, base); break;
        case 8:  Details::bitUnpack<UInt32, 8,  values>(in, out, base); break;
        case 9:  Details::bitUnpack<UInt32, 9,  values>(in, out, base); break;
        case 10: Details::bitUnpack<UInt32, 10, values>(in, out, base); break;
        case 11: Details::bitUnpack<UInt32, 11, values>(in, out, base); break;
        case 12: Details::bitUnpack<UInt32, 12, values>(in, out, base); break;
        case 13: Details::bitUnpack<UInt32, 13, values>(in, out, base); break;
        case 14: Details::bitUnpack<UInt32, 14, values>(in, out, base); break;
        case 15: Details::bitUnpack<UInt32, 15, values>(in, out, base); break;
        case 16: Details::bitUnpack<UInt32, 16, values>(in, out, base); break;
        case 17: Details::bitUnpack<UInt32, 17, values>(in, out, base); break;
        case 18: Details::bitUnpack<UInt32, 18, values>(in, out, base); break;
        case 19: Details::bitUnpack<UInt32, 19, values>(in, out, base); break;
        case 20: Details::bitUnpack<UInt32, 20, values>(in, out, base); break;
        case 21: Details::bitUnpack<UInt32, 21, values>(in, out, base); break;
        case 22: Details::bitUnpack<UInt32, 22, values>(in, out, base); break;
        case 23: Details::bitUnpack<UInt32, 23, values>(in, out, base); break;
        case 24: Details::bitUnpack<UInt32, 24, values>(in, out, base); break;
        case 25: Details::bitUnpack<UInt32, 25, values>(in, out, base); break;
        case 26: Details::bitUnpack<UInt32, 26, values>(in, out, base); break;
        case 27: Details::bitUnpack<UInt32, 27, values>(in, out, base); break;
        case 28: Details::bitUnpack<UInt32, 28, values>(in, out, base); break;
        case 29: Details::bitUnpack<UInt32, 29, values>(in, out, base); break;
        case 30: Details::bitUnpack<UInt32, 30, values>(in, out, base); break;
        case 31: Details::bitUnpack<UInt32, 31, values>(in, out, base); break;
        case 32: Details::bitUnpack<UInt32, 32, values>(in, out, base); break;
        default: throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid bits for unpacking: {}", bits);
    }
}

template <UInt16 values = DEFAULT_VALUES>
void bitPack(const UInt64 * __restrict in, UInt64 * __restrict out, const UInt16 bits, const UInt64 base)
{
    switch (bits)
    {
        case 0:  Details::bitPack<UInt64, 0,  values>(in, out, base); break;
        case 1:  Details::bitPack<UInt64, 1,  values>(in, out, base); break;
        case 2:  Details::bitPack<UInt64, 2,  values>(in, out, base); break;
        case 3:  Details::bitPack<UInt64, 3,  values>(in, out, base); break;
        case 4:  Details::bitPack<UInt64, 4,  values>(in, out, base); break;
        case 5:  Details::bitPack<UInt64, 5,  values>(in, out, base); break;
        case 6:  Details::bitPack<UInt64, 6,  values>(in, out, base); break;
        case 7:  Details::bitPack<UInt64, 7,  values>(in, out, base); break;
        case 8:  Details::bitPack<UInt64, 8,  values>(in, out, base); break;
        case 9:  Details::bitPack<UInt64, 9,  values>(in, out, base); break;
        case 10: Details::bitPack<UInt64, 10, values>(in, out, base); break;
        case 11: Details::bitPack<UInt64, 11, values>(in, out, base); break;
        case 12: Details::bitPack<UInt64, 12, values>(in, out, base); break;
        case 13: Details::bitPack<UInt64, 13, values>(in, out, base); break;
        case 14: Details::bitPack<UInt64, 14, values>(in, out, base); break;
        case 15: Details::bitPack<UInt64, 15, values>(in, out, base); break;
        case 16: Details::bitPack<UInt64, 16, values>(in, out, base); break;
        case 17: Details::bitPack<UInt64, 17, values>(in, out, base); break;
        case 18: Details::bitPack<UInt64, 18, values>(in, out, base); break;
        case 19: Details::bitPack<UInt64, 19, values>(in, out, base); break;
        case 20: Details::bitPack<UInt64, 20, values>(in, out, base); break;
        case 21: Details::bitPack<UInt64, 21, values>(in, out, base); break;
        case 22: Details::bitPack<UInt64, 22, values>(in, out, base); break;
        case 23: Details::bitPack<UInt64, 23, values>(in, out, base); break;
        case 24: Details::bitPack<UInt64, 24, values>(in, out, base); break;
        case 25: Details::bitPack<UInt64, 25, values>(in, out, base); break;
        case 26: Details::bitPack<UInt64, 26, values>(in, out, base); break;
        case 27: Details::bitPack<UInt64, 27, values>(in, out, base); break;
        case 28: Details::bitPack<UInt64, 28, values>(in, out, base); break;
        case 29: Details::bitPack<UInt64, 29, values>(in, out, base); break;
        case 30: Details::bitPack<UInt64, 30, values>(in, out, base); break;
        case 31: Details::bitPack<UInt64, 31, values>(in, out, base); break;
        case 32: Details::bitPack<UInt64, 32, values>(in, out, base); break;
        case 33: Details::bitPack<UInt64, 33, values>(in, out, base); break;
        case 34: Details::bitPack<UInt64, 34, values>(in, out, base); break;
        case 35: Details::bitPack<UInt64, 35, values>(in, out, base); break;
        case 36: Details::bitPack<UInt64, 36, values>(in, out, base); break;
        case 37: Details::bitPack<UInt64, 37, values>(in, out, base); break;
        case 38: Details::bitPack<UInt64, 38, values>(in, out, base); break;
        case 39: Details::bitPack<UInt64, 39, values>(in, out, base); break;
        case 40: Details::bitPack<UInt64, 40, values>(in, out, base); break;
        case 41: Details::bitPack<UInt64, 41, values>(in, out, base); break;
        case 42: Details::bitPack<UInt64, 42, values>(in, out, base); break;
        case 43: Details::bitPack<UInt64, 43, values>(in, out, base); break;
        case 44: Details::bitPack<UInt64, 44, values>(in, out, base); break;
        case 45: Details::bitPack<UInt64, 45, values>(in, out, base); break;
        case 46: Details::bitPack<UInt64, 46, values>(in, out, base); break;
        case 47: Details::bitPack<UInt64, 47, values>(in, out, base); break;
        case 48: Details::bitPack<UInt64, 48, values>(in, out, base); break;
        case 49: Details::bitPack<UInt64, 49, values>(in, out, base); break;
        case 50: Details::bitPack<UInt64, 50, values>(in, out, base); break;
        case 51: Details::bitPack<UInt64, 51, values>(in, out, base); break;
        case 52: Details::bitPack<UInt64, 52, values>(in, out, base); break;
        case 53: Details::bitPack<UInt64, 53, values>(in, out, base); break;
        case 54: Details::bitPack<UInt64, 54, values>(in, out, base); break;
        case 55: Details::bitPack<UInt64, 55, values>(in, out, base); break;
        case 56: Details::bitPack<UInt64, 56, values>(in, out, base); break;
        case 57: Details::bitPack<UInt64, 57, values>(in, out, base); break;
        case 58: Details::bitPack<UInt64, 58, values>(in, out, base); break;
        case 59: Details::bitPack<UInt64, 59, values>(in, out, base); break;
        case 60: Details::bitPack<UInt64, 60, values>(in, out, base); break;
        case 61: Details::bitPack<UInt64, 61, values>(in, out, base); break;
        case 62: Details::bitPack<UInt64, 62, values>(in, out, base); break;
        case 63: Details::bitPack<UInt64, 63, values>(in, out, base); break;
        case 64: Details::bitPack<UInt64, 64, values>(in, out, base); break;
        default: throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid bits for packing: {}", bits);
    }
}

template <UInt16 values = DEFAULT_VALUES>
void bitUnpack(const UInt64 * __restrict in, UInt64 * __restrict out, const UInt16 bits, const UInt64 base)
{
    switch (bits)
    {
        case 0:  Details::bitUnpack<UInt64, 0,  values>(in, out, base); break;
        case 1:  Details::bitUnpack<UInt64, 1,  values>(in, out, base); break;
        case 2:  Details::bitUnpack<UInt64, 2,  values>(in, out, base); break;
        case 3:  Details::bitUnpack<UInt64, 3,  values>(in, out, base); break;
        case 4:  Details::bitUnpack<UInt64, 4,  values>(in, out, base); break;
        case 5:  Details::bitUnpack<UInt64, 5,  values>(in, out, base); break;
        case 6:  Details::bitUnpack<UInt64, 6,  values>(in, out, base); break;
        case 7:  Details::bitUnpack<UInt64, 7,  values>(in, out, base); break;
        case 8:  Details::bitUnpack<UInt64, 8,  values>(in, out, base); break;
        case 9:  Details::bitUnpack<UInt64, 9,  values>(in, out, base); break;
        case 10: Details::bitUnpack<UInt64, 10, values>(in, out, base); break;
        case 11: Details::bitUnpack<UInt64, 11, values>(in, out, base); break;
        case 12: Details::bitUnpack<UInt64, 12, values>(in, out, base); break;
        case 13: Details::bitUnpack<UInt64, 13, values>(in, out, base); break;
        case 14: Details::bitUnpack<UInt64, 14, values>(in, out, base); break;
        case 15: Details::bitUnpack<UInt64, 15, values>(in, out, base); break;
        case 16: Details::bitUnpack<UInt64, 16, values>(in, out, base); break;
        case 17: Details::bitUnpack<UInt64, 17, values>(in, out, base); break;
        case 18: Details::bitUnpack<UInt64, 18, values>(in, out, base); break;
        case 19: Details::bitUnpack<UInt64, 19, values>(in, out, base); break;
        case 20: Details::bitUnpack<UInt64, 20, values>(in, out, base); break;
        case 21: Details::bitUnpack<UInt64, 21, values>(in, out, base); break;
        case 22: Details::bitUnpack<UInt64, 22, values>(in, out, base); break;
        case 23: Details::bitUnpack<UInt64, 23, values>(in, out, base); break;
        case 24: Details::bitUnpack<UInt64, 24, values>(in, out, base); break;
        case 25: Details::bitUnpack<UInt64, 25, values>(in, out, base); break;
        case 26: Details::bitUnpack<UInt64, 26, values>(in, out, base); break;
        case 27: Details::bitUnpack<UInt64, 27, values>(in, out, base); break;
        case 28: Details::bitUnpack<UInt64, 28, values>(in, out, base); break;
        case 29: Details::bitUnpack<UInt64, 29, values>(in, out, base); break;
        case 30: Details::bitUnpack<UInt64, 30, values>(in, out, base); break;
        case 31: Details::bitUnpack<UInt64, 31, values>(in, out, base); break;
        case 32: Details::bitUnpack<UInt64, 32, values>(in, out, base); break;
        case 33: Details::bitUnpack<UInt64, 33, values>(in, out, base); break;
        case 34: Details::bitUnpack<UInt64, 34, values>(in, out, base); break;
        case 35: Details::bitUnpack<UInt64, 35, values>(in, out, base); break;
        case 36: Details::bitUnpack<UInt64, 36, values>(in, out, base); break;
        case 37: Details::bitUnpack<UInt64, 37, values>(in, out, base); break;
        case 38: Details::bitUnpack<UInt64, 38, values>(in, out, base); break;
        case 39: Details::bitUnpack<UInt64, 39, values>(in, out, base); break;
        case 40: Details::bitUnpack<UInt64, 40, values>(in, out, base); break;
        case 41: Details::bitUnpack<UInt64, 41, values>(in, out, base); break;
        case 42: Details::bitUnpack<UInt64, 42, values>(in, out, base); break;
        case 43: Details::bitUnpack<UInt64, 43, values>(in, out, base); break;
        case 44: Details::bitUnpack<UInt64, 44, values>(in, out, base); break;
        case 45: Details::bitUnpack<UInt64, 45, values>(in, out, base); break;
        case 46: Details::bitUnpack<UInt64, 46, values>(in, out, base); break;
        case 47: Details::bitUnpack<UInt64, 47, values>(in, out, base); break;
        case 48: Details::bitUnpack<UInt64, 48, values>(in, out, base); break;
        case 49: Details::bitUnpack<UInt64, 49, values>(in, out, base); break;
        case 50: Details::bitUnpack<UInt64, 50, values>(in, out, base); break;
        case 51: Details::bitUnpack<UInt64, 51, values>(in, out, base); break;
        case 52: Details::bitUnpack<UInt64, 52, values>(in, out, base); break;
        case 53: Details::bitUnpack<UInt64, 53, values>(in, out, base); break;
        case 54: Details::bitUnpack<UInt64, 54, values>(in, out, base); break;
        case 55: Details::bitUnpack<UInt64, 55, values>(in, out, base); break;
        case 56: Details::bitUnpack<UInt64, 56, values>(in, out, base); break;
        case 57: Details::bitUnpack<UInt64, 57, values>(in, out, base); break;
        case 58: Details::bitUnpack<UInt64, 58, values>(in, out, base); break;
        case 59: Details::bitUnpack<UInt64, 59, values>(in, out, base); break;
        case 60: Details::bitUnpack<UInt64, 60, values>(in, out, base); break;
        case 61: Details::bitUnpack<UInt64, 61, values>(in, out, base); break;
        case 62: Details::bitUnpack<UInt64, 62, values>(in, out, base); break;
        case 63: Details::bitUnpack<UInt64, 63, values>(in, out, base); break;
        case 64: Details::bitUnpack<UInt64, 64, values>(in, out, base); break;
        default: throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid bits for unpacking: {}", bits);
    }
}

template <UInt16 values = DEFAULT_VALUES>
ALWAYS_INLINE UInt16 calculateBitpackedBytes(const UInt16 bits)
{
    return bits * values / 8;
}
}
}
