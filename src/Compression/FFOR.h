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
    static_assert(values % width == 0, "values must be divisible by bit-width of type T");
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

template <UnsignedInteger T, UInt16 values>
consteval auto makeBitPackDispatchTable()
{
    constexpr UInt16 size = get_width<T>() + 1;
    return [&]<UInt16... bits>(std::integer_sequence<UInt16, bits...>)
    {
        using fn = void(*)(const T * __restrict, T * __restrict, T);
        return std::array<fn, size>
        {
            +[](const T * __restrict in, T * __restrict out, const T base)
            {
                bitPack<T, bits, values>(in, out, base);
            }...
        };
    }(std::make_integer_sequence<UInt16, size>{});
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
        [&]<UInt16... step>(std::integer_sequence<UInt16, step ...>) ALWAYS_INLINE
        {
            T pack = 0;
            ((bitUnpackStep<T, bits, step, values>(in, out, base, i, pack)), ...);
        }(std::make_integer_sequence<UInt16, width>{});
    }
}

template <UnsignedInteger T, UInt16 values>
consteval auto makeBitUnpackDispatchTable()
{
    constexpr UInt16 size = get_width<T>() + 1;
    return [&]<UInt16... bits>(std::integer_sequence<UInt16, bits...>)
    {
        using fn = void(*)(const T * __restrict, T * __restrict, T);
        return std::array<fn, size>
        {
            +[](const T * __restrict in, T * __restrict out, const T base)
            {
                bitUnpack<T, bits, values>(in, out, base);
            }...
        };
    }(std::make_integer_sequence<UInt16, size>{});
}
}

constexpr UInt16 DEFAULT_VALUES = 1024;

template <UInt16 values = DEFAULT_VALUES>
void bitPack(const UInt16 * __restrict in, UInt16 * __restrict out, const UInt16 bits, const UInt16 base)
{
    static constexpr auto table = Details::makeBitPackDispatchTable<UInt16, values>();
    if (unlikely(bits >= table.size()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid bits for UInt16 packing: {}", bits);
    table[bits](in, out, base);
}

template <UInt16 values = DEFAULT_VALUES>
void bitUnpack(const UInt16 * __restrict in, UInt16 * __restrict out, const UInt16 bits, const UInt16 base)
{
    static constexpr auto table = Details::makeBitUnpackDispatchTable<UInt16, values>();
    if (unlikely(bits >= table.size()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid bits for UInt16 unpacking: {}", bits);
    table[bits](in, out, base);
}

template <UInt16 values = DEFAULT_VALUES>
void bitPack(const UInt32 * __restrict in, UInt32 * __restrict out, const UInt16 bits, const UInt32 base)
{
    static constexpr auto table = Details::makeBitPackDispatchTable<UInt32, values>();
    if (unlikely(bits >= table.size()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid bits for UInt32 packing: {}", bits);
    table[bits](in, out, base);
}

template <UInt16 values = DEFAULT_VALUES>
void bitUnpack(const UInt32 * __restrict in, UInt32 * __restrict out, const UInt16 bits, const UInt32 base)
{
    static constexpr auto table = Details::makeBitUnpackDispatchTable<UInt32, values>();
    if (unlikely(bits >= table.size()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid bits for UInt32 unpacking: {}", bits);
    table[bits](in, out, base);
}

template <UInt16 values = DEFAULT_VALUES>
void bitPack(const UInt64 * __restrict in, UInt64 * __restrict out, const UInt16 bits, const UInt64 base)
{
    static constexpr auto table = Details::makeBitPackDispatchTable<UInt64, values>();
    if (unlikely(bits >= table.size()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid bits for UInt64 packing: {}", bits);
    table[bits](in, out, base);
}

template <UInt16 values = DEFAULT_VALUES>
void bitUnpack(const UInt64 * __restrict in, UInt64 * __restrict out, const UInt16 bits, const UInt64 base)
{
    static constexpr auto table = Details::makeBitUnpackDispatchTable<UInt64, values>();
    if (unlikely(bits >= table.size()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid bits for UInt64 unpacking: {}", bits);
    table[bits](in, out, base);
}

template <UInt16 values = DEFAULT_VALUES>
ALWAYS_INLINE UInt16 calculateBitpackedBytes(const UInt16 bits)
{
    return bits * values / 8;
}
}
}
