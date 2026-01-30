#pragma once

/**
 * This file implements a FFOR algorithm inspired by the FastLanes project:
 * https://github.com/cwida/FastLanes
 *
 * Original work by Azim Afroozeh and the CWI Database Architectures Group, licensed under the MIT License.
 *
 * This implementation uses template-based C++ generator approach and adapted to the CH codebase and style.
 *
 * Algorithm:
 *   1. Subtract base (minimum value) from all integers to reduce magnitude.
 *   2. Pack resulting values using only the required number of bits (e.g. 7 bits for values [0, 127]).
 *
 * Important:
 *   * Original FastLanes implementation processes 1024 values only. Avoid using values other than 1024 at all costs as code likely won't auto-vectorise!
 *   * Input and output arrays must be aligned to 64-byte boundary for optimal access performance.
 *   * This implementation is generic to number of values, but changing this affects compatibility with FastLanes reference implementation.
*/

#include <Common/Exception.h>

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

/**
 * Returns bit-width of unsigned integer type T.
 */
template <UnsignedInteger T>
consteval UInt8 get_width()
{
    return sizeof(T) * 8;
}

/**
 * Calculates number of iterations (chunks) needed to process 'values' input values in width-sized batches.
 */
template <UnsignedInteger T, UInt16 values>
consteval UInt16 get_iterations()
{
    constexpr UInt8 width = get_width<T>();
    static_assert(values % width == 0, "values must be divisible by bit-width of type T");
    return values / width;
}

/**
 * Packs one input value. Produces compile-time step within unrolled type-width-sized chunk.
 *
 * Parameter 'index': Number of current iteration (chunk index).
 * Parameter 'agg': Accumulator for partial output word, flushed when crossing word size boundary or at end of chunk.
 * Template parameter 'step': Current step within chunk, [0, width].
 */
template <UnsignedInteger T, UInt8 bits, UInt8 step, UInt16 values>
ALWAYS_INLINE void bitPackStep(const T * __restrict in, T * __restrict out, const T base, const UInt16 index, T & agg)
{
    constexpr UInt8 width = get_width<T>();
    static_assert(bits <= width);
    static_assert(step <= width);

    constexpr UInt16 iterations = get_iterations<T, values>();

    if constexpr (bits > 0 && bits < width)
    {
        constexpr UInt8 shift = (step * bits) % width;
        constexpr T mask = (T{1} << bits) - 1;

        if constexpr (step > 0 && shift < bits) // Word boundary crossed
        {
            // Flush accumulator to output
            constexpr UInt16 out_offset = iterations * (((step - 1) * bits) / width);
            out[out_offset + index] = agg;

            if constexpr (step >= width) // End of iteration chunk
                return;

            // Start new accumulator with spill-over bits from previous input value
            if constexpr (shift > 0)
            {
                // Carry high bits from previous value
                constexpr UInt16 in_prev_offset = iterations * (step - 1);
                constexpr UInt8 prev_shift = bits - shift;
                T v = in[in_prev_offset + index] - base;
                v &= mask;
                v >>= prev_shift;
                agg = v;
            }
            else
                agg = 0; // Aligned, no spill-over
        }
        else if constexpr (step == 0)
            agg = 0;

        if constexpr (step < width)
        {
            // Take current value bits and add to accumulator
            constexpr UInt16 in_offset = iterations * step;
            T v = in[in_offset + index] - base;
            v &= mask;
            v <<= shift;
            agg |= v;
        }
    }
    else if constexpr (bits == width && step < width)
    {
        // Direct copy for full-width values
        constexpr UInt16 offset = iterations * step;
        out[offset + index] = in[offset + index] - base;
    }
}

/**
 * Packs input array into bit-packed output using strided iteration pattern.
 *
 * Iteration pattern:
 *   Processes values with stride of 'iterations' (values / width).
 *   For each chunk i in [0, iterations), processes values at positions:
 *     [i, i + iterations, i + 2*iterations, ..., i + (width-1)*iterations]
 *
 *   Example (UInt64, 1024 values):
 *     iterations = 16
 *     Chunk 0: in[0], in[16], in[32], ..., in[1008]
 *     Chunk 1: in[1], in[17], in[33], ..., in[1009]
 *
 * Chunk processing uses fold expression to unroll all steps within chunk at compile time.
 * Compiler sees all bit positions at compile time and optimizes accordingly.
 */
template <UnsignedInteger T, UInt8 bits, UInt16 values>
void bitPack(const T * __restrict in, T * __restrict out, const T base)
{
    if constexpr (bits == 0)
        return;

    constexpr UInt8 width = get_width<T>();
    static_assert(bits <= width);

    constexpr UInt16 iterations = get_iterations<T, values>();

    for (UInt16 i = 0; i < iterations; ++i)
    {
        [&]<UInt8... step>(std::integer_sequence<UInt8, step ...>) ALWAYS_INLINE
        {
            T state = 0;
            ((bitPackStep<T, bits, step, values>(in, out, base, i, state)), ...);
        }(std::make_integer_sequence<UInt8, width + 1>{});
    }
}

/**
 * Builds compile-time dispatch table for packing.
 * Each entry stores a function pointer to bitPack instantiation for specific bits.
 */
template <UnsignedInteger T, UInt16 values>
consteval auto makeBitPackDispatchTable()
{
    constexpr UInt8 size = get_width<T>() + 1;
    return [&]<UInt8... bits>(std::integer_sequence<UInt8, bits...>)
    {
        using fn = void(*)(const T * __restrict, T * __restrict, T);
        return std::array<fn, size>
        {
            +[](const T * __restrict in, T * __restrict out, const T base)
            {
                bitPack<T, bits, values>(in, out, base);
            }...
        };
    }(std::make_integer_sequence<UInt8, size>{});
}

/**
 * Unpacks one output value. Produces compile-time step within unrolled type-width-sized chunk.
 *
 * Parameter 'index': Number of current iteration (chunk index).
 * Parameter 'pack': Current input word being processed.
 * Template parameter 'step': Current step within chunk, [0, width).
 */
template <UnsignedInteger T, UInt8 bits, UInt8 step, UInt16 values>
ALWAYS_INLINE void bitUnpackStep(const T * __restrict in, T * __restrict out, const T base, const UInt16 index, T & pack)
{
    constexpr UInt8 width = get_width<T>();
    static_assert(bits <= width);
    static_assert(step < width);

    constexpr UInt16 iterations = get_iterations<T, values>();
    constexpr UInt16 out_offset = iterations * step;

    if constexpr (bits == 0) // Zero bits: all values equal to base
        out[index + out_offset] = base;
    else if constexpr (bits == width) // Full-width: direct copy
        out[index + out_offset] = in[index + out_offset] + base;
    else
    {
        constexpr UInt8 shift = (step * bits) % width;
        constexpr UInt8 bits_to_full_width = width - shift;
        constexpr UInt16 in_offset = iterations * ((step + 1) * bits / width);
        constexpr bool is_in_value_available = step < width - 1;

        T v;
        if constexpr (bits_to_full_width < bits) // Value spans word boundary
        {
            constexpr T mask1 = (T{1} << bits_to_full_width) - T{1}; // Mask for lower part of value from current word
            constexpr T mask2 = (T{1} << (bits - bits_to_full_width)) - T{1}; // Mask for upper part of value from next word

            v = (pack >> shift) & mask1;
            if constexpr (is_in_value_available)
            {
                // Get next input word
                pack = in[index + in_offset];
                v |= (pack & mask2) << bits_to_full_width;
            }
        }
        else // Value fits within current word
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

/**
 * Unpacks bit-packed input into output array using strided iteration pattern.
 *
 * Iteration pattern:
 *   Mirrors bitPack.
 *   For each chunk i in [0, iterations), reconstructs values at positions:
 *     [i, i + iterations, i + 2*iterations, ...]
 *
 *   Example (UInt64, 1024 values):
 *     iterations = 16
 *     Chunk 0 reconstructs: out[0], out[16], out[32], ..., out[1008]
 *     Chunk 1 reconstructs: out[1], out[17], out[33], ..., out[1009]
 *
 * Chunk processing uses fold expression to unroll all steps within chunk at compile time.
 * Compiler sees all bit positions at compile time and optimizes accordingly.
 */
template <UnsignedInteger T, UInt8 bits, UInt16 values>
void bitUnpack(const T * __restrict in, T * __restrict out, const T base)
{
    constexpr UInt8 width = get_width<T>();
    static_assert(bits <= width);

    constexpr UInt16 iterations = get_iterations<T, values>();

    for (UInt16 i = 0; i < iterations; ++i)
    {
        [&]<UInt8... step>(std::integer_sequence<UInt8, step ...>) ALWAYS_INLINE
        {
            T pack = 0;
            ((bitUnpackStep<T, bits, step, values>(in, out, base, i, pack)), ...);
        }(std::make_integer_sequence<UInt8, width>{});
    }
}

/**
 * Builds compile-time dispatch table for unpacking.
 * Each entry stores a function pointer to bitUnpack instantiation for specific bits.
 */
template <UnsignedInteger T, UInt16 values>
consteval auto makeBitUnpackDispatchTable()
{
    constexpr UInt8 size = get_width<T>() + 1;
    return [&]<UInt8... bits>(std::integer_sequence<UInt8, bits...>)
    {
        using fn = void(*)(const T * __restrict, T * __restrict, T);
        return std::array<fn, size>
        {
            +[](const T * __restrict in, T * __restrict out, const T base)
            {
                bitUnpack<T, bits, values>(in, out, base);
            }...
        };
    }(std::make_integer_sequence<UInt8, size>{});
}
}

/**
 * Default number of values processed by FFOR.
 * The FastLanes implementation expects 1024 values.
 * Changing this affects compatibility, and it likely won't auto-vectorise. Avoid at all cost!
 * More context: https://www.vldb.org/pvldb/vol16/p2132-afroozeh.pdf
 */
constexpr UInt16 DEFAULT_VALUES = 1024;

/**
 * Packs UInt16 input values.
 * \note Input and output arrays must be 'values' elements long and aligned to 64-byte boundary for optimal access performance.
 */
template <UInt16 values = DEFAULT_VALUES>
void bitPack(const UInt16 * __restrict in, UInt16 * __restrict out, const UInt8 bits, const UInt16 base)
{
    static constexpr auto table = Details::makeBitPackDispatchTable<UInt16, values>();
    if (unlikely(bits >= table.size()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid bits for UInt16 packing: {}", static_cast<UInt32>(bits));
    table[bits](in, out, base);
}

/**
 * Unpacks bit-packed UInt16 input values.
 * \note Input and output arrays must be 'values' elements long and aligned to 64-byte boundary for optimal access performance.
 */
template <UInt16 values = DEFAULT_VALUES>
void bitUnpack(const UInt16 * __restrict in, UInt16 * __restrict out, const UInt8 bits, const UInt16 base)
{
    static constexpr auto table = Details::makeBitUnpackDispatchTable<UInt16, values>();
    if (unlikely(bits >= table.size()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid bits for UInt16 unpacking: {}", static_cast<UInt32>(bits));
    table[bits](in, out, base);
}

/**
 * Packs UInt32 input values.
 * \note Input and output arrays must be 'values' elements long and aligned to 64-byte boundary for optimal access performance.
 */
template <UInt16 values = DEFAULT_VALUES>
void bitPack(const UInt32 * __restrict in, UInt32 * __restrict out, const UInt8 bits, const UInt32 base)
{
    static constexpr auto table = Details::makeBitPackDispatchTable<UInt32, values>();
    if (unlikely(bits >= table.size()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid bits for UInt32 packing: {}", static_cast<UInt32>(bits));
    table[bits](in, out, base);
}

/**
 * Unpacks bit-packed UInt32 input values.
 * \note Input and output arrays must be 'values' elements long and aligned to 64-byte boundary for optimal access performance.
 */
template <UInt16 values = DEFAULT_VALUES>
void bitUnpack(const UInt32 * __restrict in, UInt32 * __restrict out, const UInt8 bits, const UInt32 base)
{
    static constexpr auto table = Details::makeBitUnpackDispatchTable<UInt32, values>();
    if (unlikely(bits >= table.size()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid bits for UInt32 unpacking: {}", static_cast<UInt32>(bits));
    table[bits](in, out, base);
}

/**
 * Packs UInt64 input values.
 * \note Input and output arrays must be 'values' elements long and aligned to 64-byte boundary for optimal access performance.
 */
template <UInt16 values = DEFAULT_VALUES>
void bitPack(const UInt64 * __restrict in, UInt64 * __restrict out, const UInt8 bits, const UInt64 base)
{
    static constexpr auto table = Details::makeBitPackDispatchTable<UInt64, values>();
    if (unlikely(bits >= table.size()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid bits for UInt64 packing: {}", static_cast<UInt32>(bits));
    table[bits](in, out, base);
}

/**
 * Unpacks bit-packed UInt64 input values.
 * \note Input and output arrays must be 'values' elements long and aligned to 64-byte boundary for optimal access performance.
 */
template <UInt16 values = DEFAULT_VALUES>
void bitUnpack(const UInt64 * __restrict in, UInt64 * __restrict out, const UInt8 bits, const UInt64 base)
{
    static constexpr auto table = Details::makeBitUnpackDispatchTable<UInt64, values>();
    if (unlikely(bits >= table.size()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid bits for UInt64 unpacking: {}", static_cast<UInt32>(bits));
    table[bits](in, out, base);
}

/**
 * Calculates number of bytes required to store input values bit-packed with 'bits' bits per value.
 */
template <UInt16 values = DEFAULT_VALUES>
ALWAYS_INLINE UInt16 calculateBitpackedBytes(const UInt8 bits)
{
    return bits * values / 8;
}

// Explicit instantiations to reduce compilation times and binary size

extern template void bitPack<DEFAULT_VALUES>(const UInt16 * __restrict, UInt16 * __restrict, UInt8, UInt16);
extern template void bitPack<DEFAULT_VALUES>(const UInt32 * __restrict, UInt32 * __restrict, UInt8, UInt32);
extern template void bitPack<DEFAULT_VALUES>(const UInt64 * __restrict, UInt64 * __restrict, UInt8, UInt64);

extern template void bitUnpack<DEFAULT_VALUES>(const UInt16 * __restrict, UInt16 * __restrict, UInt8, UInt16);
extern template void bitUnpack<DEFAULT_VALUES>(const UInt32 * __restrict, UInt32 * __restrict, UInt8, UInt32);
extern template void bitUnpack<DEFAULT_VALUES>(const UInt64 * __restrict, UInt64 * __restrict, UInt8, UInt64);

}
}
