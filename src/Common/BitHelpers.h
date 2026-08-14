#pragma once

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <base/defines.h>


/** For zero argument, result is zero.
  * For arguments with most significand bit set, result is n.
  * For other arguments, returns value, rounded up to power of two.
  */
inline size_t roundUpToPowerOfTwoOrZero(size_t n)
{
    // if MSB is set, return n, to avoid return zero
    if (unlikely(n >= 0x8000000000000000ULL))
        return n;
    else if (n <= 1)
        return n;

    /// `clzll`, not `clzl`: the argument is a 64-bit `size_t`, as the hardcoded 64 and the
    /// `0x8000000000000000ULL` above both assume, but `unsigned long` - what `clzl` takes - is
    /// only 64 bits wide on LP64 platforms. On an LLP64 one it is 32, and the argument would be
    /// truncated. `clzll` is at least 64 bits everywhere.
    /// The `NOLINT` is for the same reason as the ones on `getLeadingZeroBitsUnsafe` below: a
    /// builtin is implicitly declared at its first use in a translation unit, so this line becomes
    /// the origin of `__builtin_clzll` for every unit that includes this header first. `clang-tidy`
    /// then attributes `readability-redundant-casting` on `static_cast<int>(__builtin_clzll(...))`
    /// in `contrib/arrow/cpp/src/arrow/util/bit_util.h` here, and reports it, because a note
    /// pointing into our code makes the diagnostic count as user code rather than `contrib`.
    return size_t(1) << (64 - __builtin_clzll(n - 1)); // NOLINT(readability-redundant-casting)
}


template <typename T>
inline uint32_t getLeadingZeroBitsUnsafe(T x)
{
    chassert(x != 0);

    if constexpr (sizeof(T) <= sizeof(unsigned int))
    {
        return __builtin_clz(x); // NOLINT(readability-redundant-casting)
    }
    else if constexpr (sizeof(T) <= sizeof(unsigned long int)) /// NOLINT
    {
        return __builtin_clzl(x);
    }
    else
    {
        return __builtin_clzll(x); // NOLINT(readability-redundant-casting)
    }
}


template <typename T>
inline size_t getLeadingZeroBits(T x)
{
    if (!x)
        return sizeof(x) * 8;

    return getLeadingZeroBitsUnsafe(x);
}

/** Returns log2 of number, rounded down.
  * Compiles to single 'bsr' instruction on x86.
  * For zero argument, result is unspecified.
  */
template <typename T>
inline uint32_t bitScanReverse(T x)
{
    return (std::max<size_t>(sizeof(T), sizeof(unsigned int))) * 8 - 1 - getLeadingZeroBitsUnsafe(x);
}

// Unsafe since __builtin_ctz()-family explicitly state that result is undefined on x == 0
template <typename T>
inline size_t getTrailingZeroBitsUnsafe(T x)
{
    chassert(x != 0);

    if constexpr (sizeof(T) <= sizeof(unsigned int))
    {
        return __builtin_ctz(x);
    }
    else if constexpr (sizeof(T) <= sizeof(unsigned long int)) /// NOLINT
    {
        return __builtin_ctzl(x); // NOLINT(readability-redundant-casting) clang-tidy cross-references this with arrow's bit_util.h
    }
    else
    {
        return __builtin_ctzll(x); // NOLINT(readability-redundant-casting)
    }
}

template <typename T>
inline size_t getTrailingZeroBits(T x)
{
    if (!x)
        return sizeof(x) * 8;

    return getTrailingZeroBitsUnsafe(x);
}

/** Returns a mask that has '1' for `bits` LSB set:
  * maskLowBits<UInt8>(3) => 00000111
  * maskLowBits<Int8>(3) => 00000111
  */
template <typename T>
inline T maskLowBits(unsigned char bits)
{
    using UnsignedT = std::make_unsigned_t<T>;
    if (bits == 0)
    {
        return 0;
    }

    UnsignedT result = static_cast<UnsignedT>(~UnsignedT{0});
    if (bits < sizeof(T) * 8)
    {
        result = static_cast<UnsignedT>(result >> (sizeof(UnsignedT) * 8 - bits));
    }

    return static_cast<T>(result);
}

template <std::integral T>
constexpr bool isPowerOf2(T number)
{
    return number > 0 && (number & (number - 1)) == 0;
}

/** Writes a value into a bit-packed array at the given bit offset.
  * The array must be overallocated by one element.
  * The bit range must be pre-filled with zeros.
  */
inline void writeBitsPacked64(uint64_t * dest, size_t bit_offset, uint64_t value)
{
    size_t mod = bit_offset % 64;
    dest[bit_offset / 64] |= value << mod;
    if (mod)
        dest[bit_offset / 64 + 1] |= value >> (64 - mod);
}

/** Reads a range of bits from a bit-packed array.
  * The array must be overallocated by one element.
  */
inline uint64_t readBitsPacked64(const uint64_t * src, size_t bit_offset, size_t num_bits)
{
    size_t mod = bit_offset % 64;
    uint64_t value = src[bit_offset / 64] >> mod;
    if (mod)
        value |= src[bit_offset / 64 + 1] << (64 - mod);
    return value & maskLowBits<uint64_t>(static_cast<unsigned char>(num_bits));
}
