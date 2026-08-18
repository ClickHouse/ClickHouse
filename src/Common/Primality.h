#pragma once

#include <array>
#include <limits>
#include <numeric>

#include <base/defines.h>
#include <base/extended_types.h>
#include <base/types.h>
#include <Common/Concepts.h>
#include <Common/HashTable/Hash.h>

#include <boost/multiprecision/cpp_int.hpp>
#include <boost/multiprecision/integer.hpp>
#include <boost/multiprecision/miller_rabin.hpp>

#include <pcg_random.hpp>


namespace DB::Primality
{

template <typename T>
concept is_native_uint = is_any_of<T, UInt8, UInt16, UInt32, UInt64>;

template <typename T>
concept is_big_uint = is_any_of<T, UInt128, UInt256>;

namespace detail
{

/// Precompute the prime bitmap covering the entire UInt16 range so small-value primality
/// becomes a single bit lookup at runtime.
inline constexpr size_t prime_bitmap_size = size_t{std::numeric_limits<UInt16>::max()} + 1; /// 65536

using PrimeBitmap = std::array<UInt64, prime_bitmap_size / 64>;

/// Mark prime positions with 1. Takes up 8 KiB in `.rodata` but makes primality checks for
/// small numbers (< prime_bitmap_size) extremely fast (single bit test).
inline constexpr PrimeBitmap prime_bitmap = []
{
    /// Mark every value >= 2 as a candidate prime.
    std::array<bool, prime_bitmap_size> sieve{};
    for (size_t i = 2; i < prime_bitmap_size; ++i)
        sieve[i] = true;

    /// Sieve of Eratosthenes: for each surviving prime i, cross out its multiples.
    for (size_t i = 2; i * i < prime_bitmap_size; ++i)
        if (sieve[i])
            for (size_t j = i * i; j < prime_bitmap_size; j += i)
                sieve[j] = false;

    /// Pack the boolean sieve into a UInt64-word bitmap (8x smaller, fits in L1d).
    PrimeBitmap bitmap{};
    for (size_t i = 0; i < prime_bitmap_size; ++i)
        if (sieve[i])
            bitmap[i >> 6] |= UInt64(1) << (i & 63);
    return bitmap;
}();

inline bool testBit(const PrimeBitmap & bitmap, UInt32 num)
{
    return (bitmap[num >> 6] >> (num & 63)) & UInt64(1);
}

/// Miller-Rabin primality test using a fixed witness set.
/// https://en.wikipedia.org/wiki/Miller%E2%80%93Rabin_primality_test

/// Sinclair-7 Miller-Rabin witness set - using these witnesses with the algorithm correctly
/// classifies every odd number in the UInt64 range. See https://miller-rabin.appspot.com/
inline constexpr UInt64 miller_rabin_witnesses_u64[] = {
    2,
    325,
    9375,
    28178,
    450775,
    9780504,
    1795265022,
};

/// Jaeschke 1993 3-witness set, proven correct for all n < 4,759,123,141 (> 2^32). See https://miller-rabin.appspot.com/
inline constexpr UInt32 miller_rabin_witnesses_u32[] = {2, 7, 61};

/// `(lhs * rhs) % modulus` without overflow.
inline UInt64 mulModU64(UInt64 lhs, UInt64 rhs, UInt64 modulus)
{
    return static_cast<UInt64>((static_cast<__uint128_t>(lhs) * rhs) % modulus);
}

inline bool millerRabinRoundU64(UInt64 num, UInt64 odd_factor, unsigned power_of_two_count, UInt64 witness)
{
    witness %= num;
    if (witness == 0)
        return true;

    /// witness^odd_factor mod num
    UInt64 witness_power = boost::multiprecision::powm(witness, odd_factor, num);
    if (witness_power == 1 || witness_power == num - 1)
        return true;

    for (unsigned i = 0; i + 1 < power_of_two_count; ++i)
    {
        /// witness_power = witness_power^2 mod num
        witness_power = mulModU64(witness_power, witness_power, num);
        if (witness_power == num - 1)
            return true;
    }
    return false;
}


template <size_t witness_count, typename Witness>
inline UInt8 millerRabinTestU64(UInt64 num, const Witness (&witnesses)[witness_count])
{
    /// Both invariants are guaranteed by the `isPrime()` function.
    chassert(num >= 65536);
    chassert((num & 1) != 0);

    /// Decompose `num - 1 = odd_factor * 2^power_of_two_count`.
    unsigned power_of_two_count = __builtin_ctzll(num - 1);
    UInt64 odd_factor = (num - 1) >> power_of_two_count;

    for (auto witness : witnesses)
    {
        if (!millerRabinRoundU64(num, odd_factor, power_of_two_count, static_cast<UInt64>(witness)))
            return 0;
    }
    return 1;
}

template <typename T>
auto toBoostInteger(const T & num)
{
    static_assert(std::is_same_v<T, UInt128> || std::is_same_v<T, UInt256>);
    using BoostUInt = std::conditional_t<std::is_same_v<T, UInt128>, boost::multiprecision::uint128_t, boost::multiprecision::uint256_t>;
    BoostUInt result = 0;
    for (size_t i = 0; i < std::size(num.items); ++i)
        result |= BoostUInt(num.items[i]) << (64 * i);
    return result;
}

inline constexpr UInt64 first_15_primes_product = UInt64{2} * 3 * 5 * 7 * 11 * 13 * 17 * 19 * 23 * 29 * 31 * 37 * 41 * 43 * 47;

/// Compute `num mod modulus` for a wide integer, returning a UInt64.
/// Horner's method is specialized for the case when the divisor fits in UInt64, which is much
/// faster than `wide::integer::operator%` - a general-purpose algorithm for arbitrary-width integers.
template <typename T>
UInt64 hornerMod(const T & num, UInt64 modulus)
{
    static_assert(std::is_same_v<T, UInt128> || std::is_same_v<T, UInt256>);
    UInt64 remainder = 0;
    for (size_t i = std::size(num.items); i-- > 0;)
    {
        __uint128_t accumulator = (static_cast<__uint128_t>(remainder) << 64) | num.items[i];
        remainder = static_cast<UInt64>(accumulator % modulus);
    }
    return remainder;
}


/// We want to know whether `num` is divisible by any prime in `{2, 3, 5, …, 47}` - the naive
/// way is to compute `num % p` for each of the 15 primes and OR the zero-checks together. However,
/// this would be slow.
///
/// The following expression is mathematically equivalent and is much faster to compute.
/// The constant `first_15_primes_product` is the product of the first 15 primes (P = 2·3·5·…·47),
/// `gcd(num, P)` computes the product of the primes that `num` and `P` have in common.
/// So `gcd(num, P) > 1` is true if and only if `num` shares at least one prime factor with `P`,
/// which implies that `num` is divisible by at least one of the primes in `{2, 3, …, 47}` that makes up `P`.
template <typename T>
inline bool isDivisibleByFirst15Primes(const T & num)
{
    if constexpr (std::is_same_v<T, UInt128> || std::is_same_v<T, UInt256>)
        return std::gcd(hornerMod(num, first_15_primes_product), first_15_primes_product) > 1;
    else
        return std::gcd(static_cast<UInt64>(num), first_15_primes_product) > 1;
}

}

template <is_native_uint T>
inline UInt8 isPrime(T num)
{
    if constexpr (std::is_same_v<T, UInt8> || std::is_same_v<T, UInt16>) /// guaranteed to be in [0..65535], so covered by the bitmap
        return detail::testBit(detail::prime_bitmap, num);
    else
    {
        if (num < detail::prime_bitmap_size) /// Use the fast bitmap lookup if possible
            return detail::testBit(detail::prime_bitmap, static_cast<UInt16>(num));

        /// This should cover many composite candidates cheaply before the more expensive Miller-Rabin rounds.
        if (detail::isDivisibleByFirst15Primes(num))
            return 0;

        /// More expensive Miller-Rabin check
        if constexpr (std::is_same_v<T, UInt32>)
            return detail::millerRabinTestU64(num, detail::miller_rabin_witnesses_u32);
        else if (num <= std::numeric_limits<UInt32>::max())
            return detail::millerRabinTestU64(num, detail::miller_rabin_witnesses_u32);
        else
            return detail::millerRabinTestU64(num, detail::miller_rabin_witnesses_u64);
    }
}

/// `rounds` is unused for narrow types - the result is exact regardless.
template <is_native_uint T>
inline UInt8 isProbablePrime(T num, unsigned /*rounds*/)
{
    return isPrime(num);
}

template <is_big_uint T>
UInt8 isProbablePrime(const T & num, unsigned rounds)
{
    /// If the wide value fits in UInt64, use the deterministic UInt64 path.
    if (num <= std::numeric_limits<UInt64>::max())
        return isPrime<UInt64>(static_cast<UInt64>(num.items[0]));

    /// Reject even numbers cheaply before the expensive upcoming operations.
    if ((num.items[0] & 1) == 0)
        return 0;

    /// This should cover many composite candidates cheaply before the more expensive Miller-Rabin rounds.
    if (detail::isDivisibleByFirst15Primes(num))
        return 0;

    auto boost_num = detail::toBoostInteger(num);

    /// Seed deterministically so the same (num, rounds) pair always produces the same result.
    pcg64 rng(DefaultHash<T>{}(num));

    return boost::multiprecision::miller_rabin_test(boost_num, rounds, rng);
}

}
