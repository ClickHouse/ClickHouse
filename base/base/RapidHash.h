#pragma once

/** A single-header, dependency-free version of `rapidhash`.
  *
  * Origin: https://github.com/Nicoshev/rapidhash (Nicolas De Carli, 2024), BSD-2-Clause,
  * taken from the SMHasher copy in `hashes/rapidhash.cpp`. Only the core algorithm is kept here;
  * the SMHasher framework macros (`Platform.h`, `GET_U64`, `MathMult`, ...) are dropped.
  *
  * This is the plain (neither "protected" nor "unrolled") variant producing a 64-bit value.
  *
  * The algorithm in short:
  *   1. Seed initialization: `seed ^= mix(seed ^ secret[0], secret[1]) ^ size`.
  *   2. Three size classes: `size <= 16` reads the bytes directly, `17..48` finishes with `mix`,
  *      and everything above consumes 48 bytes per iteration.
  *   3. Finalization: `mum` followed by `mix`, folding both the size and the secret constants in.
  *
  * The two primitives are `mum` - a 64x64 -> 128 bit multiplication keeping both halves -
  * and `mix` - a `mum` whose halves are then xored together.
  */

#include <cstddef>
#include <cstdint>

#include <base/unaligned.h>

namespace rapid
{

/// 64x64 -> 128 bit multiplication; `lo` and `hi` receive the low and the high half of the product.
inline void mul128(uint64_t & lo, uint64_t & hi, uint64_t a, uint64_t b)
{
    unsigned __int128 product = static_cast<unsigned __int128>(a) * b;
    lo = static_cast<uint64_t>(product);
    hi = static_cast<uint64_t>(product >> 64);
}

/// MUM: replace `a` and `b` with the low and the high half of their 128-bit product.
inline void mum(uint64_t & a, uint64_t & b)
{
    uint64_t lo = 0;
    uint64_t hi = 0;
    mul128(lo, hi, a, b);
    a = lo;
    b = hi;
}

/// MIX: a `mum` whose halves are xored together - the basic mixing step of `rapidhash`.
inline uint64_t mix(uint64_t a, uint64_t b)
{
    mum(a, b);
    return a ^ b;
}

inline uint64_t rapidhash(const void * key, size_t size, uint64_t seed)
{
    /// The secret constants of `rapidhash`.
    static constexpr uint64_t secret0 = 0x2d358dccaa6c78a5ULL;
    static constexpr uint64_t secret1 = 0x8bb84b93962eacc9ULL;
    static constexpr uint64_t secret2 = 0x4b33a62ed433d4a3ULL;

    auto read64 = [](const void * address) { return unalignedLoadLittleEndian<uint64_t>(address); };
    auto read32 = [](const void * address) { return static_cast<uint64_t>(unalignedLoadLittleEndian<uint32_t>(address)); };

    const uint8_t * p = static_cast<const uint8_t *>(key);
    uint64_t a = 0;
    uint64_t b = 0;

    seed ^= mix(seed ^ secret0, secret1) ^ size;

    if (size <= 16)
    {
        if (size >= 4)
        {
            const uint8_t * last = p + size - 4;
            const uint64_t delta = (size & 24) >> (size >> 3);
            a = (read32(p) << 32) | read32(last);
            b = (read32(p + delta) << 32) | read32(last - delta);
        }
        else if (size > 0)
        {
            /// 1..3 bytes: spread the first, the middle and the last byte over the 64-bit word.
            a = (static_cast<uint64_t>(p[0]) << 56) | (static_cast<uint64_t>(p[size >> 1]) << 32) | p[size - 1];
        }
    }
    else
    {
        size_t remaining = size;

        if (remaining > 48)
        {
            uint64_t seed1 = seed;
            uint64_t seed2 = seed;

            do
            {
                seed = mix(read64(p) ^ secret0, read64(p + 8) ^ seed);
                seed1 = mix(read64(p + 16) ^ secret1, read64(p + 24) ^ seed1);
                seed2 = mix(read64(p + 32) ^ secret2, read64(p + 40) ^ seed2);
                p += 48;
                remaining -= 48;
            } while (remaining >= 48);

            seed ^= seed1 ^ seed2;
        }

        if (remaining > 16)
        {
            seed = mix(read64(p) ^ secret2, read64(p + 8) ^ seed ^ secret1);
            if (remaining > 32)
                seed = mix(read64(p + 16) ^ secret2, read64(p + 24) ^ seed);
        }

        a = read64(p + remaining - 16);
        b = read64(p + remaining - 8);
    }

    a ^= secret1;
    b ^= seed;
    mum(a, b);
    return mix(a ^ secret0 ^ size, b ^ secret1);
}

}
