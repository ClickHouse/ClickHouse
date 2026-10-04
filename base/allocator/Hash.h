#pragma once

/// MurmurHash3 (public domain, Austin Appleby), exactly as in jemalloc. Used by the cuckoo hash and profiling.
/// jemalloc: `hash.h`.
///
/// Blocks are read in native byte order, so on big-endian platforms the results differ from little-endian ones,
/// like in jemalloc (where `hash` also switches to the x86 128-bit variant on big-endian).

#include <allocator/Common.h>

#include <climits>
#include <cstdint>
#include <cstring>

namespace jemalloc::hash
{

/// jemalloc: hash_rotl_32
JE_ALWAYS_INLINE uint32_t rotl32(uint32_t x, int8_t r)
{
    return (x << r) | (x >> (32 - r));
}

/// jemalloc: hash_rotl_64
JE_ALWAYS_INLINE uint64_t rotl64(uint64_t x, int8_t r)
{
    return (x << r) | (x >> (64 - r));
}

/// Handles unaligned reads.
/// jemalloc: hash_get_block_32
JE_ALWAYS_INLINE uint32_t getBlock32(const uint8_t * p, int i)
{
    uint32_t ret;
    std::memcpy(&ret, p + ptrdiff_t(i) * 4, sizeof(uint32_t));
    return ret;
}

/// jemalloc: hash_get_block_64
JE_ALWAYS_INLINE uint64_t getBlock64(const uint8_t * p, int i)
{
    uint64_t ret;
    std::memcpy(&ret, p + ptrdiff_t(i) * 8, sizeof(uint64_t));
    return ret;
}

/// jemalloc: hash_fmix_32
JE_ALWAYS_INLINE uint32_t fmix32(uint32_t h)
{
    h ^= h >> 16;
    h *= 0x85ebca6b;
    h ^= h >> 13;
    h *= 0xc2b2ae35;
    h ^= h >> 16;
    return h;
}

/// jemalloc: hash_fmix_64
JE_ALWAYS_INLINE uint64_t fmix64(uint64_t k)
{
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return k;
}

/// jemalloc: hash_x86_32
inline uint32_t x86_32(const void * key, int len, uint32_t seed)
{
    const uint8_t * data = static_cast<const uint8_t *>(key);
    const int nblocks = len / 4;

    uint32_t h1 = seed;

    const uint32_t c1 = 0xcc9e2d51;
    const uint32_t c2 = 0x1b873593;

    /// body
    {
        const uint8_t * blocks = data + nblocks * 4;
        for (int i = -nblocks; i; ++i)
        {
            uint32_t k1 = getBlock32(blocks, i);

            k1 *= c1;
            k1 = rotl32(k1, 15);
            k1 *= c2;

            h1 ^= k1;
            h1 = rotl32(h1, 13);
            h1 = h1 * 5 + 0xe6546b64;
        }
    }

    /// tail
    {
        const uint8_t * tail = data + nblocks * 4;
        uint32_t k1 = 0;

        switch (len & 3)
        {
            case 3:
                k1 ^= uint32_t(tail[2]) << 16;
                [[fallthrough]];
            case 2:
                k1 ^= uint32_t(tail[1]) << 8;
                [[fallthrough]];
            case 1:
                k1 ^= tail[0];
                k1 *= c1;
                k1 = rotl32(k1, 15);
                k1 *= c2;
                h1 ^= k1;
        }
    }

    /// finalization
    h1 ^= uint32_t(len);
    h1 = fmix32(h1);
    return h1;
}

/// jemalloc: hash_x86_128
inline void x86_128(const void * key, const int len, uint32_t seed, uint64_t r_out[2])
{
    const uint8_t * data = static_cast<const uint8_t *>(key);
    const int nblocks = len / 16;

    uint32_t h1 = seed;
    uint32_t h2 = seed;
    uint32_t h3 = seed;
    uint32_t h4 = seed;

    const uint32_t c1 = 0x239b961b;
    const uint32_t c2 = 0xab0e9789;
    const uint32_t c3 = 0x38b34ae5;
    const uint32_t c4 = 0xa1e38b93;

    /// body
    {
        const uint8_t * blocks = data + nblocks * 16;
        for (int i = -nblocks; i; ++i)
        {
            uint32_t k1 = getBlock32(blocks, i * 4 + 0);
            uint32_t k2 = getBlock32(blocks, i * 4 + 1);
            uint32_t k3 = getBlock32(blocks, i * 4 + 2);
            uint32_t k4 = getBlock32(blocks, i * 4 + 3);

            k1 *= c1;
            k1 = rotl32(k1, 15);
            k1 *= c2;
            h1 ^= k1;

            h1 = rotl32(h1, 19);
            h1 += h2;
            h1 = h1 * 5 + 0x561ccd1b;

            k2 *= c2;
            k2 = rotl32(k2, 16);
            k2 *= c3;
            h2 ^= k2;

            h2 = rotl32(h2, 17);
            h2 += h3;
            h2 = h2 * 5 + 0x0bcaa747;

            k3 *= c3;
            k3 = rotl32(k3, 17);
            k3 *= c4;
            h3 ^= k3;

            h3 = rotl32(h3, 15);
            h3 += h4;
            h3 = h3 * 5 + 0x96cd1c35;

            k4 *= c4;
            k4 = rotl32(k4, 18);
            k4 *= c1;
            h4 ^= k4;

            h4 = rotl32(h4, 13);
            h4 += h1;
            h4 = h4 * 5 + 0x32ac3b17;
        }
    }

    /// tail
    {
        const uint8_t * tail = data + nblocks * 16;
        uint32_t k1 = 0;
        uint32_t k2 = 0;
        uint32_t k3 = 0;
        uint32_t k4 = 0;

        switch (len & 15)
        {
            case 15:
                k4 ^= uint32_t(tail[14]) << 16;
                [[fallthrough]];
            case 14:
                k4 ^= uint32_t(tail[13]) << 8;
                [[fallthrough]];
            case 13:
                k4 ^= uint32_t(tail[12]) << 0;
                k4 *= c4;
                k4 = rotl32(k4, 18);
                k4 *= c1;
                h4 ^= k4;
                [[fallthrough]];
            case 12:
                k3 ^= uint32_t(tail[11]) << 24;
                [[fallthrough]];
            case 11:
                k3 ^= uint32_t(tail[10]) << 16;
                [[fallthrough]];
            case 10:
                k3 ^= uint32_t(tail[9]) << 8;
                [[fallthrough]];
            case 9:
                k3 ^= uint32_t(tail[8]) << 0;
                k3 *= c3;
                k3 = rotl32(k3, 17);
                k3 *= c4;
                h3 ^= k3;
                [[fallthrough]];
            case 8:
                k2 ^= uint32_t(tail[7]) << 24;
                [[fallthrough]];
            case 7:
                k2 ^= uint32_t(tail[6]) << 16;
                [[fallthrough]];
            case 6:
                k2 ^= uint32_t(tail[5]) << 8;
                [[fallthrough]];
            case 5:
                k2 ^= uint32_t(tail[4]) << 0;
                k2 *= c2;
                k2 = rotl32(k2, 16);
                k2 *= c3;
                h2 ^= k2;
                [[fallthrough]];
            case 4:
                k1 ^= uint32_t(tail[3]) << 24;
                [[fallthrough]];
            case 3:
                k1 ^= uint32_t(tail[2]) << 16;
                [[fallthrough]];
            case 2:
                k1 ^= uint32_t(tail[1]) << 8;
                [[fallthrough]];
            case 1:
                k1 ^= uint32_t(tail[0]) << 0;
                k1 *= c1;
                k1 = rotl32(k1, 15);
                k1 *= c2;
                h1 ^= k1;
                break;
        }
    }

    /// finalization
    h1 ^= uint32_t(len);
    h2 ^= uint32_t(len);
    h3 ^= uint32_t(len);
    h4 ^= uint32_t(len);

    h1 += h2;
    h1 += h3;
    h1 += h4;
    h2 += h1;
    h3 += h1;
    h4 += h1;

    h1 = fmix32(h1);
    h2 = fmix32(h2);
    h3 = fmix32(h3);
    h4 = fmix32(h4);

    h1 += h2;
    h1 += h3;
    h1 += h4;
    h2 += h1;
    h3 += h1;
    h4 += h1;

    r_out[0] = (uint64_t(h2) << 32) | h1;
    r_out[1] = (uint64_t(h4) << 32) | h3;
}

/// jemalloc: hash_x64_128
inline void x64_128(const void * key, const int len, const uint32_t seed, uint64_t r_out[2])
{
    const uint8_t * data = static_cast<const uint8_t *>(key);
    const int nblocks = len / 16;

    uint64_t h1 = seed;
    uint64_t h2 = seed;

    const uint64_t c1 = 0x87c37b91114253d5ULL;
    const uint64_t c2 = 0x4cf5ad432745937fULL;

    /// body
    {
        const uint8_t * blocks = data;
        for (int i = 0; i < nblocks; ++i)
        {
            uint64_t k1 = getBlock64(blocks, i * 2 + 0);
            uint64_t k2 = getBlock64(blocks, i * 2 + 1);

            k1 *= c1;
            k1 = rotl64(k1, 31);
            k1 *= c2;
            h1 ^= k1;

            h1 = rotl64(h1, 27);
            h1 += h2;
            h1 = h1 * 5 + 0x52dce729;

            k2 *= c2;
            k2 = rotl64(k2, 33);
            k2 *= c1;
            h2 ^= k2;

            h2 = rotl64(h2, 31);
            h2 += h1;
            h2 = h2 * 5 + 0x38495ab5;
        }
    }

    /// tail
    {
        const uint8_t * tail = data + nblocks * 16;
        uint64_t k1 = 0;
        uint64_t k2 = 0;

        switch (len & 15)
        {
            case 15:
                k2 ^= uint64_t(tail[14]) << 48;
                [[fallthrough]];
            case 14:
                k2 ^= uint64_t(tail[13]) << 40;
                [[fallthrough]];
            case 13:
                k2 ^= uint64_t(tail[12]) << 32;
                [[fallthrough]];
            case 12:
                k2 ^= uint64_t(tail[11]) << 24;
                [[fallthrough]];
            case 11:
                k2 ^= uint64_t(tail[10]) << 16;
                [[fallthrough]];
            case 10:
                k2 ^= uint64_t(tail[9]) << 8;
                [[fallthrough]];
            case 9:
                k2 ^= uint64_t(tail[8]) << 0;
                k2 *= c2;
                k2 = rotl64(k2, 33);
                k2 *= c1;
                h2 ^= k2;
                [[fallthrough]];
            case 8:
                k1 ^= uint64_t(tail[7]) << 56;
                [[fallthrough]];
            case 7:
                k1 ^= uint64_t(tail[6]) << 48;
                [[fallthrough]];
            case 6:
                k1 ^= uint64_t(tail[5]) << 40;
                [[fallthrough]];
            case 5:
                k1 ^= uint64_t(tail[4]) << 32;
                [[fallthrough]];
            case 4:
                k1 ^= uint64_t(tail[3]) << 24;
                [[fallthrough]];
            case 3:
                k1 ^= uint64_t(tail[2]) << 16;
                [[fallthrough]];
            case 2:
                k1 ^= uint64_t(tail[1]) << 8;
                [[fallthrough]];
            case 1:
                k1 ^= uint64_t(tail[0]) << 0;
                k1 *= c1;
                k1 = rotl64(k1, 31);
                k1 *= c2;
                h1 ^= k1;
                break;
        }
    }

    /// finalization
    /// `h ^= len` with `int len` converts it to `uint64_t` (sign-extending; `len` is never negative).
    h1 ^= uint64_t(int64_t(len));
    h2 ^= uint64_t(int64_t(len));

    h1 += h2;
    h2 += h1;

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 += h2;
    h2 += h1;

    r_out[0] = h1;
    r_out[1] = h2;
}

/// The hash used by the cuckoo hash table and the profiling backtrace table.
/// jemalloc: hash
inline void hash(const void * key, size_t len, const uint32_t seed, size_t r_hash[2])
{
    JE_ASSERT(len <= INT_MAX); /// Unfortunate implementation limitation.

    if constexpr (LG_SIZEOF_PTR == 3 && !config::big_endian)
    {
        static_assert(sizeof(size_t) == sizeof(uint64_t));
        uint64_t hashes[2];
        x64_128(key, int(len), seed, hashes);
        r_hash[0] = size_t(hashes[0]);
        r_hash[1] = size_t(hashes[1]);
    }
    else
    {
        uint64_t hashes[2];
        x86_128(key, int(len), seed, hashes);
        r_hash[0] = size_t(hashes[0]);
        r_hash[1] = size_t(hashes[1]);
    }
}

}
