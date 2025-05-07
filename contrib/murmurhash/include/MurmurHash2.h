//-----------------------------------------------------------------------------
// MurmurHash2 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.

#ifndef MURMURHASH2_H
#define MURMURHASH2_H

#include <stddef.h>

//-----------------------------------------------------------------------------
// Platform-specific functions and macros

// Microsoft Visual Studio

#if defined(_MSC_VER) && (_MSC_VER < 1600)

typedef unsigned char uint8_t;
typedef unsigned int uint32_t;
typedef unsigned __int64 uint64_t;

// Other compilers

#else	// defined(_MSC_VER)

#include <stdint.h>

#endif // !defined(_MSC_VER)

//-----------------------------------------------------------------------------

#ifdef __cplusplus
extern "C" {
#endif

//
//-----------------------------------------------------------------------------
// Block read - on little-endian machines this is a single load,
// while on big-endian or unknown machines the byte accesses should
// still get optimized into the most efficient instruction.
static inline __attribute__((__always_inline__)) uint32_t getblock(const uint32_t * p)
{
#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
    return *p;
#else
    const uint8_t * c = reinterpret_cast<const uint8_t *>(p);
    return static_cast<uint32_t>(c[0]) | static_cast<uint32_t>(c[1]) << 8 | static_cast<uint32_t>(c[2]) << 16
        | static_cast<uint32_t>(c[3]) << 24;
#endif
}

static inline __attribute__((__always_inline__)) uint64_t getblock(const uint64_t * p)
{
#if defined(__BYTE_ORDER__) && (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
    return *p;
#else
    const uint8_t * c = reinterpret_cast<const uint8_t *>(p);
    return static_cast<uint64_t>(c[0]) | static_cast<uint64_t>(c[1]) << 8 | static_cast<uint64_t>(c[2]) << 16
        | static_cast<uint64_t>(c[3]) << 24 | static_cast<uint64_t>(c[4]) << 32 | static_cast<uint64_t>(c[5]) << 40
        | static_cast<uint64_t>(c[6]) << 48 | static_cast<uint64_t>(c[7]) << 56;
#endif
}

inline __attribute__((__always_inline__)) uint32_t MurmurHash2(const void * key, size_t len, uint32_t seed)
{
    // 'm' and 'r' are mixing constants generated offline.
    // They're not really 'magic', they just happen to work well.

    const uint32_t m = 0x5bd1e995;
    const int r = 24;

    // Initialize the hash to a 'random' value

    uint32_t h = seed ^ len;

    // Mix 4 bytes at a time into the hash

    const unsigned char * data = static_cast<const unsigned char *>(key);

    while (len >= 4)
    {
        uint32_t k = getblock(reinterpret_cast<const uint32_t *>(data));

        k *= m;
        k ^= k >> r;
        k *= m;

        h *= m;
        h ^= k;

        data += 4;
        len -= 4;
    }

    // Handle the last few bytes of the input array

    switch (len)
    {
        case 3:
            h ^= data[2] << 16;
            [[fallthrough]];
        case 2:
            h ^= data[1] << 8;
            [[fallthrough]];
        case 1:
            h ^= data[0];
            h *= m;
    }

    // Do a few final mixes of the hash to ensure the last few
    // bytes are well-incorporated.

    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;

    return h;
}

//-----------------------------------------------------------------------------
// MurmurHash2, 64-bit versions, by Austin Appleby

// The same caveats as 32-bit MurmurHash2 apply here - beware of alignment
// and endian-ness issues if used across multiple platforms.

// 64-bit hash for 64-bit platforms
inline __attribute__((__always_inline__)) uint64_t MurmurHash64A(const void * key, size_t len, uint64_t seed)
{
    const uint64_t m = 0xc6a4a7935bd1e995llu;
    const int r = 47;

    uint64_t h = seed ^ (len * m);

    const uint64_t * data = static_cast<const uint64_t *>(key);
    const uint64_t * end = data + (len / 8);

    while (data != end)
    {
        uint64_t k = getblock(data++);

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const unsigned char * data2 = reinterpret_cast<const unsigned char *>(data);

    switch (len & 7)
    {
        case 7:
            h ^= uint64_t(data2[6]) << 48;
            [[fallthrough]];
        case 6:
            h ^= uint64_t(data2[5]) << 40;
            [[fallthrough]];
        case 5:
            h ^= uint64_t(data2[4]) << 32;
            [[fallthrough]];
        case 4:
            h ^= uint64_t(data2[3]) << 24;
            [[fallthrough]];
        case 3:
            h ^= uint64_t(data2[2]) << 16;
            [[fallthrough]];
        case 2:
            h ^= uint64_t(data2[1]) << 8;
            [[fallthrough]];
        case 1:
            h ^= uint64_t(data2[0]);
            h *= m;
    }

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}


uint64_t MurmurHash64B      ( const void * key, size_t len, uint64_t seed );
uint32_t MurmurHash2A       ( const void * key, size_t len, uint32_t seed );
uint32_t MurmurHashNeutral2 ( const void * key, size_t len, uint32_t seed );
uint32_t MurmurHashAligned2 ( const void * key, size_t len, uint32_t seed );

#ifdef __cplusplus
}
#endif

//-----------------------------------------------------------------------------

#endif // _MURMURHASH2_H_

