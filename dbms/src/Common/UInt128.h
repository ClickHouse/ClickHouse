#pragma once

#include <Common/HashTable/Hash.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#if __SSE4_2__
#include <nmmintrin.h>
#endif


namespace DB
{


/// For aggregation by SipHash or concatenation of several fields.
struct UInt128
{
/// Suppress gcc7 warnings: 'prev_key.DB::UInt128::first' may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

    UInt64 first;
    UInt64 second;

    bool operator== (const UInt128 rhs) const { return first == rhs.first && second == rhs.second; }
    bool operator!= (const UInt128 rhs) const { return first != rhs.first || second != rhs.second; }

    bool operator== (const UInt64 rhs) const { return first == rhs && second == 0; }
    bool operator!= (const UInt64 rhs) const { return first != rhs || second != 0; }

#if !__clang__
#pragma GCC diagnostic pop
#endif

    UInt128 & operator= (const UInt64 rhs) { first = rhs; second = 0; return *this; }
};

struct UInt128Hash
{
    size_t operator()(UInt128 x) const
    {
        return Hash128to64({x.first, x.second});
    }
};

#if __SSE4_2__

struct UInt128HashCRC32
{
    size_t operator()(UInt128 x) const
    {
        UInt64 crc = -1ULL;
        crc = _mm_crc32_u64(crc, x.first);
        crc = _mm_crc32_u64(crc, x.second);
        return crc;
    }
};

#else

/// On other platforms we do not use CRC32. NOTE This can be confusing.
struct UInt128HashCRC32 : public UInt128Hash {};

#endif

struct UInt128TrivialHash
{
    size_t operator()(UInt128 x) const { return x.first; }
};

inline void readBinary(UInt128 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void writeBinary(const UInt128 & x, WriteBuffer & buf) { writePODBinary(x, buf); }


/** Used for aggregation, for putting a large number of constant-length keys in a hash table.
  */
struct UInt256
{

/// Suppress gcc7 warnings: 'prev_key.DB::UInt256::a' may be used uninitialized in this function
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

    UInt64 a;
    UInt64 b;
    UInt64 c;
    UInt64 d;

    bool operator== (const UInt256 rhs) const
    {
        return a == rhs.a && b == rhs.b && c == rhs.c && d == rhs.d;

    /* So it's no better.
        return 0xFFFF == _mm_movemask_epi8(_mm_and_si128(
            _mm_cmpeq_epi8(
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(&a)),
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(&rhs.a))),
            _mm_cmpeq_epi8(
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(&c)),
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(&rhs.c)))));*/
    }

    bool operator!= (const UInt256 rhs) const { return !operator==(rhs); }

    bool operator== (const UInt64 rhs) const { return a == rhs && b == 0 && c == 0 && d == 0; }
    bool operator!= (const UInt64 rhs) const { return !operator==(rhs); }

#if !__clang__
#pragma GCC diagnostic pop
#endif

    UInt256 & operator= (const UInt64 rhs) { a = rhs; b = 0; c = 0; d = 0; return *this; }
};

struct UInt256Hash
{
    size_t operator()(UInt256 x) const
    {
        /// NOTE suboptimal
        return Hash128to64({Hash128to64({x.a, x.b}), Hash128to64({x.c, x.d})});
    }
};

#if __SSE4_2__

struct UInt256HashCRC32
{
    size_t operator()(UInt256 x) const
    {
        UInt64 crc = -1ULL;
        crc = _mm_crc32_u64(crc, x.a);
        crc = _mm_crc32_u64(crc, x.b);
        crc = _mm_crc32_u64(crc, x.c);
        crc = _mm_crc32_u64(crc, x.d);
        return crc;
    }
};

#else

/// We do not need to use CRC32 on other platforms. NOTE This can be confusing.
struct UInt256HashCRC32
{
    DefaultHash<UInt64> hash64;
    size_t operator()(UInt256 x) const
    {
        /// TODO This is not optimal.
        return hash64(hash64(hash64(hash64(x.a) ^ x.b) ^ x.c) ^ x.d);
    }
};

#endif



inline void readBinary(UInt256 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void writeBinary(const UInt256 & x, WriteBuffer & buf) { writePODBinary(x, buf); }

}
