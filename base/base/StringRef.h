#pragma once

#include <cassert>
#include <stdexcept> // for std::logic_error
#include <string>
#include <type_traits>
#include <vector>
#include <functional>
#include <iosfwd>

#include <base/defines.h>
#include <base/types.h>
#include <base/unaligned.h>
#include <base/simd.h>
#include <fmt/core.h>
#include <fmt/ostream.h>

#include <city.h>

#if defined(__SSE2__)
    #include <emmintrin.h>
#endif

#if defined(__SSE4_2__)
    #include <smmintrin.h>
    #include <nmmintrin.h>
    #define CRC_INT _mm_crc32_u64
#endif

#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
    #include <arm_acle.h>
    #define CRC_INT __crc32cd
#endif

#if defined(__aarch64__) && defined(__ARM_NEON)
    #include <arm_neon.h>
    #pragma clang diagnostic ignored "-Wreserved-identifier"
#endif

#if defined(__s390x__)
    #include <base/crc32c_s390x.h>
    #define CRC_INT s390x_crc32c
#endif

/**
 * The std::string_view-like container to avoid creating strings to find substrings in the hash table.
 */
struct StringRef
{
    const char * data = nullptr;
    size_t size = 0;

    /// Non-constexpr due to reinterpret_cast.
    template <typename CharT>
    requires (sizeof(CharT) == 1)
    StringRef(const CharT * data_, size_t size_) : data(reinterpret_cast<const char *>(data_)), size(size_)
    {
        /// Sanity check for overflowed values.
        assert(size < 0x8000000000000000ULL);
    }

    constexpr StringRef(const char * data_, size_t size_) : data(data_), size(size_) {}

    StringRef(const std::string & s) : data(s.data()), size(s.size()) {} /// NOLINT
    constexpr explicit StringRef(std::string_view s) : data(s.data()), size(s.size()) {}
    constexpr StringRef(const char * data_) : StringRef(std::string_view{data_}) {} /// NOLINT
    constexpr StringRef() = default;

    bool empty() const { return size == 0; }

    std::string toString() const { return std::string(data, size); }
    explicit operator std::string() const { return toString(); }

    std::string_view toView() const { return std::string_view(data, size); }
    constexpr explicit operator std::string_view() const { return std::string_view(data, size); }
};


using StringRefs = std::vector<StringRef>;


#if defined(__SSE2__)

/** Compare strings for equality.
  * The approach is controversial and does not win in all cases.
  * For more information, see hash_map_string_2.cpp
  */

inline bool compare8(const char * p1, const char * p2)
{
    return 0xFFFF == _mm_movemask_epi8(_mm_cmpeq_epi8(
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1)),
        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2))));
}

inline bool compare64(const char * p1, const char * p2)
{
    return 0xFFFF == _mm_movemask_epi8(
        _mm_and_si128(
            _mm_and_si128(
                _mm_cmpeq_epi8(
                    _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1)),
                    _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2))),
                _mm_cmpeq_epi8(
                    _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1) + 1),
                    _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2) + 1))),
            _mm_and_si128(
                _mm_cmpeq_epi8(
                    _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1) + 2),
                    _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2) + 2)),
                _mm_cmpeq_epi8(
                    _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1) + 3),
                    _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2) + 3)))));
}

#elif defined(__aarch64__) && defined(__ARM_NEON)

inline bool compare8(const char * p1, const char * p2)
{
    uint64_t mask = getNibbleMask(vceqq_u8(
            vld1q_u8(reinterpret_cast<const unsigned char *>(p1)), vld1q_u8(reinterpret_cast<const unsigned char *>(p2))));
    return 0xFFFFFFFFFFFFFFFF == mask;
}

inline bool compare64(const char * p1, const char * p2)
{
    uint64_t mask = getNibbleMask(vandq_u8(
        vandq_u8(vceqq_u8(vld1q_u8(reinterpret_cast<const unsigned char *>(p1)), vld1q_u8(reinterpret_cast<const unsigned char *>(p2))),
            vceqq_u8(vld1q_u8(reinterpret_cast<const unsigned char *>(p1 + 16)), vld1q_u8(reinterpret_cast<const unsigned char *>(p2 + 16)))),
        vandq_u8(vceqq_u8(vld1q_u8(reinterpret_cast<const unsigned char *>(p1 + 32)), vld1q_u8(reinterpret_cast<const unsigned char *>(p2 + 32))),
            vceqq_u8(vld1q_u8(reinterpret_cast<const unsigned char *>(p1 + 48)), vld1q_u8(reinterpret_cast<const unsigned char *>(p2 + 48))))));
    return 0xFFFFFFFFFFFFFFFF == mask;
}

#endif

#if defined(__SSE2__) || (defined(__aarch64__) && defined(__ARM_NEON))

inline bool memequalWide(const char * p1, const char * p2, size_t size)
{
    /** The order of branches and the trick with overlapping comparisons
      * are the same as in memcpy implementation.
      * See the comments in base/glibc-compatibility/memcpy/memcpy.h
      */

    if (size <= 16)
    {
        if (size >= 8)
        {
            /// Chunks of 8..16 bytes.
            return unalignedLoad<uint64_t>(p1) == unalignedLoad<uint64_t>(p2)
                && unalignedLoad<uint64_t>(p1 + size - 8) == unalignedLoad<uint64_t>(p2 + size - 8);
        }
        if (size >= 4)
        {
            /// Chunks of 4..7 bytes.
            return unalignedLoad<uint32_t>(p1) == unalignedLoad<uint32_t>(p2)
                && unalignedLoad<uint32_t>(p1 + size - 4) == unalignedLoad<uint32_t>(p2 + size - 4);
        }
        if (size >= 2)
        {
            /// Chunks of 2..3 bytes.
            return unalignedLoad<uint16_t>(p1) == unalignedLoad<uint16_t>(p2)
                && unalignedLoad<uint16_t>(p1 + size - 2) == unalignedLoad<uint16_t>(p2 + size - 2);
        }
        if (size >= 1)
        {
            /// A single byte.
            return *p1 == *p2;
        }
        return true;
    }

    while (size >= 64)
    {
        if (compare64(p1, p2))
        {
            p1 += 64;
            p2 += 64;
            size -= 64;
        }
        else
            return false;
    }

    switch (size / 16) // NOLINT(bugprone-switch-missing-default-case)
    {
        case 3: if (!compare8(p1 + 32, p2 + 32)) return false; [[fallthrough]];
        case 2: if (!compare8(p1 + 16, p2 + 16)) return false; [[fallthrough]];
        case 1: if (!compare8(p1, p2)) return false; [[fallthrough]];
        default: ;
    }

    return compare8(p1 + size - 16, p2 + size - 16);
}

#endif

inline bool operator== (StringRef lhs, StringRef rhs)
{
    if (lhs.size != rhs.size)
        return false;

    if (lhs.size == 0)
        return true;

#if defined(__SSE2__) || (defined(__aarch64__) && defined(__ARM_NEON))
    return memequalWide(lhs.data, rhs.data, lhs.size);
#else
    return 0 == memcmp(lhs.data, rhs.data, lhs.size);
#endif
}

inline bool operator!= (StringRef lhs, StringRef rhs)
{
    return !(lhs == rhs);
}

inline bool operator< (StringRef lhs, StringRef rhs)
{
    int cmp = memcmp(lhs.data, rhs.data, std::min(lhs.size, rhs.size));
    return cmp < 0 || (cmp == 0 && lhs.size < rhs.size);
}

inline bool operator> (StringRef lhs, StringRef rhs)
{
    int cmp = memcmp(lhs.data, rhs.data, std::min(lhs.size, rhs.size));
    return cmp > 0 || (cmp == 0 && lhs.size > rhs.size);
}


/** Hash functions.
  * You can use either CityHash64,
  *  or a function based on the crc32 statement,
  *  which is obviously less qualitative, but on real data sets,
  *  when used in a hash table, works much faster.
  * For more information, see hash_map_string_3.cpp
  */

struct StringRefHash64
{
    size_t operator() (StringRef x) const
    {
        return CityHash_v1_0_2::CityHash64(x.data, x.size);
    }
};

#if defined(CRC_INT)

/// Parts are taken from CityHash.

inline UInt64 hashLen16(UInt64 u, UInt64 v)
{
    return CityHash_v1_0_2::Hash128to64(CityHash_v1_0_2::uint128(u, v));
}

inline UInt64 shiftMix(UInt64 val)
{
    return val ^ (val >> 47);
}

inline UInt64 rotateByAtLeast1(UInt64 val, UInt8 shift)
{
    return (val >> shift) | (val << (64 - shift));
}

inline size_t hashLessThan8(const char * data, size_t size)
{
    static constexpr UInt64 k2 = 0x9ae16a3b2f90404fULL;
    static constexpr UInt64 k3 = 0xc949d7c7509e6557ULL;

    if (size >= 4)
    {
        UInt64 a = unalignedLoadLittleEndian<uint32_t>(data);
        return hashLen16(size + (a << 3), unalignedLoadLittleEndian<uint32_t>(data + size - 4));
    }

    if (size > 0)
    {
        uint8_t a = data[0];
        uint8_t b = data[size >> 1];
        uint8_t c = data[size - 1];
        uint32_t y = static_cast<uint32_t>(a) + (static_cast<uint32_t>(b) << 8);
        uint32_t z = static_cast<uint32_t>(size) + (static_cast<uint32_t>(c) << 2);
        return shiftMix(y * k2 ^ z * k3) * k2;
    }

    return k2;
}

inline size_t hashLessThan16(const char * data, size_t size)
{
    if (size > 8)
    {
        UInt64 a = unalignedLoadLittleEndian<UInt64>(data);
        UInt64 b = unalignedLoadLittleEndian<UInt64>(data + size - 8);
        return hashLen16(a, rotateByAtLeast1(b + size, static_cast<UInt8>(size))) ^ b;
    }

    return hashLessThan8(data, size);
}

struct CRC32Hash
{
    unsigned operator() (StringRef x) const
    {
        const char * pos = x.data;
        size_t size = x.size;

        if (size == 0)
            return 0;

        chassert(pos);

        if (size < 8)
        {
            return static_cast<unsigned>(hashLessThan8(x.data, x.size));
        }

        const char * end = pos + size;
        unsigned res = -1U;

        do
        {
            UInt64 word = unalignedLoadLittleEndian<UInt64>(pos);
            res = static_cast<unsigned>(CRC_INT(res, word));

            pos += 8;
        } while (pos + 8 < end);

        UInt64 word = unalignedLoadLittleEndian<UInt64>(end - 8);    /// I'm not sure if this is normal.
        res = static_cast<unsigned>(CRC_INT(res, word));

        return res;
    }
};

struct StringRefHash : CRC32Hash {};

#else

struct CRC32Hash
{
    unsigned operator() (StringRef /* x */) const
    {
       throw std::logic_error{"Not implemented CRC32Hash without SSE"};
    }
};

struct StringRefHash : StringRefHash64 {};

#endif


namespace std
{
    template <>
    struct hash<StringRef> : public StringRefHash {};
}


namespace ZeroTraits
{
    inline bool check(const StringRef & x) { return 0 == x.size; }
    inline void set(StringRef & x) { x.size = 0; }
}

namespace PackedZeroTraits
{
    template <typename Second, template <typename, typename> class PackedPairNoInit>
    inline bool check(const PackedPairNoInit<StringRef, Second> p)
    { return 0 == p.key.size; }

    template <typename Second, template <typename, typename> class PackedPairNoInit>
    inline void set(PackedPairNoInit<StringRef, Second> & p)
    { p.key.size = 0; }
}


std::ostream & operator<<(std::ostream & os, const StringRef & str);

template<> struct fmt::formatter<StringRef> : fmt::ostream_formatter {};
