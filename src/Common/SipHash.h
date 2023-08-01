#pragma once

/** SipHash is a fast cryptographic hash function for short strings.
  * Taken from here: https://www.131002.net/siphash/
  *
  * This is SipHash 2-4 variant.
  *
  * Two changes are made:
  * - returns also 128 bits, not only 64;
  * - done streaming (can be calculated in parts).
  *
  * On short strings (URL, search phrases) more than 3 times faster than MD5 from OpenSSL.
  * (~ 700 MB/sec, 15 million strings per second)
  */

#include <bit>
#include <string>
#include <type_traits>
#include <Core/Defines.h>
#include <base/extended_types.h>
#include <base/types.h>
#include <base/unaligned.h>
#include <Common/Exception.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

#define SIPROUND                                                  \
    do                                                            \
    {                                                             \
        v0 += v1; v1 = std::rotl(v1, 13); v1 ^= v0; v0 = std::rotl(v0, 32); \
        v2 += v3; v3 = std::rotl(v3, 16); v3 ^= v2;                    \
        v0 += v3; v3 = std::rotl(v3, 21); v3 ^= v0;                    \
        v2 += v1; v1 = std::rotl(v1, 17); v1 ^= v2; v2 = std::rotl(v2, 32); \
    } while(0)

/// Define macro CURRENT_BYTES_IDX for building index used in current_bytes array
/// to ensure correct byte order on different endian machines
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
#define CURRENT_BYTES_IDX(i) (7 - i)
#else
#define CURRENT_BYTES_IDX(i) (i)
#endif

class SipHash
{
private:
    /// State.
    UInt64 v0;
    UInt64 v1;
    UInt64 v2;
    UInt64 v3;

    /// How many bytes have been processed.
    UInt64 cnt;

    /// Whether it should use the reference algo for 128-bit or CH's version
    bool is_reference_128;

    /// The current 8 bytes of input data.
    union
    {
        UInt64 current_word;
        UInt8 current_bytes[8];
    };

    ALWAYS_INLINE void finalize()
    {
        /// In the last free byte, we write the remainder of the division by 256.
        current_bytes[CURRENT_BYTES_IDX(7)] = static_cast<UInt8>(cnt);

        v3 ^= current_word;
        SIPROUND;
        SIPROUND;
        v0 ^= current_word;

        if (is_reference_128)
            v2 ^= 0xee;
        else
            v2 ^= 0xff;
        SIPROUND;
        SIPROUND;
        SIPROUND;
        SIPROUND;
    }

public:
    /// Arguments - seed.
    SipHash(UInt64 key0 = 0, UInt64 key1 = 0, bool is_reference_128_ = false) /// NOLINT
    {
        /// Initialize the state with some random bytes and seed.
        v0 = 0x736f6d6570736575ULL ^ key0;
        v1 = 0x646f72616e646f6dULL ^ key1;
        v2 = 0x6c7967656e657261ULL ^ key0;
        v3 = 0x7465646279746573ULL ^ key1;
        is_reference_128 = is_reference_128_;

        if (is_reference_128)
            v1 ^= 0xee;

        cnt = 0;
        current_word = 0;
    }

    ALWAYS_INLINE void update(const char * data, UInt64 size)
    {
        const char * end = data + size;

        /// We'll finish to process the remainder of the previous update, if any.
        if (cnt & 7)
        {
            while (cnt & 7 && data < end)
            {
                current_bytes[CURRENT_BYTES_IDX(cnt & 7)] = *data;
                ++data;
                ++cnt;
            }

            /// If we still do not have enough bytes to an 8-byte word.
            if (cnt & 7)
                return;

            v3 ^= current_word;
            SIPROUND;
            SIPROUND;
            v0 ^= current_word;
        }

        cnt += end - data;

        while (data + 8 <= end)
        {
            current_word = unalignedLoadLittleEndian<UInt64>(data);

            v3 ^= current_word;
            SIPROUND;
            SIPROUND;
            v0 ^= current_word;

            data += 8;
        }

        /// Pad the remainder, which is missing up to an 8-byte word.
        current_word = 0;
        switch (end - data)
        {
            case 7: current_bytes[CURRENT_BYTES_IDX(6)] = data[6]; [[fallthrough]];
            case 6: current_bytes[CURRENT_BYTES_IDX(5)] = data[5]; [[fallthrough]];
            case 5: current_bytes[CURRENT_BYTES_IDX(4)] = data[4]; [[fallthrough]];
            case 4: current_bytes[CURRENT_BYTES_IDX(3)] = data[3]; [[fallthrough]];
            case 3: current_bytes[CURRENT_BYTES_IDX(2)] = data[2]; [[fallthrough]];
            case 2: current_bytes[CURRENT_BYTES_IDX(1)] = data[1]; [[fallthrough]];
            case 1: current_bytes[CURRENT_BYTES_IDX(0)] = data[0]; [[fallthrough]];
            case 0: break;
        }
    }

    template <typename T>
    ALWAYS_INLINE void update(const T & x)
    {
        if constexpr (std::endian::native == std::endian::big)
        {
            T rev_x = x;
            char *start = reinterpret_cast<char *>(&rev_x);
            char *end = start + sizeof(T);
            std::reverse(start, end);
            update(reinterpret_cast<const char *>(&rev_x), sizeof(rev_x)); /// NOLINT
        }
        else
            update(reinterpret_cast<const char *>(&x), sizeof(x)); /// NOLINT
    }

    ALWAYS_INLINE void update(const std::string & x)
    {
        update(x.data(), x.length());
    }

    ALWAYS_INLINE void update(const std::string_view x)
    {
        update(x.data(), x.size());
    }

    /// Get the result in some form. This can only be done once!

    ALWAYS_INLINE void get128(char * out)
    {
        finalize();
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        unalignedStore<UInt64>(out + 8, v0 ^ v1);
        unalignedStore<UInt64>(out, v2 ^ v3);
#else
        unalignedStore<UInt64>(out, v0 ^ v1);
        unalignedStore<UInt64>(out + 8, v2 ^ v3);
#endif
    }

    template <typename T>
    ALWAYS_INLINE void get128(T & lo, T & hi)
    {
        static_assert(sizeof(T) == 8);
        finalize();
        lo = v0 ^ v1;
        hi = v2 ^ v3;
    }

    template <typename T>
    ALWAYS_INLINE void get128(T & dst)
    {
        static_assert(sizeof(T) == 16);
        get128(reinterpret_cast<char *>(&dst));
    }

    UInt64 get64()
    {
        finalize();
        return v0 ^ v1 ^ v2 ^ v3;
    }

    UInt128 get128()
    {
        UInt128 res;
        get128(res);
        return res;
    }

    UInt128 get128Reference()
    {
        if (!is_reference_128)
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR, "Logical error: can't call get128Reference when is_reference_128 is not set");
        finalize();
        auto lo = v0 ^ v1 ^ v2 ^ v3;
        v1 ^= 0xdd;
        SIPROUND;
        SIPROUND;
        SIPROUND;
        SIPROUND;
        auto hi = v0 ^ v1 ^ v2 ^ v3;

        if constexpr (std::endian::native == std::endian::big)
        {
            lo = std::byteswap(lo);
            hi = std::byteswap(hi);
            auto tmp = hi;
            hi = lo;
            lo = tmp;
        }

        UInt128 res = hi;
        res <<= 64;
        res |= lo;
        return res;
    }
};


#undef ROTL
#undef SIPROUND

#include <cstddef>

inline void sipHash128(const char * data, const size_t size, char * out)
{
    SipHash hash;
    hash.update(data, size);
    hash.get128(out);
}

inline UInt128 sipHash128Keyed(UInt64 key0, UInt64 key1, const char * data, const size_t size)
{
    SipHash hash(key0, key1);
    hash.update(data, size);
    return hash.get128();
}

inline UInt128 sipHash128(const char * data, const size_t size)
{
    return sipHash128Keyed(0, 0, data, size);
}

inline UInt128 sipHash128ReferenceKeyed(UInt64 key0, UInt64 key1, const char * data, const size_t size)
{
    SipHash hash(key0, key1, true);
    hash.update(data, size);
    return hash.get128Reference();
}

inline UInt128 sipHash128Reference(const char * data, const size_t size)
{
    return sipHash128ReferenceKeyed(0, 0, data, size);
}

inline UInt64 sipHash64Keyed(UInt64 key0, UInt64 key1, const char * data, const size_t size)
{
    SipHash hash(key0, key1);
    hash.update(data, size);
    return hash.get64();
}

inline UInt64 sipHash64(const char * data, const size_t size)
{
    return sipHash64Keyed(0, 0, data, size);
}

template <typename T>
UInt64 sipHash64(const T & x)
{
    SipHash hash;
    hash.update(x);
    return hash.get64();
}

inline UInt64 sipHash64(const std::string & s)
{
    return sipHash64(s.data(), s.size());
}

#undef CURRENT_BYTES_IDX
