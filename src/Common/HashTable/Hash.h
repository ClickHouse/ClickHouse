#pragma once

#include <common/types.h>
#include <Core/BigInt.h>
#include <Common/UInt128.h>
#include <common/unaligned.h>

#include <type_traits>


/** Hash functions that are better than the trivial function std::hash.
  *
  * Example: when we do aggregation by the visitor ID, the performance increase is more than 5 times.
  * This is because of following reasons:
  * - in Yandex, visitor identifier is an integer that has timestamp with seconds resolution in lower bits;
  * - in typical implementation of standard library, hash function for integers is trivial and just use lower bits;
  * - traffic is non-uniformly distributed across a day;
  * - we are using open-addressing linear probing hash tables that are most critical to hash function quality,
  *   and trivial hash function gives disastrous results.
  */

/** Taken from MurmurHash. This is Murmur finalizer.
  * Faster than intHash32 when inserting into the hash table UInt64 -> UInt64, where the key is the visitor ID.
  */
inline DB::UInt64 intHash64(DB::UInt64 x)
{
    x ^= x >> 33;
    x *= 0xff51afd7ed558ccdULL;
    x ^= x >> 33;
    x *= 0xc4ceb9fe1a85ec53ULL;
    x ^= x >> 33;

    return x;
}

/** CRC32C is not very high-quality as a hash function,
  *  according to avalanche and bit independence tests (see SMHasher software), as well as a small number of bits,
  *  but can behave well when used in hash tables,
  *  due to high speed (latency 3 + 1 clock cycle, throughput 1 clock cycle).
  * Works only with SSE 4.2 support.
  */
#ifdef __SSE4_2__
#include <nmmintrin.h>
#endif

#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#include <arm_neon.h>
#endif

inline DB::UInt64 intHashCRC32(DB::UInt64 x)
{
#ifdef __SSE4_2__
    return _mm_crc32_u64(-1ULL, x);
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
    return __crc32cd(-1U, x);
#else
    /// On other platforms we do not have CRC32. NOTE This can be confusing.
    return intHash64(x);
#endif
}

inline DB::UInt64 intHashCRC32(DB::UInt64 x, DB::UInt64 updated_value)
{
#ifdef __SSE4_2__
    return _mm_crc32_u64(updated_value, x);
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
    return  __crc32cd(updated_value, x);
#else
    /// On other platforms we do not have CRC32. NOTE This can be confusing.
    return intHash64(x) ^ updated_value;
#endif
}

template <typename T>
inline typename std::enable_if<(sizeof(T) > sizeof(DB::UInt64)), DB::UInt64>::type
intHashCRC32(const T & x, DB::UInt64 updated_value)
{
    auto * begin = reinterpret_cast<const char *>(&x);
    for (size_t i = 0; i < sizeof(T); i += sizeof(UInt64))
    {
        updated_value = intHashCRC32(unalignedLoad<DB::UInt64>(begin), updated_value);
        begin += sizeof(DB::UInt64);
    }

    return updated_value;
}


inline UInt32 updateWeakHash32(const DB::UInt8 * pos, size_t size, DB::UInt32 updated_value)
{
    if (size < 8)
    {
        DB::UInt64 value = 0;
        auto * value_ptr = reinterpret_cast<unsigned char *>(&value);

        typedef __attribute__((__aligned__(1))) uint16_t uint16_unaligned_t;
        typedef __attribute__((__aligned__(1))) uint32_t uint32_unaligned_t;

        /// Adopted code from FastMemcpy.h (memcpy_tiny)
        switch (size)
        {
            case 0:
                break;
            case 1:
                value_ptr[0] = pos[0];
                break;
            case 2:
                *reinterpret_cast<uint16_t *>(value_ptr) = *reinterpret_cast<const uint16_unaligned_t *>(pos);
                break;
            case 3:
                *reinterpret_cast<uint16_t *>(value_ptr) = *reinterpret_cast<const uint16_unaligned_t *>(pos);
                value_ptr[2] = pos[2];
                break;
            case 4:
                *reinterpret_cast<uint32_t *>(value_ptr) = *reinterpret_cast<const uint32_unaligned_t *>(pos);
                break;
            case 5:
                *reinterpret_cast<uint32_t *>(value_ptr) = *reinterpret_cast<const uint32_unaligned_t *>(pos);
                value_ptr[4] = pos[4];
                break;
            case 6:
                *reinterpret_cast<uint32_t *>(value_ptr) = *reinterpret_cast<const uint32_unaligned_t *>(pos);
                *reinterpret_cast<uint16_unaligned_t *>(value_ptr + 4) =
                        *reinterpret_cast<const uint16_unaligned_t *>(pos + 4);
                break;
            case 7:
                *reinterpret_cast<uint32_t *>(value_ptr) = *reinterpret_cast<const uint32_unaligned_t *>(pos);
                *reinterpret_cast<uint32_unaligned_t *>(value_ptr + 3) =
                        *reinterpret_cast<const uint32_unaligned_t *>(pos + 3);
                break;
            default:
                __builtin_unreachable();
        }

        value_ptr[7] = size;
        return intHashCRC32(value, updated_value);
    }

    const auto * end = pos + size;
    while (pos + 8 <= end)
    {
        auto word = unalignedLoad<UInt64>(pos);
        updated_value = intHashCRC32(word, updated_value);

        pos += 8;
    }

    if (pos < end)
    {
        /// If string size is not divisible by 8.
        /// Lets' assume the string was 'abcdefghXYZ', so it's tail is 'XYZ'.
        DB::UInt8 tail_size = end - pos;
        /// Load tailing 8 bytes. Word is 'defghXYZ'.
        auto word = unalignedLoad<UInt64>(end - 8);
        /// Prepare mask which will set other 5 bytes to 0. It is 0xFFFFFFFFFFFFFFFF << 5 = 0xFFFFFF0000000000.
        /// word & mask = '\0\0\0\0\0XYZ' (bytes are reversed because of little ending)
        word &= (~UInt64(0)) << DB::UInt8(8 * (8 - tail_size));
        /// Use least byte to store tail length.
        word |= tail_size;
        /// Now word is '\3\0\0\0\0XYZ'
        updated_value = intHashCRC32(word, updated_value);
    }

    return updated_value;
}

template <typename T>
inline size_t DefaultHash64(std::enable_if_t<(sizeof(T) <= sizeof(UInt64)), T> key)
{
    union
    {
        T in;
        DB::UInt64 out;
    } u;
    u.out = 0;
    u.in = key;
    return intHash64(u.out);
}

template <typename T>
inline size_t DefaultHash64(std::enable_if_t<(sizeof(T) > sizeof(UInt64)), T> key)
{
    if constexpr (std::is_same_v<T, DB::Int128>)
    {
        return intHash64(static_cast<UInt64>(key) ^ static_cast<UInt64>(key >> 64));
    }
    if constexpr (std::is_same_v<T, DB::UInt128>)
    {
        return intHash64(key.low ^ key.high);
    }
    else if constexpr (is_big_int_v<T> && sizeof(T) == 32)
    {
        return intHash64(static_cast<UInt64>(key) ^
            static_cast<UInt64>(key >> 64) ^
            static_cast<UInt64>(key >> 128) ^
            static_cast<UInt64>(key >> 256));
    }
    __builtin_unreachable();
}

template <typename T, typename Enable = void>
struct DefaultHash;

template <typename T>
struct DefaultHash<T, std::enable_if_t<!DB::IsDecimalNumber<T>>>
{
    size_t operator() (T key) const
    {
        return DefaultHash64<T>(key);
    }
};

template <typename T>
struct DefaultHash<T, std::enable_if_t<DB::IsDecimalNumber<T>>>
{
    size_t operator() (T key) const
    {
        return DefaultHash64<typename T::NativeType>(key);
    }
};

template <typename T> struct HashCRC32;

template <typename T>
inline size_t hashCRC32(std::enable_if_t<(sizeof(T) <= sizeof(UInt64)), T> key)
{
    union
    {
        T in;
        DB::UInt64 out;
    } u;
    u.out = 0;
    u.in = key;
    return intHashCRC32(u.out);
}

template <typename T>
inline size_t hashCRC32(std::enable_if_t<(sizeof(T) > sizeof(UInt64)), T> key)
{
    return intHashCRC32(key, -1);
}

#define DEFINE_HASH(T) \
template <> struct HashCRC32<T>\
{\
    size_t operator() (T key) const\
    {\
        return hashCRC32<T>(key);\
    }\
};

DEFINE_HASH(DB::UInt8)
DEFINE_HASH(DB::UInt16)
DEFINE_HASH(DB::UInt32)
DEFINE_HASH(DB::UInt64)
DEFINE_HASH(DB::UInt128)
DEFINE_HASH(DB::UInt256)
DEFINE_HASH(DB::Int8)
DEFINE_HASH(DB::Int16)
DEFINE_HASH(DB::Int32)
DEFINE_HASH(DB::Int64)
DEFINE_HASH(DB::Int128)
DEFINE_HASH(DB::Int256)
DEFINE_HASH(DB::Float32)
DEFINE_HASH(DB::Float64)

#undef DEFINE_HASH


template <>
struct DefaultHash<DB::UInt128> : public DB::UInt128Hash {};

template <>
struct DefaultHash<DB::DummyUInt256> : public DB::UInt256Hash {};


/// It is reasonable to use for UInt8, UInt16 with sufficient hash table size.
struct TrivialHash
{
    template <typename T>
    size_t operator() (T key) const
    {
        return key;
    }
};


/** A relatively good non-cryptographic hash function from UInt64 to UInt32.
  * But worse (both in quality and speed) than just cutting intHash64.
  * Taken from here: http://www.concentric.net/~ttwang/tech/inthash.htm
  *
  * Slightly changed compared to the function by link: shifts to the right are accidentally replaced by a cyclic shift to the right.
  * This change did not affect the smhasher test results.
  *
  * It is recommended to use different salt for different tasks.
  * That was the case that in the database values were sorted by hash (for low-quality pseudo-random spread),
  *  and in another place, in the aggregate function, the same hash was used in the hash table,
  *  as a result, this aggregate function was monstrously slowed due to collisions.
  *
  * NOTE Salting is far from perfect, because it commutes with first steps of calculation.
  *
  * NOTE As mentioned, this function is slower than intHash64.
  * But occasionally, it is faster, when written in a loop and loop is vectorized.
  */
template <DB::UInt64 salt>
inline DB::UInt32 intHash32(DB::UInt64 key)
{
    key ^= salt;

    key = (~key) + (key << 18);
    key = key ^ ((key >> 31) | (key << 33));
    key = key * 21;
    key = key ^ ((key >> 11) | (key << 53));
    key = key + (key << 6);
    key = key ^ ((key >> 22) | (key << 42));

    return key;
}


/// For containers.
template <typename T, DB::UInt64 salt = 0>
struct IntHash32
{
    size_t operator() (const T & key) const
    {
        if constexpr (std::is_same_v<T, DB::Int128>)
        {
            return intHash32<salt>(static_cast<UInt64>(key) ^ static_cast<UInt64>(key >> 64));
        }
        else if constexpr (std::is_same_v<T, DB::UInt128>)
        {
            return intHash32<salt>(key.low ^ key.high);
        }
        else if constexpr (is_big_int_v<T> && sizeof(T) == 32)
        {
            return intHash32<salt>(static_cast<UInt64>(key) ^
                static_cast<UInt64>(key >> 64) ^
                static_cast<UInt64>(key >> 128) ^
                static_cast<UInt64>(key >> 256));
        }
        else if constexpr (sizeof(T) <= sizeof(UInt64))
            return intHash32<salt>(key);
        __builtin_unreachable();
    }
};
