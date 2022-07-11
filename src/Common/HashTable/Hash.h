#pragma once

#include <city.h>
#include <Core/Types.h>
#include <base/types.h>
#include <base/unaligned.h>
#include <base/StringRef.h>

#include <type_traits>


/** Hash functions that are better than the trivial function std::hash.
  *
  * Example: when we do aggregation by the visitor ID, the performance increase is more than 5 times.
  * This is because of following reasons:
  * - in Metrica web analytics system, visitor identifier is an integer that has timestamp with seconds resolution in lower bits;
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
requires (sizeof(T) > sizeof(DB::UInt64))
inline DB::UInt64 intHashCRC32(const T & x, DB::UInt64 updated_value)
{
    const auto * begin = reinterpret_cast<const char *>(&x);
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
        UInt64 value = 0;

        switch (size)
        {
            case 0:
                break;
            case 1:
                __builtin_memcpy(&value, pos, 1);
                break;
            case 2:
                __builtin_memcpy(&value, pos, 2);
                break;
            case 3:
                __builtin_memcpy(&value, pos, 3);
                break;
            case 4:
                __builtin_memcpy(&value, pos, 4);
                break;
            case 5:
                __builtin_memcpy(&value, pos, 5);
                break;
            case 6:
                __builtin_memcpy(&value, pos, 6);
                break;
            case 7:
                __builtin_memcpy(&value, pos, 7);
                break;
            default:
                __builtin_unreachable();
        }

        reinterpret_cast<unsigned char *>(&value)[7] = size;
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
requires (sizeof(T) <= sizeof(UInt64))
inline size_t DefaultHash64(T key)
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
requires (sizeof(T) > sizeof(UInt64))
inline size_t DefaultHash64(T key)
{
    if constexpr (is_big_int_v<T> && sizeof(T) == 16)
    {
        /// TODO This is classical antipattern.
        return intHash64(
            static_cast<UInt64>(key) ^
            static_cast<UInt64>(key >> 64));
    }
    else if constexpr (std::is_same_v<T, DB::UUID>)
    {
        return intHash64(
            static_cast<UInt64>(key.toUnderType()) ^
            static_cast<UInt64>(key.toUnderType() >> 64));
    }
    else if constexpr (is_big_int_v<T> && sizeof(T) == 32)
    {
        return intHash64(
            static_cast<UInt64>(key) ^
            static_cast<UInt64>(key >> 64) ^
            static_cast<UInt64>(key >> 128) ^
            static_cast<UInt64>(key >> 256));
    }
    assert(false);
    __builtin_unreachable();
}

template <typename T>
struct DefaultHash
{
    size_t operator() (T key) const
    {
        return DefaultHash64<T>(key);
    }
};

template <DB::is_decimal T>
struct DefaultHash<T>
{
    size_t operator() (T key) const
    {
        return DefaultHash64<typename T::NativeType>(key.value);
    }
};

template <typename T> struct HashCRC32;

template <typename T>
requires (sizeof(T) <= sizeof(UInt64))
inline size_t hashCRC32(T key)
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
requires (sizeof(T) > sizeof(UInt64))
inline size_t hashCRC32(T key)
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
DEFINE_HASH(DB::UUID)

#undef DEFINE_HASH


struct UInt128Hash
{
    size_t operator()(UInt128 x) const
    {
        return CityHash_v1_0_2::Hash128to64({x.items[0], x.items[1]});
    }
};

struct UUIDHash
{
    size_t operator()(DB::UUID x) const
    {
        return UInt128Hash()(x.toUnderType());
    }
};

#ifdef __SSE4_2__

struct UInt128HashCRC32
{
    size_t operator()(UInt128 x) const
    {
        UInt64 crc = -1ULL;
        crc = _mm_crc32_u64(crc, x.items[0]);
        crc = _mm_crc32_u64(crc, x.items[1]);
        return crc;
    }
};

#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)

struct UInt128HashCRC32
{
    size_t operator()(UInt128 x) const
    {
        UInt64 crc = -1ULL;
        crc = __crc32cd(crc, x.items[0]);
        crc = __crc32cd(crc, x.items[1]);
        return crc;
    }
};

#else

/// On other platforms we do not use CRC32. NOTE This can be confusing.
struct UInt128HashCRC32 : public UInt128Hash {};

#endif

struct UInt128TrivialHash
{
    size_t operator()(UInt128 x) const { return x.items[0]; }
};

struct UUIDTrivialHash
{
    size_t operator()(DB::UUID x) const { return x.toUnderType().items[0]; }
};

struct UInt256Hash
{
    size_t operator()(UInt256 x) const
    {
        /// NOTE suboptimal
        return CityHash_v1_0_2::Hash128to64({
            CityHash_v1_0_2::Hash128to64({x.items[0], x.items[1]}),
            CityHash_v1_0_2::Hash128to64({x.items[2], x.items[3]})});
    }
};

#ifdef __SSE4_2__

struct UInt256HashCRC32
{
    size_t operator()(UInt256 x) const
    {
        UInt64 crc = -1ULL;
        crc = _mm_crc32_u64(crc, x.items[0]);
        crc = _mm_crc32_u64(crc, x.items[1]);
        crc = _mm_crc32_u64(crc, x.items[2]);
        crc = _mm_crc32_u64(crc, x.items[3]);
        return crc;
    }
};

#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)

struct UInt256HashCRC32
{
    size_t operator()(UInt256 x) const
    {
        UInt64 crc = -1ULL;
        crc = __crc32cd(crc, x.items[0]);
        crc = __crc32cd(crc, x.items[1]);
        crc = __crc32cd(crc, x.items[2]);
        crc = __crc32cd(crc, x.items[3]);
        return crc;
    }
};

#else

/// We do not need to use CRC32 on other platforms. NOTE This can be confusing.
struct UInt256HashCRC32 : public UInt256Hash {};

#endif

template <>
struct DefaultHash<DB::UInt128> : public UInt128Hash {};

template <>
struct DefaultHash<DB::UInt256> : public UInt256Hash {};

template <>
struct DefaultHash<DB::UUID> : public UUIDHash {};


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
        if constexpr (is_big_int_v<T> && sizeof(T) == 16)
        {
            return intHash32<salt>(key.items[0] ^ key.items[1]);
        }
        else if constexpr (is_big_int_v<T> && sizeof(T) == 32)
        {
            return intHash32<salt>(key.items[0] ^ key.items[1] ^ key.items[2] ^ key.items[3]);
        }
        else if constexpr (sizeof(T) <= sizeof(UInt64))
        {
            return intHash32<salt>(key);
        }

        assert(false);
        __builtin_unreachable();
    }
};

template <>
struct DefaultHash<StringRef> : public StringRefHash {};
