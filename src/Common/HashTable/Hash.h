#pragma once

#include <city.h>
#include <Core/Types.h>
#include <Core/UUID.h>
#include <base/StringRef.h>
#include <base/types.h>
#include <base/unaligned.h>

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
inline UInt64 intHash64(UInt64 x)
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

#if (defined(__PPC64__) || defined(__powerpc64__)) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#include <vec_crc32.h>
#endif

#if defined(__s390x__) && __BYTE_ORDER__==__ORDER_BIG_ENDIAN__
#include <base/crc32c_s390x.h>
#endif

/// NOTE: Intel intrinsic can be confusing.
/// - https://code.google.com/archive/p/sse-intrinsics/wikis/PmovIntrinsicBug.wiki
/// - https://stackoverflow.com/questions/15752770/mm-crc32-u64-poorly-defined
inline UInt64 intHashCRC32(UInt64 x)
{
#ifdef __SSE4_2__
    return _mm_crc32_u64(-1ULL, x);
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
    return __crc32cd(-1U, x);
#elif (defined(__PPC64__) || defined(__powerpc64__)) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return crc32_ppc(-1U, reinterpret_cast<const unsigned char *>(&x), sizeof(x));
#elif defined(__s390x__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
    return s390x_crc32c(-1U, x);
#else
    /// On other platforms we do not have CRC32. NOTE This can be confusing.
    /// NOTE: consider using intHash32()
    return intHash64(x);
#endif
}
inline UInt64 intHashCRC32(UInt64 x, UInt64 updated_value)
{
#ifdef __SSE4_2__
    return _mm_crc32_u64(updated_value, x);
#elif defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
    return __crc32cd(static_cast<UInt32>(updated_value), x);
#elif (defined(__PPC64__) || defined(__powerpc64__)) && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    return crc32_ppc(updated_value, reinterpret_cast<const unsigned char *>(&x), sizeof(x));
#elif defined(__s390x__) && __BYTE_ORDER__==__ORDER_BIG_ENDIAN__
    return s390x_crc32c(updated_value, x);
#else
    /// On other platforms we do not have CRC32. NOTE This can be confusing.
    return intHash64(x) ^ updated_value;
#endif
}

template <typename T>
requires std::has_unique_object_representations_v<T> && (sizeof(T) % sizeof(UInt64) == 0)
inline UInt64 intHashCRC32(const T & x, UInt64 updated_value)
{
    const auto * begin = reinterpret_cast<const char *>(&x);
    for (size_t i = 0; i < sizeof(T); i += sizeof(UInt64))
    {
        updated_value = intHashCRC32(unalignedLoad<UInt64>(begin), updated_value);
        begin += sizeof(UInt64);
    }

    return updated_value;
}

template <std::floating_point T>
requires(sizeof(T) <= sizeof(UInt64))
inline UInt64 intHashCRC32(T x, UInt64 updated_value)
{
    static_assert(std::numeric_limits<T>::is_iec559);

    // In IEEE 754, the only two floating point numbers that compare equal are 0.0 and -0.0.
    // See std::hash<float>.
    if (x == static_cast<T>(0.0))
        return intHashCRC32(0, updated_value);

    UInt64 repr;
    if constexpr (sizeof(T) == sizeof(UInt32))
        repr = std::bit_cast<UInt32>(x);
    else
        repr = std::bit_cast<UInt64>(x);

    return intHashCRC32(repr, updated_value);
}

inline UInt32 updateWeakHash32(const UInt8 * pos, size_t size, UInt32 updated_value)
{
    if (size < 8)
    {
        UInt64 value = 0;

        switch (size)
        {
            case 0:
                break;
            case 1:
                if constexpr (std::endian::native == std::endian::little)
                    __builtin_memcpy(&value, pos, 1);
                else
                    reverseMemcpy(&value, pos, 1);
                break;
            case 2:
                if constexpr (std::endian::native == std::endian::little)
                    __builtin_memcpy(&value, pos, 2);
                else
                    reverseMemcpy(&value, pos, 2);
                break;
            case 3:
                if constexpr (std::endian::native == std::endian::little)
                    __builtin_memcpy(&value, pos, 3);
                else
                    reverseMemcpy(&value, pos, 3);
                break;
            case 4:
                if constexpr (std::endian::native == std::endian::little)
                    __builtin_memcpy(&value, pos, 4);
                else
                    reverseMemcpy(&value, pos, 4);
                break;
            case 5:
                if constexpr (std::endian::native == std::endian::little)
                    __builtin_memcpy(&value, pos, 5);
                else
                    reverseMemcpy(&value, pos, 5);
                break;
            case 6:
                if constexpr (std::endian::native == std::endian::little)
                    __builtin_memcpy(&value, pos, 6);
                else
                    reverseMemcpy(&value, pos, 6);
                break;
            case 7:
                if constexpr (std::endian::native == std::endian::little)
                    __builtin_memcpy(&value, pos, 7);
                else
                    reverseMemcpy(&value, pos, 7);
                break;
            default:
                UNREACHABLE();
        }

        reinterpret_cast<unsigned char *>(&value)[7] = size;
        return static_cast<UInt32>(intHashCRC32(value, updated_value));
    }

    const auto * end = pos + size;
    while (pos + 8 <= end)
    {
        auto word = unalignedLoadLittleEndian<UInt64>(pos);
        updated_value = static_cast<UInt32>(intHashCRC32(word, updated_value));

        pos += 8;
    }

    if (pos < end)
    {
        /// If string size is not divisible by 8.
        /// Lets' assume the string was 'abcdefghXYZ', so it's tail is 'XYZ'.
        UInt8 tail_size = end - pos;
        /// Load tailing 8 bytes. Word is 'defghXYZ'.
        auto word = unalignedLoadLittleEndian<UInt64>(end - 8);
        /// Prepare mask which will set other 5 bytes to 0. It is 0xFFFFFFFFFFFFFFFF << 5 = 0xFFFFFF0000000000.
        /// word & mask = '\0\0\0\0\0XYZ' (bytes are reversed because of little ending)
        word &= (~UInt64(0)) << UInt8(8 * (8 - tail_size));
        /// Use least byte to store tail length.
        word |= tail_size;
        /// Now word is '\3\0\0\0\0XYZ'
        updated_value = static_cast<UInt32>(intHashCRC32(word, updated_value));
    }

    return updated_value;
}

template <typename T>
requires (sizeof(T) <= sizeof(UInt64))
inline size_t DefaultHash64(T key)
{
    UInt64 out {0};
    if constexpr (std::endian::native == std::endian::little)
        std::memcpy(&out, &key, sizeof(T));
    else
        std::memcpy(reinterpret_cast<char*>(&out) + sizeof(UInt64) - sizeof(T), &key, sizeof(T));
    return intHash64(out);
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
    else if constexpr (std::is_same_v<T, DB::UUID> || std::is_same_v<T, DB::IPv6>)
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
    UNREACHABLE();
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
inline size_t hashCRC32(T key, UInt64 updated_value = -1)
{
    UInt64 out {0};
    if constexpr (std::endian::native == std::endian::little)
        std::memcpy(&out, &key, sizeof(T));
    else
        std::memcpy(reinterpret_cast<char*>(&out) + sizeof(UInt64) - sizeof(T), &key, sizeof(T));
    return intHashCRC32(out, updated_value);
}

template <typename T>
requires (sizeof(T) > sizeof(UInt64))
inline size_t hashCRC32(T key, UInt64 updated_value = -1)
{
    return intHashCRC32(key, updated_value);
}

#define DEFINE_HASH(T) \
template <> struct HashCRC32<T>\
{\
    size_t operator() (T key) const\
    {\
        return hashCRC32<T>(key);\
    }\
};

DEFINE_HASH(UInt8)
DEFINE_HASH(UInt16)
DEFINE_HASH(UInt32)
DEFINE_HASH(UInt64)
DEFINE_HASH(UInt128)
DEFINE_HASH(UInt256)
DEFINE_HASH(Int8)
DEFINE_HASH(Int16)
DEFINE_HASH(Int32)
DEFINE_HASH(Int64)
DEFINE_HASH(Int128)
DEFINE_HASH(Int256)
DEFINE_HASH(BFloat16)
DEFINE_HASH(Float32)
DEFINE_HASH(Float64)
DEFINE_HASH(DB::UUID)
DEFINE_HASH(DB::IPv4)
DEFINE_HASH(DB::IPv6)

#undef DEFINE_HASH


struct UInt128Hash
{
    size_t operator()(UInt128 x) const
    {
        return CityHash_v1_0_2::Hash128to64({x.items[UInt128::_impl::little(0)], x.items[UInt128::_impl::little(1)]});
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
        crc = __crc32cd(static_cast<UInt32>(crc), x.items[0]);
        crc = __crc32cd(static_cast<UInt32>(crc), x.items[1]);
        return crc;
    }
};

#elif defined(__s390x__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__

struct UInt128HashCRC32
{
    size_t operator()(UInt128 x) const
    {
        UInt64 crc = -1ULL;
        crc = s390x_crc32c(crc, x.items[UInt128::_impl::little(0)]);
        crc = s390x_crc32c(crc, x.items[UInt128::_impl::little(1)]);
        return crc;
    }
};
#else

/// On other platforms we do not use CRC32. NOTE This can be confusing.
struct UInt128HashCRC32 : public UInt128Hash {};

#endif

struct UInt128TrivialHash
{
    size_t operator()(UInt128 x) const { return x.items[UInt128::_impl::little(0)]; }
};

struct UUIDTrivialHash
{
    size_t operator()(DB::UUID x) const { return DB::UUIDHelpers::getHighBytes(x); }
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
        crc = __crc32cd(static_cast<UInt32>(crc), x.items[0]);
        crc = __crc32cd(static_cast<UInt32>(crc), x.items[1]);
        crc = __crc32cd(static_cast<UInt32>(crc), x.items[2]);
        crc = __crc32cd(static_cast<UInt32>(crc), x.items[3]);
        return crc;
    }
};

#elif defined(__s390x__) && __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
struct UInt256HashCRC32
{
    size_t operator()(UInt256 x) const
    {
        UInt64 crc = -1ULL;
        crc = s390x_crc32c(crc, x.items[UInt256::_impl::little(0)]);
        crc = s390x_crc32c(crc, x.items[UInt256::_impl::little(1)]);
        crc = s390x_crc32c(crc, x.items[UInt256::_impl::little(2)]);
        crc = s390x_crc32c(crc, x.items[UInt256::_impl::little(3)]);
        return crc;
    }
};
#else

/// We do not need to use CRC32 on other platforms. NOTE This can be confusing.
struct UInt256HashCRC32 : public UInt256Hash {};

#endif

template <>
struct DefaultHash<UInt128> : public UInt128Hash {};

template <>
struct DefaultHash<UInt256> : public UInt256Hash {};

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
template <UInt64 salt>
inline UInt32 intHash32(UInt64 key)
{
    key ^= salt;

    key = (~key) + (key << 18);
    key = key ^ ((key >> 31) | (key << 33));
    key = key * 21;
    key = key ^ ((key >> 11) | (key << 53));
    key = key + (key << 6);
    key = key ^ ((key >> 22) | (key << 42));

    return static_cast<UInt32>(key);
}


/// For containers.
template <typename T, UInt64 salt = 0>
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
            UInt64 out {0};
            if constexpr (std::endian::native == std::endian::little)
                std::memcpy(&out, &key, sizeof(T));
            else
                std::memcpy(reinterpret_cast<char*>(&out) + sizeof(UInt64) - sizeof(T), &key, sizeof(T));
            return intHash32<salt>(out);
        }

        UNREACHABLE();
    }
};

template <>
struct DefaultHash<StringRef> : public StringRefHash {};
