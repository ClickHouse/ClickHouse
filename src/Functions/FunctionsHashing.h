#pragma once

#include <city.h>
#include <farmhash.h>
#include <metrohash.h>
#include <wyhash.h>
#include <MurmurHash2.h>
#include <MurmurHash3.h>

#include "config.h"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wused-but-marked-unused"
#include <xxhash.h>

#include <Common/SipHash.h>
#include <Common/typeid_cast.h>
#include <Common/safe_cast.h>
#include <Common/HashTable/Hash.h>

#if USE_SSL
#    include <openssl/md5.h>
#endif

#include <bit>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnMap.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/PerformanceAdaptors.h>
#include <Common/TargetSpecific.h>
#include <base/IPv4andIPv6.h>
#include <base/range.h>
#include <base/bit_cast.h>
#include <base/unaligned.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int NOT_IMPLEMENTED;
    extern const int ILLEGAL_COLUMN;
}

namespace impl
{
    struct SipHashKey
    {
        UInt64 key0 = 0;
        UInt64 key1 = 0;
    };

    struct SipHashKeyColumns
    {
        ColumnPtr key0;
        ColumnPtr key1;
        bool is_const;
        const ColumnArray::Offsets * offsets = nullptr;

        size_t size() const
        {
            assert(key0 && key1);
            assert(key0->size() == key1->size());
            if (offsets != nullptr && !offsets->empty())
                return offsets->back();
            return key0->size();
        }

        SipHashKey getKey(size_t i) const
        {
            if (is_const)
                i = 0;
            assert(key0->size() == key1->size());
            if (offsets != nullptr && i > 0)
            {
                const auto * const begin = std::upper_bound(offsets->begin(), offsets->end(), i - 1);
                const auto * upper = std::upper_bound(begin, offsets->end(), i);
                if (upper != offsets->end())
                    i = upper - begin;
            }
            const auto & key0data = assert_cast<const ColumnUInt64 &>(*key0).getData();
            const auto & key1data = assert_cast<const ColumnUInt64 &>(*key1).getData();
            assert(key0->size() > i);
            return {key0data[i], key1data[i]};
        }
    };

    static SipHashKeyColumns parseSipHashKeyColumns(const ColumnWithTypeAndName & key)
    {
        const auto * col_key = key.column.get();

        bool is_const;
        const ColumnTuple * col_key_tuple;
        if (isColumnConst(*col_key))
        {
            is_const = true;
            col_key_tuple = checkAndGetColumnConstData<ColumnTuple>(col_key);
        }
        else
        {
            is_const = false;
            col_key_tuple = checkAndGetColumn<ColumnTuple>(col_key);
        }

        if (!col_key_tuple || col_key_tuple->tupleSize() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The key must be of type Tuple(UInt64, UInt64)");

        SipHashKeyColumns result{.key0 = col_key_tuple->getColumnPtr(0), .key1 = col_key_tuple->getColumnPtr(1), .is_const = is_const};

        assert(result.key0);
        assert(result.key1);

        if (!checkColumn<ColumnUInt64>(*result.key0))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 1st element of the key tuple is not of type UInt64");
        if (!checkColumn<ColumnUInt64>(*result.key1))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The 2nd element of the key tuple is not of type UInt64");

        if (result.size() == 1)
            result.is_const = true;

        return result;
    }
}

/** Hashing functions.
  *
  * halfMD5: String -> UInt64
  *
  * A faster cryptographic hash function:
  * sipHash64: String -> UInt64
  *
  * Fast non-cryptographic hash function for strings:
  * cityHash64: String -> UInt64
  *
  * A non-cryptographic hashes from a tuple of values of any types (uses respective function for strings and intHash64 for numbers):
  * cityHash64: any* -> UInt64
  * sipHash64: any* -> UInt64
  * halfMD5: any* -> UInt64
  *
  * Fast non-cryptographic hash function from any integer:
  * intHash32: number -> UInt32
  * intHash64: number -> UInt64
  *
  */


struct IntHash32Impl
{
    using ReturnType = UInt32;

    static UInt32 apply(UInt64 x)
    {
        /// seed is taken from /dev/urandom. It allows you to avoid undesirable dependencies with hashes in different data structures.
        return intHash32<0x75D9543DE018BF45ULL>(x);
    }
};

struct IntHash64Impl
{
    using ReturnType = UInt64;

    static UInt64 apply(UInt64 x)
    {
        return intHash64(x ^ 0x4CF2D2BAAE6DA887ULL);
    }
};

template<typename T, typename HashFunction>
T combineHashesFunc(T t1, T t2)
{
    transformEndianness<std::endian::little>(t1);
    transformEndianness<std::endian::little>(t2);
    const T hashes[] {t1, t2};
    return HashFunction::apply(reinterpret_cast<const char *>(hashes), sizeof(hashes));
}


struct SipHash64Impl
{
    static constexpr auto name = "sipHash64";
    using ReturnType = UInt64;

    static UInt64 apply(const char * begin, size_t size) { return sipHash64(begin, size); }
    static UInt64 combineHashes(UInt64 h1, UInt64 h2) { return combineHashesFunc<UInt64, SipHash64Impl>(h1, h2); }

    static constexpr bool use_int_hash_for_pods = false;
};

struct SipHash64KeyedImpl
{
    static constexpr auto name = "sipHash64Keyed";
    using ReturnType = UInt64;
    using Key = impl::SipHashKey;
    using KeyColumns = impl::SipHashKeyColumns;

    static KeyColumns parseKeyColumns(const ColumnWithTypeAndName & key) { return impl::parseSipHashKeyColumns(key); }
    static Key getKey(const KeyColumns & key, size_t i) { return key.getKey(i); }

    static UInt64 applyKeyed(const Key & key, const char * begin, size_t size) { return sipHash64Keyed(key.key0, key.key1, begin, size); }

    static UInt64 combineHashesKeyed(const Key & key, UInt64 h1, UInt64 h2)
    {
        transformEndianness<std::endian::little>(h1);
        transformEndianness<std::endian::little>(h2);
        const UInt64 hashes[]{h1, h2};
        return applyKeyed(key, reinterpret_cast<const char *>(hashes), sizeof(hashes));
    }

    static constexpr bool use_int_hash_for_pods = false;
};

#if USE_SSL
struct HalfMD5Impl
{
    static constexpr auto name = "halfMD5";
    using ReturnType = UInt64;

    static UInt64 apply(const char * begin, size_t size)
    {
        union
        {
            unsigned char char_data[16];
            uint64_t uint64_data;
        } buf;

        MD5_CTX ctx;
        MD5_Init(&ctx);
        MD5_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
        MD5_Final(buf.char_data, &ctx);

        /// Compatibility with existing code. Cast need for old poco AND macos where UInt64 != uint64_t
        transformEndianness<std::endian::big>(buf.uint64_data);
        return buf.uint64_data;
    }

    static UInt64 combineHashes(UInt64 h1, UInt64 h2)
    {
        return combineHashesFunc<UInt64, HalfMD5Impl>(h1, h2);
    }

    /// If true, it will use intHash32 or intHash64 to hash POD types. This behaviour is intended for better performance of some functions.
    /// Otherwise it will hash bytes in memory as a string using corresponding hash function.

    static constexpr bool use_int_hash_for_pods = false;
};
#endif

struct SipHash128Impl
{
    static constexpr auto name = "sipHash128";

    using ReturnType = UInt128;

    static UInt128 combineHashes(UInt128 h1, UInt128 h2) { return combineHashesFunc<UInt128, SipHash128Impl>(h1, h2); }
    static UInt128 apply(const char * data, const size_t size) { return sipHash128(data, size); }

    static constexpr bool use_int_hash_for_pods = false;
};

struct SipHash128KeyedImpl
{
    static constexpr auto name = "sipHash128Keyed";
    using ReturnType = UInt128;
    using Key = impl::SipHashKey;
    using KeyColumns = impl::SipHashKeyColumns;

    static KeyColumns parseKeyColumns(const ColumnWithTypeAndName & key) { return impl::parseSipHashKeyColumns(key); }
    static Key getKey(const KeyColumns & key, size_t i) { return key.getKey(i); }

    static UInt128 applyKeyed(const Key & key, const char * begin, size_t size) { return sipHash128Keyed(key.key0, key.key1, begin, size); }

    static UInt128 combineHashesKeyed(const Key & key, UInt128 h1, UInt128 h2)
    {
        transformEndianness<std::endian::little>(h1);
        transformEndianness<std::endian::little>(h2);
        const UInt128 hashes[]{h1, h2};
        return applyKeyed(key, reinterpret_cast<const char *>(hashes), sizeof(hashes));
    }

    static constexpr bool use_int_hash_for_pods = false;
};

struct SipHash128ReferenceImpl
{
    static constexpr auto name = "sipHash128Reference";

    using ReturnType = UInt128;

    static UInt128 combineHashes(UInt128 h1, UInt128 h2) { return combineHashesFunc<UInt128, SipHash128ReferenceImpl>(h1, h2); }

    static UInt128 apply(const char * data, const size_t size) { return sipHash128Reference(data, size); }

    static constexpr bool use_int_hash_for_pods = false;
};

struct SipHash128ReferenceKeyedImpl
{
    static constexpr auto name = "sipHash128ReferenceKeyed";
    using ReturnType = UInt128;
    using Key = impl::SipHashKey;
    using KeyColumns = impl::SipHashKeyColumns;

    static KeyColumns parseKeyColumns(const ColumnWithTypeAndName & key) { return impl::parseSipHashKeyColumns(key); }
    static Key getKey(const KeyColumns & key, size_t i) { return key.getKey(i); }

    static UInt128 applyKeyed(const Key & key, const char * begin, size_t size)
    {
        return sipHash128ReferenceKeyed(key.key0, key.key1, begin, size);
    }

    static UInt128 combineHashesKeyed(const Key & key, UInt128 h1, UInt128 h2)
    {
#if __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
        UInt128 tmp;
        reverseMemcpy(&tmp, &h1, sizeof(UInt128));
        h1 = tmp;
        reverseMemcpy(&tmp, &h2, sizeof(UInt128));
        h2 = tmp;
#endif
        UInt128 hashes[] = {h1, h2};
        return applyKeyed(key, reinterpret_cast<const char *>(hashes), 2 * sizeof(UInt128));
    }

    static constexpr bool use_int_hash_for_pods = false;
};

/** Why we need MurmurHash2?
  * MurmurHash2 is an outdated hash function, superseded by MurmurHash3 and subsequently by CityHash, xxHash, HighwayHash.
  * Usually there is no reason to use MurmurHash.
  * It is needed for the cases when you already have MurmurHash in some applications and you want to reproduce it
  * in ClickHouse as is. For example, it is needed to reproduce the behaviour
  * for NGINX a/b testing module: https://nginx.ru/en/docs/http/ngx_http_split_clients_module.html
  */
struct MurmurHash2Impl32
{
    static constexpr auto name = "murmurHash2_32";

    using ReturnType = UInt32;

    static UInt32 apply(const char * data, const size_t size)
    {
        return MurmurHash2(data, size, 0);
    }

    static UInt32 combineHashes(UInt32 h1, UInt32 h2)
    {
        return IntHash32Impl::apply(h1) ^ h2;
    }

    static constexpr bool use_int_hash_for_pods = false;
};

struct MurmurHash2Impl64
{
    static constexpr auto name = "murmurHash2_64";
    using ReturnType = UInt64;

    static UInt64 apply(const char * data, const size_t size)
    {
        return MurmurHash64A(data, size, 0);
    }

    static UInt64 combineHashes(UInt64 h1, UInt64 h2)
    {
        return IntHash64Impl::apply(h1) ^ h2;
    }

    static constexpr bool use_int_hash_for_pods = false;
};

/// To be compatible with gcc: https://github.com/gcc-mirror/gcc/blob/41d6b10e96a1de98e90a7c0378437c3255814b16/libstdc%2B%2B-v3/include/bits/functional_hash.h#L191
struct GccMurmurHashImpl
{
    static constexpr auto name = "gccMurmurHash";
    using ReturnType = UInt64;

    static UInt64 apply(const char * data, const size_t size)
    {
        return MurmurHash64A(data, size, 0xc70f6907UL);
    }

    static UInt64 combineHashes(UInt64 h1, UInt64 h2)
    {
        return IntHash64Impl::apply(h1) ^ h2;
    }

    static constexpr bool use_int_hash_for_pods = false;
};

/// To be compatible with Default Partitioner in Kafka:
///     murmur2: https://github.com/apache/kafka/blob/461c5cfe056db0951d9b74f5adc45973670404d7/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L480
///     Default Partitioner: https://github.com/apache/kafka/blob/139f7709bd3f5926901a21e55043388728ccca78/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java#L328
struct KafkaMurmurHashImpl
{
    static constexpr auto name = "kafkaMurmurHash";

    using ReturnType = UInt32;

    static UInt32 apply(const char * data, const size_t size)
    {
        return MurmurHash2(data, size, 0x9747b28cU) & 0x7fffffff;
    }

    static UInt32 combineHashes(UInt32 h1, UInt32 h2)
    {
        return IntHash32Impl::apply(h1) ^ h2;
    }

    static constexpr bool use_int_hash_for_pods = false;
};

struct MurmurHash3Impl32
{
    static constexpr auto name = "murmurHash3_32";
    using ReturnType = UInt32;

    static UInt32 apply(const char * data, const size_t size)
    {
        union
        {
            UInt32 h;
            char bytes[sizeof(h)];
        };
        MurmurHash3_x86_32(data, size, 0, bytes);
        return h;
    }

    static UInt32 combineHashes(UInt32 h1, UInt32 h2)
    {
        return IntHash32Impl::apply(h1) ^ h2;
    }

    static constexpr bool use_int_hash_for_pods = false;
};

struct MurmurHash3Impl64
{
    static constexpr auto name = "murmurHash3_64";
    using ReturnType = UInt64;

    static UInt64 apply(const char * data, const size_t size)
    {
        union
        {
            UInt64 h[2];
            char bytes[16];
        };
        MurmurHash3_x64_128(data, size, 0, bytes);
        return h[0] ^ h[1];
    }

    static UInt64 combineHashes(UInt64 h1, UInt64 h2) { return IntHash64Impl::apply(h1) ^ h2; }

    static constexpr bool use_int_hash_for_pods = false;
};

struct MurmurHash3Impl128
{
    static constexpr auto name = "murmurHash3_128";

    using ReturnType = UInt128;

    static UInt128 apply(const char * data, const size_t size)
    {
        char bytes[16];
        MurmurHash3_x64_128(data, size, 0, bytes);
        return *reinterpret_cast<UInt128 *>(bytes);
    }

    static UInt128 combineHashes(UInt128 h1, UInt128 h2) { return combineHashesFunc<UInt128, MurmurHash3Impl128>(h1, h2); }

    static constexpr bool use_int_hash_for_pods = false;
};

/// Care should be taken to do all calculation in unsigned integers (to avoid undefined behaviour on overflow)
///  but obtain the same result as it is done in signed integers with two's complement arithmetic.
struct JavaHashImpl
{
    static constexpr auto name = "javaHash";
    using ReturnType = Int32;

    static ReturnType apply(int64_t x)
    {
        return static_cast<ReturnType>(
            static_cast<uint32_t>(x) ^ static_cast<uint32_t>(static_cast<uint64_t>(x) >> 32));
    }

    template <class T>
    requires std::same_as<T, int8_t> || std::same_as<T, int16_t> || std::same_as<T, int32_t>
    static ReturnType apply(T x)
    {
        return x;
    }

    template <class T>
    requires(!std::same_as<T, int8_t> && !std::same_as<T, int16_t> && !std::same_as<T, int32_t>)
    static ReturnType apply(T x)
    {
        if (std::is_unsigned_v<T>)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsigned types are not supported");
        const size_t size = sizeof(T);
        const char * data = reinterpret_cast<const char *>(&x);
        return apply(data, size);
    }

    static ReturnType apply(const char * data, const size_t size)
    {
        UInt32 h = 0;
        for (size_t i = 0; i < size; ++i)
            h = 31 * h + static_cast<UInt32>(static_cast<Int8>(data[i]));
        return static_cast<Int32>(h);
    }

    static ReturnType combineHashes(Int32, Int32)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Java hash is not combineable for multiple arguments");
    }

    static constexpr bool use_int_hash_for_pods = false;
};

struct JavaHashUTF16LEImpl
{
    static constexpr auto name = "javaHashUTF16LE";
    using ReturnType = Int32;

    static Int32 apply(const char * raw_data, const size_t raw_size)
    {
        char * data = const_cast<char *>(raw_data);
        size_t size = raw_size;

        // Remove Byte-order-mark(0xFFFE) for UTF-16LE
        if (size >= 2 && data[0] == '\xFF' && data[1] == '\xFE')
        {
            data += 2;
            size -= 2;
        }

        if (size % 2 != 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Arguments for javaHashUTF16LE must be in the form of UTF-16");

        UInt32 h = 0;
        for (size_t i = 0; i < size; i += 2)
            h = 31 * h + static_cast<UInt16>(static_cast<UInt8>(data[i]) | static_cast<UInt8>(data[i + 1]) << 8);

        return static_cast<Int32>(h);
    }

    static Int32 combineHashes(Int32, Int32)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Java hash is not combineable for multiple arguments");
    }

    static constexpr bool use_int_hash_for_pods = false;
};

/// This is just JavaHash with zeroed out sign bit.
/// This function is used in Hive for versions before 3.0,
///  after 3.0, Hive uses murmur-hash3.
struct HiveHashImpl
{
    static constexpr auto name = "hiveHash";
    using ReturnType = Int32;

    static Int32 apply(const char * data, const size_t size)
    {
        return static_cast<Int32>(0x7FFFFFFF & static_cast<UInt32>(JavaHashImpl::apply(data, size)));
    }

    static Int32 combineHashes(Int32, Int32)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Hive hash is not combineable for multiple arguments");
    }

    static constexpr bool use_int_hash_for_pods = false;
};

struct ImplCityHash64
{
    static constexpr auto name = "cityHash64";
    using ReturnType = UInt64;
    using uint128_t = CityHash_v1_0_2::uint128;

    static auto combineHashes(UInt64 h1, UInt64 h2) { return CityHash_v1_0_2::Hash128to64(uint128_t(h1, h2)); }
    static auto apply(const char * s, const size_t len) { return CityHash_v1_0_2::CityHash64(s, len); }
    static constexpr bool use_int_hash_for_pods = true;
};

// see farmhash.h for definition of NAMESPACE_FOR_HASH_FUNCTIONS
struct ImplFarmFingerprint64
{
    static constexpr auto name = "farmFingerprint64";
    using ReturnType = UInt64;
    using uint128_t = NAMESPACE_FOR_HASH_FUNCTIONS::uint128_t;

    static auto combineHashes(UInt64 h1, UInt64 h2) { return NAMESPACE_FOR_HASH_FUNCTIONS::Fingerprint(uint128_t(h1, h2)); }
    static auto apply(const char * s, const size_t len) { return NAMESPACE_FOR_HASH_FUNCTIONS::Fingerprint64(s, len); }
    static constexpr bool use_int_hash_for_pods = true;
};

// see farmhash.h for definition of NAMESPACE_FOR_HASH_FUNCTIONS
struct ImplFarmHash64
{
    static constexpr auto name = "farmHash64";
    using ReturnType = UInt64;
    using uint128_t = NAMESPACE_FOR_HASH_FUNCTIONS::uint128_t;

    static auto combineHashes(UInt64 h1, UInt64 h2) { return NAMESPACE_FOR_HASH_FUNCTIONS::Hash128to64(uint128_t(h1, h2)); }
    static auto apply(const char * s, const size_t len) { return NAMESPACE_FOR_HASH_FUNCTIONS::Hash64(s, len); }
    static constexpr bool use_int_hash_for_pods = true;
};

struct ImplMetroHash64
{
    static constexpr auto name = "metroHash64";
    using ReturnType = UInt64;
    using uint128_t = CityHash_v1_0_2::uint128;

    static auto combineHashes(UInt64 h1, UInt64 h2) { return CityHash_v1_0_2::Hash128to64(uint128_t(h1, h2)); }
    static auto apply(const char * s, const size_t len)
    {
        union
        {
            UInt64 u64;
            uint8_t u8[sizeof(u64)];
        };

        metrohash64_1(reinterpret_cast<const uint8_t *>(s), len, 0, u8);

        return u64;
    }

    static constexpr bool use_int_hash_for_pods = true;
};

struct ImplXxHash32
{
    static constexpr auto name = "xxHash32";
    using ReturnType = UInt32;

    static auto apply(const char * s, const size_t len) { return XXH_INLINE_XXH32(s, len, 0); }
    /**
      *  With current implementation with more than 1 arguments it will give the results
      *  non-reproducible from outside of CH.
      *
      *  Proper way of combining several input is to use streaming mode of hash function
      *  https://github.com/Cyan4973/xxHash/issues/114#issuecomment-334908566
      *
      *  In common case doable by init_state / update_state / finalize_state
      */
    static auto combineHashes(UInt32 h1, UInt32 h2) { return IntHash32Impl::apply(h1) ^ h2; }

    static constexpr bool use_int_hash_for_pods = false;
};

struct ImplXxHash64
{
    static constexpr auto name = "xxHash64";
    using ReturnType = UInt64;
    using uint128_t = CityHash_v1_0_2::uint128;

    static auto apply(const char * s, const size_t len) { return XXH_INLINE_XXH64(s, len, 0); }

    /*
       With current implementation with more than 1 arguments it will give the results
       non-reproducible from outside of CH. (see comment on ImplXxHash32).
     */
    static auto combineHashes(UInt64 h1, UInt64 h2) { return CityHash_v1_0_2::Hash128to64(uint128_t(h1, h2)); }

    static constexpr bool use_int_hash_for_pods = false;
};

struct ImplXXH3
{
    static constexpr auto name = "xxh3";
    using ReturnType = UInt64;
    using uint128_t = CityHash_v1_0_2::uint128;

    static auto apply(const char * s, const size_t len) { return XXH_INLINE_XXH3_64bits(s, len); }

    /*
       With current implementation with more than 1 arguments it will give the results
       non-reproducible from outside of CH. (see comment on ImplXxHash32).
     */
    static auto combineHashes(UInt64 h1, UInt64 h2) { return CityHash_v1_0_2::Hash128to64(uint128_t(h1, h2)); }

    static constexpr bool use_int_hash_for_pods = false;
};

DECLARE_MULTITARGET_CODE(

template <typename Impl, typename Name>
class FunctionIntHash : public IFunction
{
public:
    static constexpr auto name = Name::name;

private:
    using ToType = typename Impl::ReturnType;

    template <typename FromType>
    ColumnPtr executeType(const ColumnsWithTypeAndName & arguments) const
    {
        using ColVecType = ColumnVectorOrDecimal<FromType>;

        if (const ColVecType * col_from = checkAndGetColumn<ColVecType>(arguments[0].column.get()))
        {
            auto col_to = ColumnVector<ToType>::create();

            const typename ColVecType::Container & vec_from = col_from->getData();
            typename ColumnVector<ToType>::Container & vec_to = col_to->getData();

            size_t size = vec_from.size();
            vec_to.resize(size);
            for (size_t i = 0; i < size; ++i)
                vec_to[i] = Impl::apply(vec_from[i]);

            return col_to;
        }

        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(), Name::name);
    }

public:
    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!arguments[0]->isValueRepresentedByNumber())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
                arguments[0]->getName(), getName());

        return std::make_shared<DataTypeNumber<typename Impl::ReturnType>>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeNumber<typename Impl::ReturnType>>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const IDataType * from_type = arguments[0].type.get();
        WhichDataType which(from_type);

        if (which.isUInt8())
            return executeType<UInt8>(arguments);
        if (which.isUInt16())
            return executeType<UInt16>(arguments);
        if (which.isUInt32())
            return executeType<UInt32>(arguments);
        if (which.isUInt64())
            return executeType<UInt64>(arguments);
        if (which.isInt8())
            return executeType<Int8>(arguments);
        if (which.isInt16())
            return executeType<Int16>(arguments);
        if (which.isInt32())
            return executeType<Int32>(arguments);
        if (which.isInt64())
            return executeType<Int64>(arguments);
        if (which.isDate())
            return executeType<UInt16>(arguments);
        if (which.isDate32())
            return executeType<Int32>(arguments);
        if (which.isDateTime())
            return executeType<UInt32>(arguments);
        if (which.isDecimal32())
            return executeType<Decimal32>(arguments);
        if (which.isDecimal64())
            return executeType<Decimal64>(arguments);
        if (which.isIPv4())
            return executeType<IPv4>(arguments);

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}",
            arguments[0].type->getName(), getName());
    }
};

) // DECLARE_MULTITARGET_CODE

template <typename Impl, typename Name>
class FunctionIntHash : public TargetSpecific::Default::FunctionIntHash<Impl, Name>
{
public:
    explicit FunctionIntHash(ContextPtr context) : selector(context)
    {
        selector.registerImplementation<TargetArch::Default,
            TargetSpecific::Default::FunctionIntHash<Impl, Name>>();

    #if USE_MULTITARGET_CODE
        selector.registerImplementation<TargetArch::AVX2,
            TargetSpecific::AVX2::FunctionIntHash<Impl, Name>>();
        selector.registerImplementation<TargetArch::AVX512F,
            TargetSpecific::AVX512F::FunctionIntHash<Impl, Name>>();
    #endif
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionIntHash>(context);
    }

private:
    ImplementationSelector<IFunction> selector;
};

DECLARE_MULTITARGET_CODE(

template <typename Impl, bool Keyed, typename KeyType, typename KeyColumnsType>
class FunctionAnyHash : public IFunction
{
public:
    static constexpr auto name = Impl::name;

private:
    using ToType = typename Impl::ReturnType;

    template <typename FromType, bool first>
    void executeIntType(const KeyColumnsType & key_cols, const IColumn * column, typename ColumnVector<ToType>::Container & vec_to) const
    {
        using ColVecType = ColumnVectorOrDecimal<FromType>;
        KeyType key{};
        if constexpr (Keyed)
            key = Impl::getKey(key_cols, 0);

        if (const ColVecType * col_from = checkAndGetColumn<ColVecType>(column))
        {
            const typename ColVecType::Container & vec_from = col_from->getData();
            const size_t size = vec_from.size();
            for (size_t i = 0; i < size; ++i)
            {
                ToType hash;

                if constexpr (Keyed)
                    if (!key_cols.is_const && i != 0)
                        key = Impl::getKey(key_cols, i);

                if constexpr (Impl::use_int_hash_for_pods)
                {
                    if constexpr (std::is_same_v<ToType, UInt64>)
                        hash = IntHash64Impl::apply(bit_cast<UInt64>(vec_from[i]));
                    else
                        hash = IntHash32Impl::apply(bit_cast<UInt32>(vec_from[i]));
                }
                else
                {
                    if constexpr (std::is_same_v<Impl, JavaHashImpl>)
                        hash = JavaHashImpl::apply(vec_from[i]);
                    else
                    {
                        auto value = vec_from[i];
                        transformEndianness<std::endian::little>(value);
                        hash = apply(key, reinterpret_cast<const char *>(&value), sizeof(value));
                    }
                }

                if constexpr (first)
                    vec_to[i] = hash;
                else
                    vec_to[i] = combineHashes(key, vec_to[i], hash);
            }
        }
        else if (auto col_from_const = checkAndGetColumnConst<ColVecType>(column))
        {
            if constexpr (Keyed)
            {
                if (!key_cols.is_const)
                {
                    ColumnPtr full_column = col_from_const->convertToFullColumn();
                    return executeIntType<FromType, first>(key_cols, full_column.get(), vec_to);
                }
            }
            auto value = col_from_const->template getValue<FromType>();

            ToType hash;
            if constexpr (Impl::use_int_hash_for_pods)
            {
                if constexpr (std::is_same_v<ToType, UInt64>)
                    hash = IntHash64Impl::apply(bit_cast<UInt64>(value));
                else
                    hash = IntHash32Impl::apply(bit_cast<UInt32>(value));
            }
            else
            {
                if constexpr (std::is_same_v<Impl, JavaHashImpl>)
                    hash = JavaHashImpl::apply(value);
                else
                {
                    transformEndianness<std::endian::little>(value);
                    hash = apply(key, reinterpret_cast<const char *>(&value), sizeof(value));
                }
            }

            const size_t size = vec_to.size();
            if constexpr (first)
                vec_to.assign(size, hash);
            else
            {
                for (size_t i = 0; i < size; ++i)
                {
                    if constexpr (Keyed)
                        if (!key_cols.is_const && i != 0)
                            key = Impl::getKey(key_cols, i);
                    vec_to[i] = combineHashes(key, vec_to[i], hash);
                }
            }
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                column->getName(), getName());
    }

    template <typename FromType, bool first>
    void executeBigIntType(const KeyColumnsType & key_cols, const IColumn * column, typename ColumnVector<ToType>::Container & vec_to) const
    {
        using ColVecType = ColumnVectorOrDecimal<FromType>;
        KeyType key{};
        if constexpr (Keyed)
            key = Impl::getKey(key_cols, 0);

        static const auto to_little_endian = [](auto & value)
        {
            // IPv6 addresses are parsed into four 32-bit components in big-endian ordering on both platforms, so no change is necessary.
            // Reference: `parseIPv6orIPv4` in src/Common/formatIPv6.h.
            if constexpr (std::endian::native == std::endian::big && std::is_same_v<std::remove_reference_t<decltype(value)>, IPv6>)
                return;

            transformEndianness<std::endian::little>(value);
        };

        if (const ColVecType * col_from = checkAndGetColumn<ColVecType>(column))
        {
            const typename ColVecType::Container & vec_from = col_from->getData();
            size_t size = vec_from.size();
            for (size_t i = 0; i < size; ++i)
            {
                ToType hash;
                if constexpr (Keyed)
                    if (!key_cols.is_const && i != 0)
                        key = Impl::getKey(key_cols, i);
                if constexpr (std::endian::native == std::endian::little)
                    hash = apply(key, reinterpret_cast<const char *>(&vec_from[i]), sizeof(vec_from[i]));
                else
                {
                    auto value = vec_from[i];
                    to_little_endian(value);

                    hash = apply(key, reinterpret_cast<const char *>(&value), sizeof(value));
                }
                if constexpr (first)
                    vec_to[i] = hash;
                else
                    vec_to[i] = combineHashes(key, vec_to[i], hash);
            }
        }
        else if (auto col_from_const = checkAndGetColumnConst<ColVecType>(column))
        {
            if constexpr (Keyed)
            {
                if (!key_cols.is_const)
                {
                    ColumnPtr full_column = col_from_const->convertToFullColumn();
                    return executeBigIntType<FromType, first>(key_cols, full_column.get(), vec_to);
                }
            }
            auto value = col_from_const->template getValue<FromType>();
            to_little_endian(value);

            const auto hash = apply(key, reinterpret_cast<const char *>(&value), sizeof(value));
            const size_t size = vec_to.size();
            if constexpr (first)
                vec_to.assign(size, hash);
            else
            {
                for (size_t i = 0; i < size; ++i)
                {
                    if constexpr (Keyed)
                        if (!key_cols.is_const && i != 0)
                            key = Impl::getKey(key_cols, i);
                    vec_to[i] = combineHashes(key, vec_to[i], hash);
                }
            }
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}",
                column->getName(), getName());
    }

    template <bool first>
    void executeGeneric(const KeyColumnsType & key_cols, const IColumn * column, typename ColumnVector<ToType>::Container & vec_to) const
    {
        KeyType key{};
        if constexpr (Keyed)
            key = Impl::getKey(key_cols, 0);
        for (size_t i = 0, size = column->size(); i < size; ++i)
        {
            if constexpr (Keyed)
                if (!key_cols.is_const && i != 0)
                    key = Impl::getKey(key_cols, i);
            StringRef bytes = column->getDataAt(i);
            const ToType hash = apply(key, bytes.data, bytes.size);
            if constexpr (first)
                vec_to[i] = hash;
            else
                vec_to[i] = combineHashes(key, vec_to[i], hash);
        }
    }

    template <bool first>
    void executeString(const KeyColumnsType & key_cols, const IColumn * column, typename ColumnVector<ToType>::Container & vec_to) const
    {
        KeyType key{};
        if constexpr (Keyed)
            key = Impl::getKey(key_cols, 0);
        if (const ColumnString * col_from = checkAndGetColumn<ColumnString>(column))
        {
            const typename ColumnString::Chars & data = col_from->getChars();
            const typename ColumnString::Offsets & offsets = col_from->getOffsets();
            size_t size = offsets.size();

            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                if constexpr (Keyed)
                    if (!key_cols.is_const && i != 0)
                        key = Impl::getKey(key_cols, i);
                const ToType hash = apply(key,
                    reinterpret_cast<const char *>(&data[current_offset]),
                    offsets[i] - current_offset - 1);

                if constexpr (first)
                    vec_to[i] = hash;
                else
                    vec_to[i] = combineHashes(key, vec_to[i], hash);

                current_offset = offsets[i];
            }
        }
        else if (const ColumnFixedString * col_from_fixed = checkAndGetColumn<ColumnFixedString>(column))
        {
            const typename ColumnString::Chars & data = col_from_fixed->getChars();
            size_t n = col_from_fixed->getN();
            size_t size = data.size() / n;

            for (size_t i = 0; i < size; ++i)
            {
                if constexpr (Keyed)
                    if (!key_cols.is_const && i != 0)
                        key = Impl::getKey(key_cols, i);
                const ToType hash = apply(key, reinterpret_cast<const char *>(&data[i * n]), n);
                if constexpr (first)
                    vec_to[i] = hash;
                else
                    vec_to[i] = combineHashes(key, vec_to[i], hash);
            }
        }
        else if (const ColumnConst * col_from_const = checkAndGetColumnConstStringOrFixedString(column))
        {
            if constexpr (Keyed)
            {
                if (!key_cols.is_const)
                {
                    ColumnPtr full_column = col_from_const->convertToFullColumn();
                    return executeString<first>(key_cols, full_column.get(), vec_to);
                }
            }
            String value = col_from_const->getValue<String>();
            const ToType hash = apply(key, value.data(), value.size());
            const size_t size = vec_to.size();

            if constexpr (first)
                vec_to.assign(size, hash);
            else
            {
                for (size_t i = 0; i < size; ++i)
                {
                    if constexpr (Keyed)
                        if (!key_cols.is_const && i != 0)
                            key = Impl::getKey(key_cols, i);
                    vec_to[i] = combineHashes(key, vec_to[i], hash);
                }
            }
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                    column->getName(), getName());
    }

    template <bool first>
    void executeArray(const KeyColumnsType & key_cols, const IDataType * type, const IColumn * column, typename ColumnVector<ToType>::Container & vec_to) const
    {
        const IDataType * nested_type = typeid_cast<const DataTypeArray &>(*type).getNestedType().get();

        if (const ColumnArray * col_from = checkAndGetColumn<ColumnArray>(column))
        {
            const IColumn * nested_column = &col_from->getData();
            const ColumnArray::Offsets & offsets = col_from->getOffsets();
            const size_t nested_size = nested_column->size();

            typename ColumnVector<ToType>::Container vec_temp(nested_size);
            bool nested_is_first = true;

            if constexpr (Keyed)
            {
                KeyColumnsType key_cols_tmp{key_cols};
                key_cols_tmp.offsets = &offsets;
                executeForArgument(key_cols_tmp, nested_type, nested_column, vec_temp, nested_is_first);
            }
            else
                executeForArgument(key_cols, nested_type, nested_column, vec_temp, nested_is_first);

            const size_t size = offsets.size();

            ColumnArray::Offset current_offset = 0;
            KeyType key{};
            if constexpr (Keyed)
                key = Impl::getKey(key_cols, 0);
            for (size_t i = 0; i < size; ++i)
            {
                if constexpr (Keyed)
                    if (!key_cols.is_const && i != 0)
                        key = Impl::getKey(key_cols, i);
                ColumnArray::Offset next_offset = offsets[i];

                ToType hash;
                if constexpr (std::is_same_v<ToType, UInt64>)
                    hash = IntHash64Impl::apply(next_offset - current_offset);
                else
                    hash = IntHash32Impl::apply(next_offset - current_offset);

                if constexpr (first)
                    vec_to[i] = hash;
                else
                    vec_to[i] = combineHashes(key, vec_to[i], hash);

                for (size_t j = current_offset; j < next_offset; ++j)
                    vec_to[i] = combineHashes(key, vec_to[i], vec_temp[j]);

                current_offset = offsets[i];
            }
        }
        else if (const ColumnConst * col_from_const = checkAndGetColumnConst<ColumnArray>(column))
        {
            /// NOTE: here, of course, you can do without the materialization of the column.
            ColumnPtr full_column = col_from_const->convertToFullColumn();
            executeArray<first>(key_cols, type, full_column.get(), vec_to);
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                    column->getName(), getName());
    }

    template <bool first>
    void executeAny(const KeyColumnsType & key_cols, const IDataType * from_type, const IColumn * icolumn, typename ColumnVector<ToType>::Container & vec_to) const
    {
        WhichDataType which(from_type);

        if (icolumn->size() != vec_to.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Argument column '{}' size {} doesn't match result column size {} of function {}",
                icolumn->getName(), icolumn->size(), vec_to.size(), getName());

        if constexpr (Keyed)
            if (key_cols.size() != vec_to.size() && key_cols.size() != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Key column size {} doesn't match result column size {} of function {}", key_cols.size(), vec_to.size(), getName());

        if      (which.isUInt8()) executeIntType<UInt8, first>(key_cols, icolumn, vec_to);
        else if (which.isUInt16()) executeIntType<UInt16, first>(key_cols, icolumn, vec_to);
        else if (which.isUInt32()) executeIntType<UInt32, first>(key_cols, icolumn, vec_to);
        else if (which.isUInt64()) executeIntType<UInt64, first>(key_cols, icolumn, vec_to);
        else if (which.isUInt128()) executeBigIntType<UInt128, first>(key_cols, icolumn, vec_to);
        else if (which.isUInt256()) executeBigIntType<UInt256, first>(key_cols, icolumn, vec_to);
        else if (which.isInt8()) executeIntType<Int8, first>(key_cols, icolumn, vec_to);
        else if (which.isInt16()) executeIntType<Int16, first>(key_cols, icolumn, vec_to);
        else if (which.isInt32()) executeIntType<Int32, first>(key_cols, icolumn, vec_to);
        else if (which.isInt64()) executeIntType<Int64, first>(key_cols, icolumn, vec_to);
        else if (which.isInt128()) executeBigIntType<Int128, first>(key_cols, icolumn, vec_to);
        else if (which.isInt256()) executeBigIntType<Int256, first>(key_cols, icolumn, vec_to);
        else if (which.isUUID()) executeBigIntType<UUID, first>(key_cols, icolumn, vec_to);
        else if (which.isIPv4()) executeIntType<IPv4, first>(key_cols, icolumn, vec_to);
        else if (which.isIPv6()) executeBigIntType<IPv6, first>(key_cols, icolumn, vec_to);
        else if (which.isEnum8()) executeIntType<Int8, first>(key_cols, icolumn, vec_to);
        else if (which.isEnum16()) executeIntType<Int16, first>(key_cols, icolumn, vec_to);
        else if (which.isDate()) executeIntType<UInt16, first>(key_cols, icolumn, vec_to);
        else if (which.isDate32()) executeIntType<Int32, first>(key_cols, icolumn, vec_to);
        else if (which.isDateTime()) executeIntType<UInt32, first>(key_cols, icolumn, vec_to);
        /// TODO: executeIntType() for Decimal32/64 leads to incompatible result
        else if (which.isDecimal32()) executeBigIntType<Decimal32, first>(key_cols, icolumn, vec_to);
        else if (which.isDecimal64()) executeBigIntType<Decimal64, first>(key_cols, icolumn, vec_to);
        else if (which.isDecimal128()) executeBigIntType<Decimal128, first>(key_cols, icolumn, vec_to);
        else if (which.isDecimal256()) executeBigIntType<Decimal256, first>(key_cols, icolumn, vec_to);
        else if (which.isFloat32()) executeIntType<Float32, first>(key_cols, icolumn, vec_to);
        else if (which.isFloat64()) executeIntType<Float64, first>(key_cols, icolumn, vec_to);
        else if (which.isString()) executeString<first>(key_cols, icolumn, vec_to);
        else if (which.isFixedString()) executeString<first>(key_cols, icolumn, vec_to);
        else if (which.isArray()) executeArray<first>(key_cols, from_type, icolumn, vec_to);
        else executeGeneric<first>(key_cols, icolumn, vec_to);
    }

    /// Return a fixed random-looking magic number when input is empty.
    static constexpr auto filler = 0xe28dbde7fe22e41c;

    void executeForArgument(const KeyColumnsType & key_cols, const IDataType * type, const IColumn * column, typename ColumnVector<ToType>::Container & vec_to, bool & is_first) const
    {
        /// Flattening of tuples.
        if (const ColumnTuple * tuple = typeid_cast<const ColumnTuple *>(column))
        {
            const auto & tuple_columns = tuple->getColumns();
            const DataTypes & tuple_types = typeid_cast<const DataTypeTuple &>(*type).getElements();
            size_t tuple_size = tuple_columns.size();

            if (0 == tuple_size && is_first)
                for (auto & hash : vec_to)
                    hash = static_cast<ToType>(filler);

            for (size_t i = 0; i < tuple_size; ++i)
                executeForArgument(key_cols, tuple_types[i].get(), tuple_columns[i].get(), vec_to, is_first);
        }
        else if (const ColumnTuple * tuple_const = checkAndGetColumnConstData<ColumnTuple>(column))
        {
            const auto & tuple_columns = tuple_const->getColumns();
            const DataTypes & tuple_types = typeid_cast<const DataTypeTuple &>(*type).getElements();
            size_t tuple_size = tuple_columns.size();

            if (0 == tuple_size && is_first)
                for (auto & hash : vec_to)
                    hash = static_cast<ToType>(filler);

            for (size_t i = 0; i < tuple_size; ++i)
            {
                auto tmp = ColumnConst::create(tuple_columns[i], column->size());
                executeForArgument(key_cols, tuple_types[i].get(), tmp.get(), vec_to, is_first);
            }
        }
        else if (const auto * map = checkAndGetColumn<ColumnMap>(column))
        {
            const auto & type_map = assert_cast<const DataTypeMap &>(*type);
            executeForArgument(key_cols, type_map.getNestedType().get(), map->getNestedColumnPtr().get(), vec_to, is_first);
        }
        else if (const auto * const_map = checkAndGetColumnConst<ColumnMap>(column))
        {
            executeForArgument(key_cols, type, const_map->convertToFullColumnIfConst().get(), vec_to, is_first);
        }
        else
        {
            if (is_first)
                executeAny<true>(key_cols, type, column, vec_to);
            else
                executeAny<false>(key_cols, type, column, vec_to);
        }

        is_first = false;
    }

public:
    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        if constexpr (std::is_same_v<ToType, UInt128>) /// backward-compatible
        {
            return std::make_shared<DataTypeFixedString>(sizeof(UInt128));
        }
        else
            return std::make_shared<DataTypeNumber<ToType>>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        if constexpr (std::is_same_v<ToType, UInt128>) /// backward-compatible
        {
            return std::make_shared<DataTypeFixedString>(sizeof(UInt128));
        }
        else
            return std::make_shared<DataTypeNumber<ToType>>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto col_to = ColumnVector<ToType>::create(input_rows_count);

        if (input_rows_count != 0)
        {
            typename ColumnVector<ToType>::Container & vec_to = col_to->getData();

            /// If using a "keyed" algorithm, the first argument is the key and
            /// the data starts from the second argument.
            /// Otherwise there is no key and all arguments are interpreted as data.
            constexpr size_t first_data_argument = Keyed;

            if (arguments.size() <= first_data_argument)
                vec_to.assign(input_rows_count, static_cast<ToType>(filler));

            KeyColumnsType key_cols{};
            if constexpr (Keyed)
                if (!arguments.empty())
                    key_cols = Impl::parseKeyColumns(arguments[0]);

            /// The function supports arbitrary number of arguments of arbitrary types.
            bool is_first_argument = true;
            for (size_t i = first_data_argument; i < arguments.size(); ++i)
            {
                const auto & col = arguments[i];
                executeForArgument(key_cols, col.type.get(), col.column.get(), vec_to, is_first_argument);
            }
        }

        if constexpr (std::is_same_v<ToType, UInt128>) /// backward-compatible
        {
            if constexpr (std::endian::native == std::endian::big)
                std::ranges::for_each(col_to->getData(), transformEndianness<std::endian::little, std::endian::native, ToType>);

            auto col_to_fixed_string = ColumnFixedString::create(sizeof(UInt128));
            const auto & data = col_to->getData();
            auto & chars = col_to_fixed_string->getChars();
            chars.resize(data.size() * sizeof(UInt128));
            memcpy(chars.data(), data.data(), data.size() * sizeof(UInt128));
            return col_to_fixed_string;
        }

        return col_to;
    }

    static ToType apply(const KeyType & key, const char * begin, size_t size)
    {
        if constexpr (Keyed)
            return Impl::applyKeyed(key, begin, size);
        else
            return Impl::apply(begin, size);
    }

    static ToType combineHashes(const KeyType & key, ToType h1, ToType h2)
    {
        if constexpr (Keyed)
            return Impl::combineHashesKeyed(key, h1, h2);
        else
            return Impl::combineHashes(h1, h2);
    }
};

) // DECLARE_MULTITARGET_CODE

template <typename Impl, bool Keyed = false, typename KeyType = char, typename KeyColumnsType = char>
class FunctionAnyHash : public TargetSpecific::Default::FunctionAnyHash<Impl, Keyed, KeyType, KeyColumnsType>
{
public:
    explicit FunctionAnyHash(ContextPtr context) : selector(context)
    {
        selector
            .registerImplementation<TargetArch::Default, TargetSpecific::Default::FunctionAnyHash<Impl, Keyed, KeyType, KeyColumnsType>>();

#if USE_MULTITARGET_CODE
        selector.registerImplementation<TargetArch::AVX2, TargetSpecific::AVX2::FunctionAnyHash<Impl, Keyed, KeyType, KeyColumnsType>>();
        selector
            .registerImplementation<TargetArch::AVX512F, TargetSpecific::AVX512F::FunctionAnyHash<Impl, Keyed, KeyType, KeyColumnsType>>();
#endif
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return selector.selectAndExecute(arguments, result_type, input_rows_count);
    }

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionAnyHash>(context);
    }

private:
    ImplementationSelector<IFunction> selector;
};


struct URLHashImpl
{
    static UInt64 apply(const char * data, const size_t size)
    {
        /// do not take last slash, '?' or '#' character into account
        if (size > 0 && (data[size - 1] == '/' || data[size - 1] == '?' || data[size - 1] == '#'))
            return CityHash_v1_0_2::CityHash64(data, size - 1);

        return CityHash_v1_0_2::CityHash64(data, size);
    }
};


struct URLHierarchyHashImpl
{
    static size_t findLevelLength(const UInt64 level, const char * begin, const char * end)
    {
        const auto * pos = begin;

        /// Let's parse everything that goes before the path

        /// Suppose that the protocol has already been changed to lowercase.
        while (pos < end && ((*pos > 'a' && *pos < 'z') || (*pos > '0' && *pos < '9')))
            ++pos;

        /** We will calculate the hierarchy only for URLs in which there is a protocol, and after it there are two slashes.
          * (http, file - fit, mailto, magnet - do not fit), and after two slashes there is still something
          * For the rest, simply return the full URL as the only element of the hierarchy.
          */
        if (pos == begin || pos == end || !(pos + 3 < end && pos[0] == ':' && pos[1] == '/' && pos[2] == '/'))
        {
            return 0 == level ? end - begin : 0;
        }
        pos += 3;

        /// The domain for simplicity is everything that after the protocol and the two slashes, until the next slash or before `?` or `#`
        while (pos < end && !(*pos == '/' || *pos == '?' || *pos == '#'))
            ++pos;

        if (pos != end)
            ++pos;

        if (0 == level)
            return pos - begin;

        UInt64 current_level = 0;

        while (current_level != level && pos < end)
        {
            /// We go to the next `/` or `?` or `#`, skipping all at the beginning.
            while (pos < end && (*pos == '/' || *pos == '?' || *pos == '#'))
                ++pos;
            if (pos == end)
                break;
            while (pos < end && !(*pos == '/' || *pos == '?' || *pos == '#'))
                ++pos;

            if (pos != end)
                ++pos;

            ++current_level;
        }

        return current_level == level ? pos - begin : 0;
    }

    static UInt64 apply(const UInt64 level, const char * data, const size_t size)
    {
        return URLHashImpl::apply(data, findLevelLength(level, data, data + size));
    }
};


class FunctionURLHash : public IFunction
{
public:
    static constexpr auto name = "URLHash";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionURLHash>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto arg_count = arguments.size();
        if (arg_count != 1 && arg_count != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Number of arguments for function {} doesn't match: "
                "passed {}, should be 1 or 2.", getName(), arg_count);

        const auto * first_arg = arguments.front().get();
        if (!WhichDataType(first_arg).isString())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", first_arg->getName(), getName());

        if (arg_count == 2)
        {
            const auto & second_arg = arguments.back();
            if (!isInteger(second_arg))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", second_arg->getName(), getName());
        }

        return std::make_shared<DataTypeUInt64>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const auto arg_count = arguments.size();

        if (arg_count == 1)
            return executeSingleArg(arguments);
        if (arg_count == 2)
            return executeTwoArgs(arguments);
        throw Exception(ErrorCodes::LOGICAL_ERROR, "got into IFunction::execute with unexpected number of arguments");
    }

private:
    ColumnPtr executeSingleArg(const ColumnsWithTypeAndName & arguments) const
    {
        const auto * col_untyped = arguments.front().column.get();

        if (const auto * col_from = checkAndGetColumn<ColumnString>(col_untyped))
        {
            const auto size = col_from->size();
            auto col_to = ColumnUInt64::create(size);

            const auto & chars = col_from->getChars();
            const auto & offsets = col_from->getOffsets();
            auto & out = col_to->getData();

            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                out[i] = URLHashImpl::apply(
                    reinterpret_cast<const char *>(&chars[current_offset]),
                    offsets[i] - current_offset - 1);

                current_offset = offsets[i];
            }

            return col_to;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }

    ColumnPtr executeTwoArgs(const ColumnsWithTypeAndName & arguments) const
    {
        const auto * level_col = arguments.back().column.get();
        const auto * col_untyped = arguments.front().column.get();
        size_t size = col_untyped->size();

        if (const auto * col_from = checkAndGetColumn<ColumnString>(col_untyped))
        {
            auto col_to = ColumnUInt64::create(size);

            const auto & chars = col_from->getChars();
            const auto & offsets = col_from->getOffsets();
            auto & out = col_to->getData();

            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                out[i] = URLHierarchyHashImpl::apply(
                    level_col->getUInt(i),
                    reinterpret_cast<const char *>(&chars[current_offset]),
                    offsets[i] - current_offset - 1);

                current_offset = offsets[i];
            }

            return col_to;
        }
        if (const auto * col_const_from = checkAndGetColumnConstData<ColumnString>(col_untyped))
        {
            auto col_to = ColumnUInt64::create(size);
            auto & out = col_to->getData();

            const auto & chars = col_const_from->getChars();
            const auto & offsets = col_const_from->getOffsets();

            for (size_t i = 0; i < size; ++i)
            {
                out[i] = URLHierarchyHashImpl::apply(level_col->getUInt(i), reinterpret_cast<const char *>(chars.data()), offsets[0] - 1);
            }

            return col_to;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of argument of function {}", arguments[0].column->getName(), getName());
    }
};

struct ImplWyHash64
{
    static constexpr auto name = "wyHash64";
    using ReturnType = UInt64;

    static UInt64 apply(const char * s, const size_t len) { return wyhash(s, len, 0, _wyp); }
    static UInt64 combineHashes(UInt64 h1, UInt64 h2) { return combineHashesFunc<UInt64, ImplWyHash64>(h1, h2); }

    static constexpr bool use_int_hash_for_pods = false;
};

struct NameIntHash32 { static constexpr auto name = "intHash32"; };
struct NameIntHash64 { static constexpr auto name = "intHash64"; };

using FunctionSipHash64 = FunctionAnyHash<SipHash64Impl>;
using FunctionSipHash64Keyed = FunctionAnyHash<SipHash64KeyedImpl, true, SipHash64KeyedImpl::Key, SipHash64KeyedImpl::KeyColumns>;
using FunctionIntHash32 = FunctionIntHash<IntHash32Impl, NameIntHash32>;
using FunctionIntHash64 = FunctionIntHash<IntHash64Impl, NameIntHash64>;
#if USE_SSL
using FunctionHalfMD5 = FunctionAnyHash<HalfMD5Impl>;
#endif
using FunctionSipHash128 = FunctionAnyHash<SipHash128Impl>;
using FunctionSipHash128Keyed = FunctionAnyHash<SipHash128KeyedImpl, true, SipHash128KeyedImpl::Key, SipHash128KeyedImpl::KeyColumns>;
using FunctionSipHash128Reference = FunctionAnyHash<SipHash128ReferenceImpl>;
using FunctionSipHash128ReferenceKeyed
    = FunctionAnyHash<SipHash128ReferenceKeyedImpl, true, SipHash128ReferenceKeyedImpl::Key, SipHash128ReferenceKeyedImpl::KeyColumns>;
using FunctionCityHash64 = FunctionAnyHash<ImplCityHash64>;
using FunctionFarmFingerprint64 = FunctionAnyHash<ImplFarmFingerprint64>;
using FunctionFarmHash64 = FunctionAnyHash<ImplFarmHash64>;
using FunctionMetroHash64 = FunctionAnyHash<ImplMetroHash64>;

using FunctionMurmurHash2_32 = FunctionAnyHash<MurmurHash2Impl32>;
using FunctionMurmurHash2_64 = FunctionAnyHash<MurmurHash2Impl64>;
using FunctionGccMurmurHash = FunctionAnyHash<GccMurmurHashImpl>;
using FunctionKafkaMurmurHash = FunctionAnyHash<KafkaMurmurHashImpl>;
using FunctionMurmurHash3_32 = FunctionAnyHash<MurmurHash3Impl32>;
using FunctionMurmurHash3_64 = FunctionAnyHash<MurmurHash3Impl64>;
using FunctionMurmurHash3_128 = FunctionAnyHash<MurmurHash3Impl128>;

using FunctionJavaHash = FunctionAnyHash<JavaHashImpl>;
using FunctionJavaHashUTF16LE = FunctionAnyHash<JavaHashUTF16LEImpl>;
using FunctionHiveHash = FunctionAnyHash<HiveHashImpl>;

using FunctionXxHash32 = FunctionAnyHash<ImplXxHash32>;
using FunctionXxHash64 = FunctionAnyHash<ImplXxHash64>;
using FunctionXXH3 = FunctionAnyHash<ImplXXH3>;

using FunctionWyHash64 = FunctionAnyHash<ImplWyHash64>;
}

#pragma clang diagnostic pop
