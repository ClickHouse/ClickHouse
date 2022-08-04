#pragma once

#include <city.h>
#include <farmhash.h>
#include <metrohash.h>
#include <MurmurHash2.h>
#include <MurmurHash3.h>
#include <wyhash.h>

#include "config_functions.h"
#include "config_core.h"

#if USE_BLAKE3
#    include <blake3.h>
#endif

#include <Common/SipHash.h>
#include <Common/typeid_cast.h>
#include <Common/HashTable/Hash.h>
#include <xxhash.h>

#if USE_SSL
#    include <openssl/md4.h>
#    include <openssl/md5.h>
#    include <openssl/sha.h>
#endif

#include <Poco/ByteOrder.h>
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
#include <base/range.h>
#include <base/bit_cast.h>


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
    T hashes[] = {t1, t2};
    return HashFunction::apply(reinterpret_cast<const char *>(hashes), 2 * sizeof(T));
}


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

        return Poco::ByteOrder::flipBytes(static_cast<Poco::UInt64>(buf.uint64_data));        /// Compatibility with existing code. Cast need for old poco AND macos where UInt64 != uint64_t
    }

    static UInt64 combineHashes(UInt64 h1, UInt64 h2)
    {
        UInt64 hashes[] = {h1, h2};
        return apply(reinterpret_cast<const char *>(hashes), 16);
    }

    /// If true, it will use intHash32 or intHash64 to hash POD types. This behaviour is intended for better performance of some functions.
    /// Otherwise it will hash bytes in memory as a string using corresponding hash function.

    static constexpr bool use_int_hash_for_pods = false;
};

struct MD4Impl
{
    static constexpr auto name = "MD4";
    enum { length = MD4_DIGEST_LENGTH };

    static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
    {
        MD4_CTX ctx;
        MD4_Init(&ctx);
        MD4_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
        MD4_Final(out_char_data, &ctx);
    }
};

struct MD5Impl
{
    static constexpr auto name = "MD5";
    enum { length = MD5_DIGEST_LENGTH };

    static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
    {
        MD5_CTX ctx;
        MD5_Init(&ctx);
        MD5_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
        MD5_Final(out_char_data, &ctx);
    }
};

struct SHA1Impl
{
    static constexpr auto name = "SHA1";
    enum { length = SHA_DIGEST_LENGTH };

    static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
    {
        SHA_CTX ctx;
        SHA1_Init(&ctx);
        SHA1_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
        SHA1_Final(out_char_data, &ctx);
    }
};

struct SHA224Impl
{
    static constexpr auto name = "SHA224";
    enum { length = SHA224_DIGEST_LENGTH };

    static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
    {
        SHA256_CTX ctx;
        SHA224_Init(&ctx);
        SHA224_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
        SHA224_Final(out_char_data, &ctx);
    }
};

struct SHA256Impl
{
    static constexpr auto name = "SHA256";
    enum { length = SHA256_DIGEST_LENGTH };

    static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
    {
        SHA256_CTX ctx;
        SHA256_Init(&ctx);
        SHA256_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
        SHA256_Final(out_char_data, &ctx);
    }
};

struct SHA384Impl
{
    static constexpr auto name = "SHA384";
    enum { length = SHA384_DIGEST_LENGTH };

    static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
    {
        SHA512_CTX ctx;
        SHA384_Init(&ctx);
        SHA384_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
        SHA384_Final(out_char_data, &ctx);
    }
};

struct SHA512Impl
{
    static constexpr auto name = "SHA512";
    enum { length = 64 };

    static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
    {
        SHA512_CTX ctx;
        SHA512_Init(&ctx);
        SHA512_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
        SHA512_Final(out_char_data, &ctx);
    }
};
#endif

struct SipHash64Impl
{
    static constexpr auto name = "sipHash64";
    using ReturnType = UInt64;

    static UInt64 apply(const char * begin, size_t size)
    {
        return sipHash64(begin, size);
    }

    static UInt64 combineHashes(UInt64 h1, UInt64 h2)
    {
        return combineHashesFunc<UInt64, SipHash64Impl>(h1, h2);
    }

    static constexpr bool use_int_hash_for_pods = false;
};

struct SipHash128Impl
{
    static constexpr auto name = "sipHash128";

    using ReturnType = UInt128;

    static UInt128 combineHashes(UInt128 h1, UInt128 h2)
    {
        return combineHashesFunc<UInt128, SipHash128Impl>(h1, h2);
    }

    static UInt128 apply(const char * data, const size_t size)
    {
        return sipHash128(data, size);
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

    static UInt64 combineHashes(UInt64 h1, UInt64 h2)
    {
        return IntHash64Impl::apply(h1) ^ h2;
    }

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

    static UInt128 combineHashes(UInt128 h1, UInt128 h2)
    {
        return combineHashesFunc<UInt128, MurmurHash3Impl128>(h1, h2);
    }

    static constexpr bool use_int_hash_for_pods = false;
};

/// http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452
/// Care should be taken to do all calculation in unsigned integers (to avoid undefined behaviour on overflow)
///  but obtain the same result as it is done in signed integers with two's complement arithmetic.
struct JavaHashImpl
{
    static constexpr auto name = "javaHash";
    using ReturnType = Int32;

    static Int32 apply(const char * data, const size_t size)
    {
        UInt32 h = 0;
        for (size_t i = 0; i < size; ++i)
            h = 31 * h + static_cast<UInt32>(static_cast<Int8>(data[i]));
        return static_cast<Int32>(h);
    }

    static Int32 combineHashes(Int32, Int32)
    {
        throw Exception("Java hash is not combineable for multiple arguments", ErrorCodes::NOT_IMPLEMENTED);
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
            throw Exception("Arguments for javaHashUTF16LE must be in the form of UTF-16", ErrorCodes::BAD_ARGUMENTS);

        UInt32 h = 0;
        for (size_t i = 0; i < size; i += 2)
            h = 31 * h + static_cast<UInt16>(static_cast<UInt8>(data[i]) | static_cast<UInt8>(data[i + 1]) << 8);

        return static_cast<Int32>(h);
    }

    static Int32 combineHashes(Int32, Int32)
    {
        throw Exception("Java hash is not combineable for multiple arguments", ErrorCodes::NOT_IMPLEMENTED);
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
        throw Exception("Hive hash is not combineable for multiple arguments", ErrorCodes::NOT_IMPLEMENTED);
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

    static auto apply(const char * s, const size_t len) { return XXH32(s, len, 0); }
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

    static auto apply(const char * s, const size_t len) { return XXH64(s, len, 0); }

    /*
       With current implementation with more than 1 arguments it will give the results
       non-reproducible from outside of CH. (see comment on ImplXxHash32).
     */
    static auto combineHashes(UInt64 h1, UInt64 h2) { return CityHash_v1_0_2::Hash128to64(uint128_t(h1, h2)); }

    static constexpr bool use_int_hash_for_pods = false;
};


#if USE_BLAKE3
struct ImplBLAKE3
{
    static constexpr auto name = "BLAKE3";
    enum { length = OUT_LEN };

    static void apply(const char * begin, const size_t size, unsigned char* out_char_data)
    {
        #if defined(MEMORY_SANITIZER)
            auto err_msg = blake3_apply_shim_msan_compat(begin, size, out_char_data);
        #else
            auto err_msg = blake3_apply_shim(begin, size, out_char_data);
        #endif
        if (err_msg != nullptr)
        {
            throw Exception("Function returned error message: " + std::string(err_msg), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            blake3_free_char_pointer(err_msg);
        }
    }
};
#endif


template <typename Impl>
class FunctionStringHashFixedString : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionStringHashFixedString>(); }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]))
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeFixedString>(Impl::length);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        if (const ColumnString * col_from = checkAndGetColumn<ColumnString>(arguments[0].column.get()))
        {
            auto col_to = ColumnFixedString::create(Impl::length);

            const typename ColumnString::Chars & data = col_from->getChars();
            const typename ColumnString::Offsets & offsets = col_from->getOffsets();
            auto & chars_to = col_to->getChars();
            const auto size = offsets.size();
            chars_to.resize(size * Impl::length);

            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                Impl::apply(
                    reinterpret_cast<const char *>(&data[current_offset]),
                    offsets[i] - current_offset - 1,
                    reinterpret_cast<uint8_t *>(&chars_to[i * Impl::length]));

                current_offset = offsets[i];
            }

            return col_to;
        }
        else if (
            const ColumnFixedString * col_from_fix = checkAndGetColumn<ColumnFixedString>(arguments[0].column.get()))
        {
            auto col_to = ColumnFixedString::create(Impl::length);
            const typename ColumnFixedString::Chars & data = col_from_fix->getChars();
            const auto size = col_from_fix->size();
            auto & chars_to = col_to->getChars();
            const auto length = col_from_fix->getN();
            chars_to.resize(size * Impl::length);
            for (size_t i = 0; i < size; ++i)
            {
                Impl::apply(
                    reinterpret_cast<const char *>(&data[i * length]), length, reinterpret_cast<uint8_t *>(&chars_to[i * Impl::length]));
            }
            return col_to;
        }
        else
            throw Exception("Illegal column " + arguments[0].column->getName()
                    + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
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
        else
            throw Exception("Illegal column " + arguments[0].column->getName()
                    + " of first argument of function " + Name::name,
                ErrorCodes::ILLEGAL_COLUMN);
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
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

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
        else if (which.isUInt16())
            return executeType<UInt16>(arguments);
        else if (which.isUInt32())
            return executeType<UInt32>(arguments);
        else if (which.isUInt64())
            return executeType<UInt64>(arguments);
        else if (which.isInt8())
            return executeType<Int8>(arguments);
        else if (which.isInt16())
            return executeType<Int16>(arguments);
        else if (which.isInt32())
            return executeType<Int32>(arguments);
        else if (which.isInt64())
            return executeType<Int64>(arguments);
        else if (which.isDate())
            return executeType<UInt16>(arguments);
        else if (which.isDate32())
            return executeType<Int32>(arguments);
        else if (which.isDateTime())
            return executeType<UInt32>(arguments);
        else if (which.isDecimal32())
            return executeType<Decimal32>(arguments);
        else if (which.isDecimal64())
            return executeType<Decimal64>(arguments);
        else
            throw Exception("Illegal type " + arguments[0].type->getName() + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
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

template <typename Impl>
class FunctionAnyHash : public IFunction
{
public:
    static constexpr auto name = Impl::name;

private:
    using ToType = typename Impl::ReturnType;

    template <typename FromType, bool first>
    void executeIntType(const IColumn * column, typename ColumnVector<ToType>::Container & vec_to) const
    {
        using ColVecType = ColumnVectorOrDecimal<FromType>;

        if (const ColVecType * col_from = checkAndGetColumn<ColVecType>(column))
        {
            const typename ColVecType::Container & vec_from = col_from->getData();
            size_t size = vec_from.size();
            for (size_t i = 0; i < size; ++i)
            {
                ToType h;

                if constexpr (Impl::use_int_hash_for_pods)
                {
                    if constexpr (std::is_same_v<ToType, UInt64>)
                        h = IntHash64Impl::apply(bit_cast<UInt64>(vec_from[i]));
                    else
                        h = IntHash32Impl::apply(bit_cast<UInt32>(vec_from[i]));
                }
                else
                {
                    h = Impl::apply(reinterpret_cast<const char *>(&vec_from[i]), sizeof(vec_from[i]));
                }

                if constexpr (first)
                    vec_to[i] = h;
                else
                    vec_to[i] = Impl::combineHashes(vec_to[i], h);
            }
        }
        else if (auto col_from_const = checkAndGetColumnConst<ColVecType>(column))
        {
            auto value = col_from_const->template getValue<FromType>();
            ToType hash;
            if constexpr (std::is_same_v<ToType, UInt64>)
                hash = IntHash64Impl::apply(bit_cast<UInt64>(value));
            else
                hash = IntHash32Impl::apply(bit_cast<UInt32>(value));

            size_t size = vec_to.size();
            if constexpr (first)
            {
                vec_to.assign(size, hash);
            }
            else
            {
                for (size_t i = 0; i < size; ++i)
                    vec_to[i] = Impl::combineHashes(vec_to[i], hash);
            }
        }
        else
            throw Exception("Illegal column " + column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    template <typename FromType, bool first>
    void executeBigIntType(const IColumn * column, typename ColumnVector<ToType>::Container & vec_to) const
    {
        using ColVecType = ColumnVectorOrDecimal<FromType>;

        if (const ColVecType * col_from = checkAndGetColumn<ColVecType>(column))
        {
            const typename ColVecType::Container & vec_from = col_from->getData();
            size_t size = vec_from.size();
            for (size_t i = 0; i < size; ++i)
            {
                ToType h = Impl::apply(reinterpret_cast<const char *>(&vec_from[i]), sizeof(vec_from[i]));

                if constexpr (first)
                    vec_to[i] = h;
                else
                    vec_to[i] = Impl::combineHashes(vec_to[i], h);
            }
        }
        else if (auto col_from_const = checkAndGetColumnConst<ColVecType>(column))
        {
            auto value = col_from_const->template getValue<FromType>();

            ToType h = Impl::apply(reinterpret_cast<const char *>(&value), sizeof(value));

            size_t size = vec_to.size();
            if constexpr (first)
            {
                vec_to.assign(size, h);
            }
            else
            {
                for (size_t i = 0; i < size; ++i)
                    vec_to[i] = Impl::combineHashes(vec_to[i], h);
            }
        }
        else
            throw Exception("Illegal column " + column->getName()
                + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    template <bool first>
    void executeGeneric(const IColumn * column, typename ColumnVector<ToType>::Container & vec_to) const
    {
        for (size_t i = 0, size = column->size(); i < size; ++i)
        {
            StringRef bytes = column->getDataAt(i);
            const ToType h = Impl::apply(bytes.data, bytes.size);
            if constexpr (first)
                vec_to[i] = h;
            else
                vec_to[i] = Impl::combineHashes(vec_to[i], h);
        }
    }

    template <bool first>
    void executeString(const IColumn * column, typename ColumnVector<ToType>::Container & vec_to) const
    {
        if (const ColumnString * col_from = checkAndGetColumn<ColumnString>(column))
        {
            const typename ColumnString::Chars & data = col_from->getChars();
            const typename ColumnString::Offsets & offsets = col_from->getOffsets();
            size_t size = offsets.size();

            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                const ToType h = Impl::apply(
                    reinterpret_cast<const char *>(&data[current_offset]),
                    offsets[i] - current_offset - 1);

                if constexpr (first)
                    vec_to[i] = h;
                else
                    vec_to[i] = Impl::combineHashes(vec_to[i], h);

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
                const ToType h = Impl::apply(reinterpret_cast<const char *>(&data[i * n]), n);
                if constexpr (first)
                    vec_to[i] = h;
                else
                    vec_to[i] = Impl::combineHashes(vec_to[i], h);
            }
        }
        else if (const ColumnConst * col_from_const = checkAndGetColumnConstStringOrFixedString(column))
        {
            String value = col_from_const->getValue<String>();
            const ToType hash = Impl::apply(value.data(), value.size());
            const size_t size = vec_to.size();

            if constexpr (first)
            {
                vec_to.assign(size, hash);
            }
            else
            {
                for (size_t i = 0; i < size; ++i)
                {
                    vec_to[i] = Impl::combineHashes(vec_to[i], hash);
                }
            }
        }
        else
            throw Exception("Illegal column " + column->getName()
                    + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    template <bool first>
    void executeArray(const IDataType * type, const IColumn * column, typename ColumnVector<ToType>::Container & vec_to) const
    {
        const IDataType * nested_type = typeid_cast<const DataTypeArray *>(type)->getNestedType().get();

        if (const ColumnArray * col_from = checkAndGetColumn<ColumnArray>(column))
        {
            const IColumn * nested_column = &col_from->getData();
            const ColumnArray::Offsets & offsets = col_from->getOffsets();
            const size_t nested_size = nested_column->size();

            typename ColumnVector<ToType>::Container vec_temp(nested_size);
            bool nested_is_first = true;
            executeForArgument(nested_type, nested_column, vec_temp, nested_is_first);

            const size_t size = offsets.size();

            ColumnArray::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                ColumnArray::Offset next_offset = offsets[i];

                ToType h;
                if constexpr (std::is_same_v<ToType, UInt64>)
                    h = IntHash64Impl::apply(next_offset - current_offset);
                else
                    h = IntHash32Impl::apply(next_offset - current_offset);

                if constexpr (first)
                    vec_to[i] = h;
                else
                    vec_to[i] = Impl::combineHashes(vec_to[i], h);

                for (size_t j = current_offset; j < next_offset; ++j)
                    vec_to[i] = Impl::combineHashes(vec_to[i], vec_temp[j]);

                current_offset = offsets[i];
            }
        }
        else if (const ColumnConst * col_from_const = checkAndGetColumnConst<ColumnArray>(column))
        {
            /// NOTE: here, of course, you can do without the materialization of the column.
            ColumnPtr full_column = col_from_const->convertToFullColumn();
            executeArray<first>(type, &*full_column, vec_to);
        }
        else
            throw Exception("Illegal column " + column->getName()
                    + " of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }

    template <bool first>
    void executeAny(const IDataType * from_type, const IColumn * icolumn, typename ColumnVector<ToType>::Container & vec_to) const
    {
        WhichDataType which(from_type);

        if      (which.isUInt8()) executeIntType<UInt8, first>(icolumn, vec_to);
        else if (which.isUInt16()) executeIntType<UInt16, first>(icolumn, vec_to);
        else if (which.isUInt32()) executeIntType<UInt32, first>(icolumn, vec_to);
        else if (which.isUInt64()) executeIntType<UInt64, first>(icolumn, vec_to);
        else if (which.isUInt128()) executeBigIntType<UInt128, first>(icolumn, vec_to);
        else if (which.isUInt256()) executeBigIntType<UInt256, first>(icolumn, vec_to);
        else if (which.isInt8()) executeIntType<Int8, first>(icolumn, vec_to);
        else if (which.isInt16()) executeIntType<Int16, first>(icolumn, vec_to);
        else if (which.isInt32()) executeIntType<Int32, first>(icolumn, vec_to);
        else if (which.isInt64()) executeIntType<Int64, first>(icolumn, vec_to);
        else if (which.isInt128()) executeBigIntType<Int128, first>(icolumn, vec_to);
        else if (which.isInt256()) executeBigIntType<Int256, first>(icolumn, vec_to);
        else if (which.isUUID()) executeBigIntType<UUID, first>(icolumn, vec_to);
        else if (which.isEnum8()) executeIntType<Int8, first>(icolumn, vec_to);
        else if (which.isEnum16()) executeIntType<Int16, first>(icolumn, vec_to);
        else if (which.isDate()) executeIntType<UInt16, first>(icolumn, vec_to);
        else if (which.isDate32()) executeIntType<Int32, first>(icolumn, vec_to);
        else if (which.isDateTime()) executeIntType<UInt32, first>(icolumn, vec_to);
        /// TODO: executeIntType() for Decimal32/64 leads to incompatible result
        else if (which.isDecimal32()) executeBigIntType<Decimal32, first>(icolumn, vec_to);
        else if (which.isDecimal64()) executeBigIntType<Decimal64, first>(icolumn, vec_to);
        else if (which.isDecimal128()) executeBigIntType<Decimal128, first>(icolumn, vec_to);
        else if (which.isDecimal256()) executeBigIntType<Decimal256, first>(icolumn, vec_to);
        else if (which.isFloat32()) executeIntType<Float32, first>(icolumn, vec_to);
        else if (which.isFloat64()) executeIntType<Float64, first>(icolumn, vec_to);
        else if (which.isString()) executeString<first>(icolumn, vec_to);
        else if (which.isFixedString()) executeString<first>(icolumn, vec_to);
        else if (which.isArray()) executeArray<first>(from_type, icolumn, vec_to);
        else executeGeneric<first>(icolumn, vec_to);
    }

    void executeForArgument(const IDataType * type, const IColumn * column, typename ColumnVector<ToType>::Container & vec_to, bool & is_first) const
    {
        /// Flattening of tuples.
        if (const ColumnTuple * tuple = typeid_cast<const ColumnTuple *>(column))
        {
            const auto & tuple_columns = tuple->getColumns();
            const DataTypes & tuple_types = typeid_cast<const DataTypeTuple &>(*type).getElements();
            size_t tuple_size = tuple_columns.size();
            for (size_t i = 0; i < tuple_size; ++i)
                executeForArgument(tuple_types[i].get(), tuple_columns[i].get(), vec_to, is_first);
        }
        else if (const ColumnTuple * tuple_const = checkAndGetColumnConstData<ColumnTuple>(column))
        {
            const auto & tuple_columns = tuple_const->getColumns();
            const DataTypes & tuple_types = typeid_cast<const DataTypeTuple &>(*type).getElements();
            size_t tuple_size = tuple_columns.size();
            for (size_t i = 0; i < tuple_size; ++i)
            {
                auto tmp = ColumnConst::create(tuple_columns[i], column->size());
                executeForArgument(tuple_types[i].get(), tmp.get(), vec_to, is_first);
            }
        }
        else if (const auto * map = checkAndGetColumn<ColumnMap>(column))
        {
            const auto & type_map = assert_cast<const DataTypeMap &>(*type);
            executeForArgument(type_map.getNestedType().get(), map->getNestedColumnPtr().get(), vec_to, is_first);
        }
        else if (const auto * const_map = checkAndGetColumnConstData<ColumnMap>(column))
        {
            const auto & type_map = assert_cast<const DataTypeMap &>(*type);
            executeForArgument(type_map.getNestedType().get(), const_map->getNestedColumnPtr().get(), vec_to, is_first);
        }
        else
        {
            if (is_first)
                executeAny<true>(type, column, vec_to);
            else
                executeAny<false>(type, column, vec_to);
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

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        size_t rows = input_rows_count;
        auto col_to = ColumnVector<ToType>::create(rows);

        typename ColumnVector<ToType>::Container & vec_to = col_to->getData();

        if (arguments.empty())
        {
            /// Constant random number from /dev/urandom is used as a hash value of empty list of arguments.
            vec_to.assign(rows, static_cast<ToType>(0xe28dbde7fe22e41c));
        }

        /// The function supports arbitrary number of arguments of arbitrary types.

        bool is_first_argument = true;
        for (const auto & col : arguments)
            executeForArgument(col.type.get(), col.column.get(), vec_to, is_first_argument);

        if constexpr (std::is_same_v<ToType, UInt128>) /// backward-compatible
        {
            auto col_to_fixed_string = ColumnFixedString::create(sizeof(UInt128));
            col_to_fixed_string->getChars() = std::move(*reinterpret_cast<ColumnFixedString::Chars *>(&col_to->getData()));
            return col_to_fixed_string;
        }

        return col_to;
    }
};

) // DECLARE_MULTITARGET_CODE

template <typename Impl>
class FunctionAnyHash : public TargetSpecific::Default::FunctionAnyHash<Impl>
{
public:
    explicit FunctionAnyHash(ContextPtr context) : selector(context)
    {
        selector.registerImplementation<TargetArch::Default,
            TargetSpecific::Default::FunctionAnyHash<Impl>>();

    #if USE_MULTITARGET_CODE
        selector.registerImplementation<TargetArch::AVX2,
            TargetSpecific::AVX2::FunctionAnyHash<Impl>>();
        selector.registerImplementation<TargetArch::AVX512F,
            TargetSpecific::AVX512F::FunctionAnyHash<Impl>>();
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
        *    (http, file - fit, mailto, magnet - do not fit), and after two slashes there is still something
        *    For the rest, simply return the full URL as the only element of the hierarchy.
        */
        if (pos == begin || pos == end || !(*pos++ == ':' && pos < end && *pos++ == '/' && pos < end && *pos++ == '/' && pos < end))
        {
            pos = end;
            return 0 == level ? pos - begin : 0;
        }

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
            throw Exception{"Number of arguments for function " + getName() + " doesn't match: passed " +
                toString(arg_count) + ", should be 1 or 2.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        const auto * first_arg = arguments.front().get();
        if (!WhichDataType(first_arg).isString())
            throw Exception{"Illegal type " + first_arg->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (arg_count == 2)
        {
            const auto & second_arg = arguments.back();
            if (!isInteger(second_arg))
                throw Exception{"Illegal type " + second_arg->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return std::make_shared<DataTypeUInt64>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const auto arg_count = arguments.size();

        if (arg_count == 1)
            return executeSingleArg(arguments);
        else if (arg_count == 2)
            return executeTwoArgs(arguments);
        else
            throw Exception{"got into IFunction::execute with unexpected number of arguments", ErrorCodes::LOGICAL_ERROR};
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
        else
            throw Exception{"Illegal column " + arguments[0].column->getName() +
                " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
    }

    ColumnPtr executeTwoArgs(const ColumnsWithTypeAndName & arguments) const
    {
        const auto * level_col = arguments.back().column.get();
        if (!isColumnConst(*level_col))
            throw Exception{"Second argument of function " + getName() + " must be an integral constant", ErrorCodes::ILLEGAL_COLUMN};

        const auto level = level_col->get64(0);

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
                out[i] = URLHierarchyHashImpl::apply(
                    level,
                    reinterpret_cast<const char *>(&chars[current_offset]),
                    offsets[i] - current_offset - 1);

                current_offset = offsets[i];
            }

            return col_to;
        }
        else
            throw Exception{"Illegal column " + arguments[0].column->getName() +
                " of argument of function " + getName(), ErrorCodes::ILLEGAL_COLUMN};
    }
};

struct ImplWyHash64
{
    static constexpr auto name = "wyHash64";
    using ReturnType = UInt64;

    static UInt64 apply(const char * s, const size_t len)
    {
        return wyhash(s, len, 0, _wyp);
    }
    static UInt64 combineHashes(UInt64 h1, UInt64 h2)
    {
        union
        {
            UInt64 u64[2];
            char chars[16];
        };
        u64[0] = h1;
        u64[1] = h2;
        return apply(chars, 16);
    }

    static constexpr bool use_int_hash_for_pods = false;
};

struct NameIntHash32 { static constexpr auto name = "intHash32"; };
struct NameIntHash64 { static constexpr auto name = "intHash64"; };

using FunctionSipHash64 = FunctionAnyHash<SipHash64Impl>;
using FunctionIntHash32 = FunctionIntHash<IntHash32Impl, NameIntHash32>;
using FunctionIntHash64 = FunctionIntHash<IntHash64Impl, NameIntHash64>;
#if USE_SSL
using FunctionMD4 = FunctionStringHashFixedString<MD4Impl>;
using FunctionHalfMD5 = FunctionAnyHash<HalfMD5Impl>;
using FunctionMD5 = FunctionStringHashFixedString<MD5Impl>;
using FunctionSHA1 = FunctionStringHashFixedString<SHA1Impl>;
using FunctionSHA224 = FunctionStringHashFixedString<SHA224Impl>;
using FunctionSHA256 = FunctionStringHashFixedString<SHA256Impl>;
using FunctionSHA384 = FunctionStringHashFixedString<SHA384Impl>;
using FunctionSHA512 = FunctionStringHashFixedString<SHA512Impl>;
#endif
using FunctionSipHash128 = FunctionAnyHash<SipHash128Impl>;
using FunctionCityHash64 = FunctionAnyHash<ImplCityHash64>;
using FunctionFarmFingerprint64 = FunctionAnyHash<ImplFarmFingerprint64>;
using FunctionFarmHash64 = FunctionAnyHash<ImplFarmHash64>;
using FunctionMetroHash64 = FunctionAnyHash<ImplMetroHash64>;

using FunctionMurmurHash2_32 = FunctionAnyHash<MurmurHash2Impl32>;
using FunctionMurmurHash2_64 = FunctionAnyHash<MurmurHash2Impl64>;
using FunctionGccMurmurHash = FunctionAnyHash<GccMurmurHashImpl>;
using FunctionMurmurHash3_32 = FunctionAnyHash<MurmurHash3Impl32>;
using FunctionMurmurHash3_64 = FunctionAnyHash<MurmurHash3Impl64>;
using FunctionMurmurHash3_128 = FunctionAnyHash<MurmurHash3Impl128>;

using FunctionJavaHash = FunctionAnyHash<JavaHashImpl>;
using FunctionJavaHashUTF16LE = FunctionAnyHash<JavaHashUTF16LEImpl>;
using FunctionHiveHash = FunctionAnyHash<HiveHashImpl>;

using FunctionXxHash32 = FunctionAnyHash<ImplXxHash32>;
using FunctionXxHash64 = FunctionAnyHash<ImplXxHash64>;

using FunctionWyHash64 = FunctionAnyHash<ImplWyHash64>;

#if USE_BLAKE3
    using FunctionBLAKE3 = FunctionStringHashFixedString<ImplBLAKE3>;
#endif

}
