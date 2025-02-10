#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeFixedString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <base/IPv4andIPv6.h>

#include "config.h"

#if USE_BLAKE3
#    include <llvm/Support/BLAKE3.h>
#endif

#if USE_SSL
#    include <openssl/evp.h>
#    include <openssl/md4.h>
#    include <openssl/md5.h>
#    include <openssl/ripemd.h>
#    include <openssl/sha.h>
#endif

/// Instatiating only the functions that require FunctionStringHashFixedString in a separate file
/// to better parallelize the build procedure and avoid MSan build failure
/// due to excessive resource consumption.

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}


#if USE_SSL

struct MD4Impl
{
    static constexpr auto name = "MD4";
    enum
    {
        length = MD4_DIGEST_LENGTH
    };

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
    enum
    {
        length = MD5_DIGEST_LENGTH
    };

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
    enum
    {
        length = SHA_DIGEST_LENGTH
    };

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
    enum
    {
        length = SHA224_DIGEST_LENGTH
    };

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
    enum
    {
        length = SHA256_DIGEST_LENGTH
    };

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
    enum
    {
        length = SHA384_DIGEST_LENGTH
    };

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
    enum
    {
        length = 64
    };

    static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
    {
        SHA512_CTX ctx;
        SHA512_Init(&ctx);
        SHA512_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
        SHA512_Final(out_char_data, &ctx);
    }
};

struct SHA512Impl256
{
    static constexpr auto name = "SHA512_256";
    enum
    {
        length = 32
    };

    static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
    {
        /// Here, we use the EVP interface that is common to both BoringSSL and OpenSSL. Though BoringSSL is the default
        /// SSL library that we use, for S390X architecture only OpenSSL is supported. But the SHA512-256, SHA512_256_Init,
        /// SHA512_256_Update, SHA512_256_Final methods to calculate hash (similar to the other SHA functions) aren't available
        /// in the current version of OpenSSL that we use which necessitates the use of the EVP interface.
        auto * md_ctx = EVP_MD_CTX_create();
        EVP_DigestInit_ex(md_ctx, EVP_sha512_256(), nullptr /*engine*/);
        EVP_DigestUpdate(md_ctx, begin, size);
        EVP_DigestFinal_ex(md_ctx, out_char_data, nullptr /*size*/);
        EVP_MD_CTX_destroy(md_ctx);
    }
};

struct RIPEMD160Impl
{
    static constexpr auto name = "RIPEMD160";
    enum
    {
        length = RIPEMD160_DIGEST_LENGTH
    };

    static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
    {
        RIPEMD160_CTX ctx;
        RIPEMD160_Init(&ctx);
        RIPEMD160_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
        RIPEMD160_Final(out_char_data, &ctx);
    }
};
#endif

#if USE_BLAKE3
struct ImplBLAKE3
{
    static constexpr auto name = "BLAKE3";
    enum
    {
        length = 32
    };

    static void apply(const char * begin, const size_t size, unsigned char * out_char_data)
    {
        static_assert(LLVM_BLAKE3_OUT_LEN == ImplBLAKE3::length);
        auto & result = *reinterpret_cast<std::array<uint8_t, LLVM_BLAKE3_OUT_LEN> *>(out_char_data);

        llvm::BLAKE3 hasher;
        if (size > 0)
            hasher.update(llvm::StringRef(begin, size));
        hasher.final(result);
    }
};

#endif

template <typename Impl>
class FunctionStringHashFixedString : public IFunction
{
public:
    static constexpr auto name = Impl::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionStringHashFixedString>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isStringOrFixedString(arguments[0]) && !isIPv6(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

        return std::make_shared<DataTypeFixedString>(Impl::length);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        if (const ColumnString * col_from = checkAndGetColumn<ColumnString>(arguments[0].column.get()))
        {
            auto col_to = ColumnFixedString::create(Impl::length);

            const typename ColumnString::Chars & data = col_from->getChars();
            const typename ColumnString::Offsets & offsets = col_from->getOffsets();
            auto & chars_to = col_to->getChars();
            chars_to.resize(input_rows_count * Impl::length);

            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                Impl::apply(
                    reinterpret_cast<const char *>(&data[current_offset]),
                    offsets[i] - current_offset - 1,
                    reinterpret_cast<uint8_t *>(&chars_to[i * Impl::length]));

                current_offset = offsets[i];
            }

            return col_to;
        }
        if (const ColumnFixedString * col_from_fix = checkAndGetColumn<ColumnFixedString>(arguments[0].column.get()))
        {
            auto col_to = ColumnFixedString::create(Impl::length);
            const typename ColumnFixedString::Chars & data = col_from_fix->getChars();
            auto & chars_to = col_to->getChars();
            const auto length = col_from_fix->getN();
            chars_to.resize(input_rows_count * Impl::length);
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                Impl::apply(
                    reinterpret_cast<const char *>(&data[i * length]), length, reinterpret_cast<uint8_t *>(&chars_to[i * Impl::length]));
            }
            return col_to;
        }
        if (const ColumnIPv6 * col_from_ip = checkAndGetColumn<ColumnIPv6>(arguments[0].column.get()))
        {
            auto col_to = ColumnFixedString::create(Impl::length);
            const typename ColumnIPv6::Container & data = col_from_ip->getData();
            auto & chars_to = col_to->getChars();
            const auto length = sizeof(IPv6::UnderlyingType);
            chars_to.resize(input_rows_count * Impl::length);
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                Impl::apply(reinterpret_cast<const char *>(&data[i]), length, reinterpret_cast<uint8_t *>(&chars_to[i * Impl::length]));
            }
            return col_to;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", arguments[0].column->getName(), getName());
    }
};

#if USE_SSL || USE_BLAKE3
REGISTER_FUNCTION(HashFixedStrings)
{
#    if USE_SSL
    using FunctionMD4 = FunctionStringHashFixedString<MD4Impl>;
    using FunctionMD5 = FunctionStringHashFixedString<MD5Impl>;
    using FunctionSHA1 = FunctionStringHashFixedString<SHA1Impl>;
    using FunctionSHA224 = FunctionStringHashFixedString<SHA224Impl>;
    using FunctionSHA256 = FunctionStringHashFixedString<SHA256Impl>;
    using FunctionSHA384 = FunctionStringHashFixedString<SHA384Impl>;
    using FunctionSHA512 = FunctionStringHashFixedString<SHA512Impl>;
    using FunctionSHA512_256 = FunctionStringHashFixedString<SHA512Impl256>;
    using FunctionRIPEMD160 = FunctionStringHashFixedString<RIPEMD160Impl>;

    factory.registerFunction<FunctionRIPEMD160>(FunctionDocumentation{
        .description = R"(Calculates the RIPEMD-160 hash of the given string.)",
        .syntax = "SELECT RIPEMD160(s);",
        .arguments = {{"s", "The input [String](../../sql-reference/data-types/string.md)."}},
        .returned_value
        = "The RIPEMD160 hash of the given input string returned as a [FixedString(20)](../../sql-reference/data-types/fixedstring.md).",
        .examples
        = {{"",
            "SELECT HEX(RIPEMD160('The quick brown fox jumps over the lazy dog'));",
            R"(
┌─HEX(RIPEMD160('The quick brown fox jumps over the lazy dog'))─┐
│ 37F332F68DB77BD9D7EDD4969571AD671CF9DD3B                      │
└───────────────────────────────────────────────────────────────┘
         )"}}});
    factory.registerFunction<FunctionMD4>(FunctionDocumentation{
        .description = R"(Calculates the MD4 hash of the given string.)",
        .syntax = "SELECT MD4(s);",
        .arguments = {{"s", "The input [String](../../sql-reference/data-types/string.md)."}},
        .returned_value
        = "The MD4 hash of the given input string returned as a [FixedString(16)](../../sql-reference/data-types/fixedstring.md).",
        .examples
        = {{"",
            "SELECT HEX(MD4('abc'));",
            R"(
┌─hex(MD4('abc'))──────────────────┐
│ A448017AAF21D8525FC10AE87AA6729D │
└──────────────────────────────────┘
            )"}}});
    factory.registerFunction<FunctionMD5>(FunctionDocumentation{
        .description = R"(Calculates the MD5 hash of the given string.)",
        .syntax = "SELECT MD5(s);",
        .arguments = {{"s", "The input [String](../../sql-reference/data-types/string.md)."}},
        .returned_value
        = "The MD5 hash of the given input string returned as a [FixedString(16)](../../sql-reference/data-types/fixedstring.md).",
        .examples
        = {{"",
            "SELECT HEX(MD5('abc'));",
            R"(
┌─hex(MD5('abc'))──────────────────┐
│ 900150983CD24FB0D6963F7D28E17F72 │
└──────────────────────────────────┘
            )"}}});
    factory.registerFunction<FunctionSHA1>(FunctionDocumentation{
        .description = R"(Calculates the SHA1 hash of the given string.)",
        .syntax = "SELECT SHA1(s);",
        .arguments = {{"s", "The input [String](../../sql-reference/data-types/string.md)."}},
        .returned_value
        = "The SHA1 hash of the given input string returned as a [FixedString](../../sql-reference/data-types/fixedstring.md).",
        .examples
        = {{"",
            "SELECT HEX(SHA1('abc'));",
            R"(
┌─hex(SHA1('abc'))─────────────────────────┐
│ A9993E364706816ABA3E25717850C26C9CD0D89D │
└──────────────────────────────────────────┘
            )"}}});
    factory.registerFunction<FunctionSHA224>(FunctionDocumentation{
        .description = R"(Calculates the SHA224 hash of the given string.)",
        .syntax = "SELECT SHA224(s);",
        .arguments = {{"s", "The input [String](../../sql-reference/data-types/string.md)."}},
        .returned_value
        = "The SHA224 hash of the given input string returned as a [FixedString](../../sql-reference/data-types/fixedstring.md).",
        .examples
        = {{"",
            "SELECT HEX(SHA224('abc'));",
            R"(
┌─hex(SHA224('abc'))───────────────────────────────────────┐
│ 23097D223405D8228642A477BDA255B32AADBCE4BDA0B3F7E36C9DA7 │
└──────────────────────────────────────────────────────────┘
            )"}}});
    factory.registerFunction<FunctionSHA256>(FunctionDocumentation{
        .description = R"(Calculates the SHA256 hash of the given string.)",
        .syntax = "SELECT SHA256(s);",
        .arguments = {{"s", "The input [String](../../sql-reference/data-types/string.md)."}},
        .returned_value
        = "The SHA256 hash of the given input string returned as a [FixedString](../../sql-reference/data-types/fixedstring.md).",
        .examples
        = {{"",
            "SELECT HEX(SHA256('abc'));",
            R"(
┌─hex(SHA256('abc'))───────────────────────────────────────────────┐
│ BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD │
└──────────────────────────────────────────────────────────────────┘
            )"}}});
    factory.registerFunction<FunctionSHA384>(FunctionDocumentation{
        .description = R"(Calculates the SHA384 hash of the given string.)",
        .syntax = "SELECT SHA384(s);",
        .arguments = {{"s", "The input [String](../../sql-reference/data-types/string.md)."}},
        .returned_value
        = "The SHA384 hash of the given input string returned as a [FixedString](../../sql-reference/data-types/fixedstring.md).",
        .examples
        = {{"",
            "SELECT HEX(SHA384('abc'));",
            R"(
┌─hex(SHA384('abc'))───────────────────────────────────────────────────────────────────────────────┐
│ CB00753F45A35E8BB5A03D699AC65007272C32AB0EDED1631A8B605A43FF5BED8086072BA1E7CC2358BAECA134C825A7 │
└──────────────────────────────────────────────────────────────────────────────────────────────────┘
            )"}}});
    factory.registerFunction<FunctionSHA512>(FunctionDocumentation{
        .description = R"(Calculates the SHA512 hash of the given string.)",
        .syntax = "SELECT SHA512(s);",
        .arguments = {{"s", "The input [String](../../sql-reference/data-types/string.md)."}},
        .returned_value
        = "The SHA512 hash of the given input string returned as a [FixedString](../../sql-reference/data-types/fixedstring.md).",
        .examples
        = {{"",
            "SELECT HEX(SHA512('abc'));",
            R"(
┌─hex(SHA512('abc'))───────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ DDAF35A193617ABACC417349AE20413112E6FA4E89A97EA20A9EEEE64B55D39A2192992A274FC1A836BA3C23A3FEEBBD454D4423643CE80E2A9AC94FA54CA49F │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
            )"}}});
    factory.registerFunction<FunctionSHA512_256>(FunctionDocumentation{
        .description = R"(Calculates the SHA512_256 hash of the given string.)",
        .syntax = "SELECT SHA512_256(s);",
        .arguments = {{"s", "The input [String](../../sql-reference/data-types/string.md)."}},
        .returned_value
        = "The SHA512_256 hash of the given input string returned as a [FixedString](../../sql-reference/data-types/fixedstring.md).",
        .examples
        = {{"",
            "SELECT HEX(SHA512_256('abc'));",
            R"(
┌─hex(SHA512_256('abc'))───────────────────────────────────────────┐
│ 53048E2681941EF99B2E29B76B4C7DABE4C2D0C634FC6D46E0E2F13107E7AF23 │
└──────────────────────────────────────────────────────────────────┘
            )"}}});


#    endif

#    if USE_BLAKE3
    using FunctionBLAKE3 = FunctionStringHashFixedString<ImplBLAKE3>;
    factory.registerFunction<FunctionBLAKE3>(FunctionDocumentation{
        .description = R"(
    Calculates BLAKE3 hash string and returns the resulting set of bytes as FixedString.
    This cryptographic hash-function is integrated into ClickHouse with BLAKE3 Rust library.
    The function is rather fast and shows approximately two times faster performance compared to SHA-2, while generating hashes of the same length as SHA-256.
    It returns a BLAKE3 hash as a byte array with type FixedString(32).
    )",
        .examples{{"hash", "SELECT hex(BLAKE3('ABC'))", ""}},
        .categories{"Hash"}});
#    endif
}
#endif
}
