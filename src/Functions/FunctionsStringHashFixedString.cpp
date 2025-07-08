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
#    include <openssl/ripemd.h>
#    include <openssl/sha.h>
#    include <openssl/md4.h>
#    include <openssl/md5.h>
#    include <Common/OpenSSLHelpers.h>
#endif

#if USE_SHA3IUF
extern "C" {
    #include <sha3.h>
}
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
    extern const int OPENSSL_ERROR;
    extern const int LOGICAL_ERROR;
}


#if USE_SSL
using EVP_MD_CTX_ptr = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>;

/// Initializes a context with the right provider in the constructor.
/// Apply() then only copies it (once per new thread), this is faster than re-creating the context every time.
template <typename ProviderImpl>
class OpenSSLProvider
{
public:
    static constexpr auto name = ProviderImpl::name;
    static constexpr auto length = ProviderImpl::length;

    OpenSSLProvider()
        : ctx_template(EVP_MD_CTX_new(), &EVP_MD_CTX_free)
    {
        if (!ctx_template)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_MD_CTX_new failed: {}", getOpenSSLErrors());

        if (EVP_DigestInit_ex(ctx_template.get(), ProviderImpl::provider(), nullptr) != 1)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_DigestInit_ex failed: {}", getOpenSSLErrors());
    }

    void apply(const char * begin, size_t size, unsigned char * out_char_data)
    {
        if (!ctx_template)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No context provided");

        thread_local EVP_MD_CTX_ptr ctx(EVP_MD_CTX_new(), &EVP_MD_CTX_free);

        if (EVP_MD_CTX_copy_ex(ctx.get(), ctx_template.get()) != 1)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_MD_CTX_copy_ex failed: {}", getOpenSSLErrors());

        if (EVP_DigestUpdate(ctx.get(), begin, size) != 1)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_DigestUpdate failed: {}", getOpenSSLErrors());

        if (EVP_DigestFinal_ex(ctx.get(), out_char_data, nullptr) != 1)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_DigestFinal_ex failed: {}", getOpenSSLErrors());
    }

private:
    EVP_MD_CTX_ptr ctx_template;
};

struct MD4Impl
{
    static constexpr auto name = "MD4";
    static constexpr const EVP_MD * (*provider)() = &EVP_md4;
    enum
    {
        length = MD4_DIGEST_LENGTH
    };
};

struct MD5Impl
{
    static constexpr auto name = "MD5";
    static constexpr const EVP_MD * (*provider)() = &EVP_md5;
    enum
    {
        length = MD5_DIGEST_LENGTH
    };
};

struct SHA1Impl
{
    static constexpr auto name = "SHA1";
    static constexpr const EVP_MD * (*provider)() = &EVP_sha1;
    enum
    {
        length = SHA_DIGEST_LENGTH
    };
};

struct SHA224Impl
{
    static constexpr auto name = "SHA224";
    static constexpr const EVP_MD * (*provider)() = &EVP_sha224;
    enum
    {
        length = SHA224_DIGEST_LENGTH
    };

};

struct SHA256Impl
{
    static constexpr auto name = "SHA256";
    static constexpr const EVP_MD * (*provider)() = &EVP_sha256;
    enum
    {
        length = SHA256_DIGEST_LENGTH
    };
};

struct SHA384Impl
{
    static constexpr auto name = "SHA384";
    static constexpr const EVP_MD * (*provider)() = &EVP_sha384;
    enum
    {
        length = SHA384_DIGEST_LENGTH
    };
};

struct SHA512Impl
{
    static constexpr auto name = "SHA512";
    static constexpr const EVP_MD * (*provider)() = &EVP_sha512;
    enum
    {
        length = SHA512_DIGEST_LENGTH
    };
};

struct SHA512Impl256
{
    static constexpr auto name = "SHA512_256";
    static constexpr const EVP_MD * (*provider)() = &EVP_sha512_256;
    enum
    {
        length = SHA256_DIGEST_LENGTH
    };
};

struct RIPEMD160Impl
{
    static constexpr auto name = "RIPEMD160";
    static constexpr const EVP_MD * (*provider)() = &EVP_ripemd160;
    enum
    {
        length = RIPEMD160_DIGEST_LENGTH
    };
};
#endif

template <typename Impl>
class GenericProvider
{
public:
    static constexpr auto name = Impl::name;
    static constexpr auto length = Impl::length;

    void apply(const char* begin, size_t size, unsigned char* out_char_data)
    {
        Impl::apply(begin, size, out_char_data);
    }
};

#if USE_BLAKE3
struct ImplBLAKE3
{
    static constexpr auto name = "BLAKE3";
    enum
    {
        length = 32
    };

    static void apply(const char * begin, size_t size, unsigned char * out_char_data)
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

#if USE_SHA3IUF
struct Keccak256Impl
{
    static constexpr auto name = "keccak256";
    enum
    {
        length = 32
    };

    static void apply(const char * begin, size_t size, unsigned char * out_char_data)
    {
        sha3_HashBuffer(256, SHA3_FLAGS_KECCAK, begin, size, out_char_data, Keccak256Impl::length);
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
        auto hasher = Impl();

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
                hasher.apply(
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
                hasher.apply(
                    reinterpret_cast<const char *>(&data[i * length]),
                    length,
                    reinterpret_cast<uint8_t *>(&chars_to[i * Impl::length])
                );
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
                hasher.apply(
                    reinterpret_cast<const char *>(&data[i]),
                    length,
                    reinterpret_cast<uint8_t *>(&chars_to[i * Impl::length])
                );
            }
            return col_to;
        }
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", arguments[0].column->getName(), getName());
    }
};

#if USE_SSL || USE_BLAKE3 || USE_SHA3IUF
REGISTER_FUNCTION(HashFixedStrings)
{
#    if USE_SSL
    using FunctionMD4 = FunctionStringHashFixedString<OpenSSLProvider<MD4Impl>>;
    using FunctionMD5 = FunctionStringHashFixedString<OpenSSLProvider<MD5Impl>>;
    using FunctionSHA1 = FunctionStringHashFixedString<OpenSSLProvider<SHA1Impl>>;
    using FunctionSHA224 = FunctionStringHashFixedString<OpenSSLProvider<SHA224Impl>>;
    using FunctionSHA256 = FunctionStringHashFixedString<OpenSSLProvider<SHA256Impl>>;
    using FunctionSHA384 = FunctionStringHashFixedString<OpenSSLProvider<SHA384Impl>>;
    using FunctionSHA512 = FunctionStringHashFixedString<OpenSSLProvider<SHA512Impl>>;
    using FunctionSHA512_256 = FunctionStringHashFixedString<OpenSSLProvider<SHA512Impl256>>;
    using FunctionRIPEMD160 = FunctionStringHashFixedString<OpenSSLProvider<RIPEMD160Impl>>;

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
    using FunctionBLAKE3 = FunctionStringHashFixedString<GenericProvider<ImplBLAKE3>>;
    factory.registerFunction<FunctionBLAKE3>(FunctionDocumentation{
        .description = R"(
    Calculates BLAKE3 hash string and returns the resulting set of bytes as FixedString.
    This cryptographic hash-function is integrated into ClickHouse with BLAKE3 Rust library.
    The function is rather fast and shows approximately two times faster performance compared to SHA-2, while generating hashes of the same length as SHA-256.
    It returns a BLAKE3 hash as a byte array with type FixedString(32).
    )",
        .examples{{"hash", "SELECT hex(BLAKE3('ABC'))", ""}},
        .category{"Hash"}});
#    endif

#   if USE_SHA3IUF
    using FunctionKeccak256 = FunctionStringHashFixedString<GenericProvider<Keccak256Impl>>;
    factory.registerFunction<FunctionKeccak256>(FunctionDocumentation{
        .description = R"(Calculates the Keccak-256 cryptographic hash of the given string.
        This hash function is widely used in blockchain applications, particularly Ethereum.)",
        .syntax = "SELECT keccak256(message)",
        .arguments = {{"message", "The input [String](../../sql-reference/data-types/string.md)."}},
        .returned_value
        = "A [FixedString(32)](../../sql-reference/data-types/fixedstring.md) containing the 32-byte Keccak-256 hash of the input string.",
        .examples
        = {{"",
            "SELECT hex(keccak256('hello'))",
            R"(
┌─hex(keccak256('hello'))──────────────────────────────────────────┐
│ 1C8AFF950685C2ED4BC3174F3472287B56D9517B9C948127319A09A7A36DEAC8 │
└──────────────────────────────────────────────────────────────────┘
        )"}}});
#    endif
}
#endif
}
