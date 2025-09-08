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
#    include <Common/Crypto/OpenSSLInitializer.h>
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
    extern const int SUPPORT_IS_DISABLED;
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
    static constexpr auto available_in_fips_mode = ProviderImpl::available_in_fips_mode;

    OpenSSLProvider()
        : ctx_template(EVP_MD_CTX_new(), &EVP_MD_CTX_free)
    {
        if (OpenSSLInitializer::instance().isFIPSEnabled() && !available_in_fips_mode)
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Function {} is not available in FIPS mode", name);

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
    static constexpr bool available_in_fips_mode = false;
    enum
    {
        length = MD4_DIGEST_LENGTH
    };
};

struct MD5Impl
{
    static constexpr auto name = "MD5";
    static constexpr const EVP_MD * (*provider)() = &EVP_md5;
    static constexpr bool available_in_fips_mode = false;
    enum
    {
        length = MD5_DIGEST_LENGTH
    };
};

struct SHA1Impl
{
    static constexpr auto name = "SHA1";
    static constexpr const EVP_MD * (*provider)() = &EVP_sha1;
    static constexpr bool available_in_fips_mode = false;
    enum
    {
        length = SHA_DIGEST_LENGTH
    };
};

struct SHA224Impl
{
    static constexpr auto name = "SHA224";
    static constexpr const EVP_MD * (*provider)() = &EVP_sha224;
    static constexpr bool available_in_fips_mode = true;
    enum
    {
        length = SHA224_DIGEST_LENGTH
    };

};

struct SHA256Impl
{
    static constexpr auto name = "SHA256";
    static constexpr const EVP_MD * (*provider)() = &EVP_sha256;
    static constexpr bool available_in_fips_mode = true;
    enum
    {
        length = SHA256_DIGEST_LENGTH
    };
};

struct SHA384Impl
{
    static constexpr auto name = "SHA384";
    static constexpr const EVP_MD * (*provider)() = &EVP_sha384;
    static constexpr bool available_in_fips_mode = true;
    enum
    {
        length = SHA384_DIGEST_LENGTH
    };
};

struct SHA512Impl
{
    static constexpr auto name = "SHA512";
    static constexpr const EVP_MD * (*provider)() = &EVP_sha512;
    static constexpr bool available_in_fips_mode = true;
    enum
    {
        length = SHA512_DIGEST_LENGTH
    };
};

struct SHA512Impl256
{
    static constexpr auto name = "SHA512_256";
    static constexpr const EVP_MD * (*provider)() = &EVP_sha512_256;
    static constexpr bool available_in_fips_mode = true;
    enum
    {
        length = SHA256_DIGEST_LENGTH
    };
};

struct RIPEMD160Impl
{
    static constexpr auto name = "RIPEMD160";
    static constexpr const EVP_MD * (*provider)() = &EVP_ripemd160;
    static constexpr bool available_in_fips_mode = false;
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
                    offsets[i] - current_offset,
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

    FunctionDocumentation::Description description_BLAKE3  = R"(
Calculates BLAKE3 hash string and returns the resulting set of bytes as FixedString.
This cryptographic hash-function is integrated into ClickHouse with BLAKE3 Rust library.
The function is rather fast and shows approximately two times faster performance compared to SHA-2, while generating hashes of the same length as SHA-256.
It returns a BLAKE3 hash as a byte array with type FixedString(32).
    )";
    FunctionDocumentation::Syntax syntax_BLAKE3 = "BLAKE3(message)";
    FunctionDocumentation::Arguments arguments_BLAKE3 = {
        {"message", "The input string to hash.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_BLAKE3 = {
        "Returns the 32-byte BLAKE3 hash of the input string as a fixed-length string.", {"FixedString(32)"}
    };
    FunctionDocumentation::Examples example_BLAKE3 = {
    {
        "hash",
        R"(
SELECT hex(BLAKE3('ABC'))
        )",
        R"(
┌─hex(BLAKE3('ABC'))───────────────────────────────────────────────┐
│ D1717274597CF0289694F75D96D444B992A096F1AFD8E7BBFA6EBB1D360FEDFC │
└──────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_BLAKE3 = {22, 10};
    FunctionDocumentation::Category category_BLAKE3 = FunctionDocumentation::Category::Hash;
    FunctionDocumentation documentation_BLAKE3 = {description_BLAKE3, syntax_BLAKE3, arguments_BLAKE3, returned_value_BLAKE3, example_BLAKE3, introduced_in_BLAKE3, category_BLAKE3};

    factory.registerFunction<FunctionBLAKE3>(documentation_BLAKE3);
#    endif

#   if USE_SHA3IUF
    using FunctionKeccak256 = FunctionStringHashFixedString<GenericProvider<Keccak256Impl>>;

    FunctionDocumentation::Description description_keccak256 = R"(
Calculates the Keccak-256 cryptographic hash of the given string.
This hash function is widely used in blockchain applications, particularly Ethereum.
    )";
    FunctionDocumentation::Syntax syntax_keccak256 = "keccak256(message)";
    FunctionDocumentation::Arguments arguments_keccak256 = {
        {"message", "The input string to hash.", {"String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_keccak256 = {
        "Returns the 32-byte Keccak-256 hash of the input string as a fixed-length string.", {"FixedString(32)"}
    };
    FunctionDocumentation::Examples example_keccak256 = {
    {
        "Usage example",
        R"(
SELECT hex(keccak256('hello'))
        )",
        R"(
┌─hex(keccak256('hello'))──────────────────────────────────────────┐
│ 1C8AFF950685C2ED4BC3174F3472287B56D9517B9C948127319A09A7A36DEAC8 │
└──────────────────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_keccak256 = {25, 4};
    FunctionDocumentation::Category category_keccak256 = FunctionDocumentation::Category::Hash;
    FunctionDocumentation documentation_keccak256 = {description_keccak256, syntax_keccak256, arguments_keccak256, returned_value_keccak256, example_keccak256, introduced_in_keccak256, category_keccak256};

    factory.registerFunction<FunctionKeccak256>(documentation_keccak256);
#    endif
}
#endif
}
