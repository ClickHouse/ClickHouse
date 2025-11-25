#include "config.h"

#if USE_SSL

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/OpenSSLHelpers.h>

#include <openssl/evp.h>
#include <openssl/hmac.h>

#include <Poco/String.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int OPENSSL_ERROR;
}

namespace
{

const EVP_MD * getHashAlgorithm(const String & mode)
{
    String mode_lower = Poco::toLower(mode);

    if (mode_lower == "md5")
        return EVP_md5();
    else if (mode_lower == "sha1")
        return EVP_sha1();
    else if (mode_lower == "sha224")
        return EVP_sha224();
    else if (mode_lower == "sha256")
        return EVP_sha256();
    else if (mode_lower == "sha384")
        return EVP_sha384();
    else if (mode_lower == "sha512")
        return EVP_sha512();
    else
        return nullptr;
}

class FunctionHMAC : public IFunction
{
public:
    static constexpr auto name = "HMAC";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionHMAC>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        validateFunctionArguments(
            *this,
            arguments,
            FunctionArgumentDescriptors{
                {"mode", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), isColumnConst, "Hash algorithm name (e.g., 'sha256')"},
                {"message", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), {}, "Message to be authenticated"},
                {"key", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), {}, "Secret key for HMAC"},
            });

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const StringRef mode = arguments[0].column->getDataAt(0);
        const EVP_MD * evp_md = getHashAlgorithm(mode.toString());

        if (evp_md == nullptr)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid hash algorithm: '{}'. Supported algorithms are: md5, sha1, sha224, sha256, sha384, sha512 (case-insensitive)",
                mode.toString());

        const auto message_column = arguments[1].column;
        const auto key_column = arguments[2].column;

        auto result_column = ColumnString::create();
        ColumnString::Chars & result_data = result_column->getChars();
        ColumnString::Offsets & result_offsets = result_column->getOffsets();

        const size_t digest_length = EVP_MD_size(evp_md);

        // Pre-allocate result data
        {
            size_t total_size = 0;
            for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
                total_size += digest_length;
            result_data.resize(total_size);
        }

        UInt8 * result_ptr = result_data.data();

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            const StringRef message_value = message_column->getDataAt(row_idx);
            const StringRef key_value = key_column->getDataAt(row_idx);

            unsigned int actual_digest_length = 0;
            unsigned char * hmac_result = HMAC(
                evp_md,
                reinterpret_cast<const unsigned char *>(key_value.data),
                static_cast<int>(key_value.size),
                reinterpret_cast<const unsigned char *>(message_value.data),
                message_value.size,
                reinterpret_cast<unsigned char *>(result_ptr),
                &actual_digest_length);

            if (hmac_result == nullptr)
                throw Exception(ErrorCodes::OPENSSL_ERROR, "HMAC computation failed: {}", getOpenSSLErrors());

            if (actual_digest_length != digest_length)
                throw Exception(ErrorCodes::OPENSSL_ERROR, "HMAC digest length mismatch: expected {}, got {}", digest_length, actual_digest_length);

            result_ptr += digest_length;
            result_offsets.push_back(result_offsets.empty() ? digest_length : result_offsets.back() + digest_length);
        }

        return result_column;
    }
};

}

REGISTER_FUNCTION(HMAC)
{
    FunctionDocumentation::Description description = R"(
Computes the HMAC (Hash-based Message Authentication Code) for the given message using the specified hash algorithm and secret key.

Supported hash algorithms (case-insensitive):
- md5
- sha1
- sha224
- sha256
- sha384
- sha512
    )";

    FunctionDocumentation::Syntax syntax = "HMAC(mode, message, key)";

    FunctionDocumentation::Arguments arguments = {
        {"mode", "Hash algorithm name (case-insensitive). Supported: md5, sha1, sha224, sha256, sha384, sha512.", {"String"}},
        {"message", "Message to be authenticated.", {"String"}},
        {"key", "Secret key for HMAC.", {"String"}}
    };

    FunctionDocumentation::ReturnedValue returned_value = {
        "Returns a binary string containing the HMAC digest.",
        {"String"}
    };

    FunctionDocumentation::Examples examples = {
        {
            "Basic HMAC-SHA256",
            R"(
SELECT hex(HMAC('sha256', 'The quick brown fox jumps over the lazy dog', 'secret_key'));
            )",
            R"(
┌─hex(HMAC('sha256', 'The quick brown fox jumps over the lazy dog', 'secret_key'))─┐
│ 31FD15FA0F61FD40DC09D919D4AA5B4141A0B27C1D51E74A6789A890AAAA187C                 │
└──────────────────────────────────────────────────────────────────────────────────┘
            )"
        },
        {
            "Different hash algorithms",
            R"(
SELECT
    hex(HMAC('md5', 'message', 'key')) AS hmac_md5,
    hex(HMAC('sha1', 'message', 'key')) AS hmac_sha1,
    hex(HMAC('sha256', 'message', 'key')) AS hmac_sha256;
            )",
            R"(
┌─hmac_md5─────────────────────────┬─hmac_sha1────────────────────────────────┬─hmac_sha256──────────────────────────────────────────────────────┐
│ 4E4748E62B463521F6775FBF921234B5 │ 2088DF74D5F2146B48146CAF4965377E9D0BE3A4 │ 6E9EF29B75FFFC5B7ABAE527D58FDADB2FE42E7219011976917343065F58ED4A │
└──────────────────────────────────┴──────────────────────────────────────────┴──────────────────────────────────────────────────────────────────┘
            )"
        },
        {
            "Case-insensitive mode",
            R"(
SELECT
    HMAC('SHA256', 'message', 'key') = HMAC('sha256', 'message', 'key') AS same_result,
    HMAC('SHA256', 'message', 'key') = HMAC('Sha256', 'message', 'key') AS also_same;
            )",
            R"(
┌─same_result─┬─also_same─┐
│           1 │         1 │
└─────────────┴───────────┘
            )"
        }
    };

    FunctionDocumentation::IntroducedIn introduced_in = {25, 12};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Encryption;

    FunctionDocumentation documentation = {
        description,
        syntax,
        arguments,
        returned_value,
        examples,
        introduced_in,
        category
    };

    factory.registerFunction<FunctionHMAC>(documentation, FunctionFactory::Case::Insensitive);
}

}

#endif

