#include "config.h"

#if USE_SSL

#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Common/MapWithMemoryTracking.h>
#include <Common/OpenSSLHelpers.h>
#include <Common/SetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <openssl/evp.h>
#include <openssl/hmac.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int OPENSSL_ERROR;
}

namespace
{

const EVP_MD * getHashAlgorithm(const std::string_view & mode)
{
    return EVP_MD_fetch(nullptr, std::string{mode}.c_str(), nullptr);
}

class FunctionHMAC final : public IFunction
{
private:
    inline static std::once_flag supported_algorithms_flag;
    inline static MapWithMemoryTracking<std::string, SetWithMemoryTracking<std::string>> grouped_algorithms;

    static void fetchAndGroupSupportedAlgorithms()
    {
        /// The callback is invoked from OpenSSL C code (EVP_MD_do_all_sorted), so it must be
        /// noexcept: a C++ exception (e.g. a memory limit hit while inserting) must not unwind
        /// through the C frames. Capture it and rethrow once the C call has returned.
        struct CallbackState
        {
            MapWithMemoryTracking<std::string, SetWithMemoryTracking<std::string>> algorithms_map;
            std::exception_ptr exception;
        };
        CallbackState state;

        EVP_MD_do_all_sorted(
            [](const EVP_MD * /* md */, const char * md_name, const char * alias, void * arg) noexcept
            {
                auto & cb_state = *static_cast<CallbackState *>(arg);
                if (cb_state.exception)
                    return;
                try
                {
                    std::string primary_name = md_name;
                    cb_state.algorithms_map[primary_name].insert(primary_name);
                    if (alias)
                        cb_state.algorithms_map[primary_name].insert(alias);
                }
                catch (...)
                {
                    cb_state.exception = std::current_exception();
                }
            },
            &state);

        if (state.exception)
            std::rethrow_exception(state.exception);

        auto & algorithms_map = state.algorithms_map;

        /// Filter out algorithms that cannot actually be fetched
        /// (e.g., non-approved algorithms when running in FIPS mode)
        for (auto it = algorithms_map.begin(); it != algorithms_map.end();)
        {
            EVP_MD * md = EVP_MD_fetch(nullptr, it->first.c_str(), nullptr);
            if (md == nullptr)
                it = algorithms_map.erase(it);
            else
            {
                EVP_MD_free(md);
                ++it;
            }
        }

        grouped_algorithms = std::move(algorithms_map);
    }

    static const MapWithMemoryTracking<std::string, SetWithMemoryTracking<std::string>> & getGroupedAlgorithms()
    {
        std::call_once(supported_algorithms_flag, [] { fetchAndGroupSupportedAlgorithms(); });
        return grouped_algorithms;
    }

public:
    static constexpr auto name = "HMAC";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionHMAC>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 3; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    String getSignatureString() const override
    {
        return "(const StringOrFixedString, StringOrFixedString, StringOrFixedString) -> String";
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const std::string_view mode = arguments[0].column->getDataAt(0);
        const EVP_MD * evp_md = getHashAlgorithm(mode);

        if (evp_md == nullptr)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid hash algorithm: '{}'. Supported algorithms are: {}.",
                mode,
                getSupportedAlgorithmsAsString());

        const auto message_column = arguments[1].column;
        const auto key_column = arguments[2].column;

        auto result_column = ColumnString::create();
        ColumnString::Chars & result_data = result_column->getChars();
        ColumnString::Offsets & result_offsets = result_column->getOffsets();

        const size_t digest_length = EVP_MD_size(evp_md);

        // Pre-allocate result data
        const size_t total_size = input_rows_count * digest_length;
        result_data.resize(total_size);

        UInt8 * result_ptr = result_data.data();

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            const std::string_view message_value = message_column->getDataAt(row_idx);
            const std::string_view key_value = key_column->getDataAt(row_idx);

            unsigned int actual_digest_length = 0;
            const unsigned char * hmac_result = HMAC(
                evp_md,
                key_value.data(),
                static_cast<int>(key_value.size()),
                reinterpret_cast<const unsigned char *>(message_value.data()),
                message_value.size(),
                reinterpret_cast<unsigned char *>(result_ptr),
                &actual_digest_length);

            if (hmac_result == nullptr)
                throw Exception(ErrorCodes::OPENSSL_ERROR, "HMAC computation failed: {}", getOpenSSLErrors());

            if (actual_digest_length != digest_length)
                throw Exception(ErrorCodes::OPENSSL_ERROR, "HMAC digest length mismatch: expected {}, got {}", digest_length, actual_digest_length);

            result_ptr += digest_length;
            result_offsets.push_back((row_idx + 1) * digest_length);
        }

        return result_column;
    }

    static std::string getSupportedAlgorithmsAsString(bool by_lines = false)
    {
        const auto & algorithms = getGroupedAlgorithms();
        VectorWithMemoryTracking<std::string> formatted_algorithms;

        for (const auto & [primary, aliases] : algorithms)
        {
            if (aliases.size() > 1)
                formatted_algorithms.emplace_back(fmt::format("{} (aliases: {})", primary, fmt::join(aliases, ", ")));
            else
                formatted_algorithms.emplace_back(primary);
        }
        if (by_lines)
            return fmt::format("- {}", fmt::join(formatted_algorithms, "\n- "));
        else
            return fmt::format("{}", fmt::join(formatted_algorithms, ", "));
    }
};

}

REGISTER_FUNCTION(FunctionHMAC)
{
    FunctionDocumentation::Description description = fmt::format(R"(
Computes the HMAC (Hash-based Message Authentication Code) for the given message using the specified hash algorithm and secret key.

Supported hash algorithms:
{}
    )", FunctionHMAC::getSupportedAlgorithmsAsString(true));

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
    hmac('SHA256', 'message', 'key') = HMAC('sha256', 'message', 'key') AS same_result,
    HMAC('SHA256', 'message', 'key') = Hmac('Sha256', 'message', 'key') AS also_same;
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
        {},
        returned_value,
        examples,
        introduced_in,
        category
    };

    factory.registerFunction<FunctionHMAC>(documentation, FunctionFactory::Case::Insensitive);
}

}

#endif

