#include <Storages/Kafka/AWSMSKIAMAuth.h>

#include "config.h"

#if USE_AWS_S3

#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/re2.h>
#include <Core/SettingsFields.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Poco/URI.h>
#include <boost/algorithm/string/trim.hpp>
#include <cppkafka/configuration.h>
#include <librdkafka/rdkafka.h>
#include <IO/S3/Client.h>
#include <IO/S3/Credentials.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/signer/AWSAuthV4Signer.h>
#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/URI.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <chrono>
#include <mutex>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int AWS_ERROR;
}

namespace AWSMSKIAMAuth
{

namespace
{
    constexpr std::chrono::seconds TOKEN_LIFETIME{300};
    constexpr std::chrono::seconds PRESIGNED_URL_EXPIRY{900};

    String generateAWSMSKToken(
        const String & region,
        const Aws::Auth::AWSCredentials & credentials)
    {
        try
        {
            String service_host = "kafka." + region + ".amazonaws.com";

            Aws::Http::URI uri;
            uri.SetScheme(Aws::Http::Scheme::HTTPS);
            uri.SetAuthority(service_host);
            uri.SetPath("/");
            uri.AddQueryStringParameter("Action", "kafka-cluster:Connect");

            auto request = Aws::Http::CreateHttpRequest(
                uri,
                Aws::Http::HttpMethod::HTTP_GET,
                Aws::Utils::Stream::DefaultResponseStreamFactoryMethod);

            Aws::Client::AWSAuthV4Signer signer(
                std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(credentials),
                "kafka-cluster",
                region,
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
                true);

            if (!signer.PresignRequest(*request, PRESIGNED_URL_EXPIRY.count()))
            {
                throw Exception(ErrorCodes::AWS_ERROR, "Failed to presign AWS MSK IAM request");
            }

            String presigned_url = request->GetURIString();

            if (presigned_url.empty() || !presigned_url.contains("Action=kafka-cluster%3AConnect"))
            {
                throw Exception(ErrorCodes::AWS_ERROR,
                    "Invalid presigned URL generated: missing required Action parameter");
            }

            // Add User-Agent parameter AFTER signature (not included in signing)
            presigned_url += "&User-Agent=clickhouse-msk-iam";

            // AWS MSK requires Base64-URL encoding (RFC 4648 §5): + → -, / → _, remove padding
            return base64Encode(presigned_url, /* url_encoding */ true, /* no_padding */ true);
        }
        catch (const std::exception & e)
        {
            throw Exception(ErrorCodes::AWS_ERROR, "Failed to generate AWS MSK token: {}", e.what());
        }
    }

    void oauthBearerTokenRefreshCallback(
        rd_kafka_t * rk,
        const char * oauthbearer_config,
        void * /* opaque */)
    {
        LoggerPtr log = getLogger("AWSMSKIAMAuth");
        std::shared_ptr<S3::S3CredentialsProviderChain> provider;
        String region;

        // NOTE: We cannot use the global 'opaque' pointer because it is used by cppkafka
        // to store the Consumer/Producer instance. Overwriting it would break cppkafka callbacks.
        // Instead, we pass the context pointer address in the configuration string.

        OAuthBearerTokenRefreshContext * ctx = nullptr;

        if (oauthbearer_config && oauthbearer_config[0] != '\0')
        {
            try
            {
                Poco::URI uri;
                uri.setQuery(oauthbearer_config);
                auto params = uri.getQueryParameters();

                for (const auto & [key, value] : params)
                {
                    if (key == "context_ptr")
                    {
                        uintptr_t ptr_val = 0;
                        ReadBufferFromString rb(value);
                        if (tryReadIntText(ptr_val, rb))
                            ctx = reinterpret_cast<OAuthBearerTokenRefreshContext *>(ptr_val);
                    }
                }
            }
            catch (...)
            {
                // Ignore parsing errors, will fall back to default behavior (error)
                tryLogCurrentException(log, "Failed to parse OAuth bearer config");
            }
        }

        if (ctx)
        {
            if (ctx->log)
            {
                log = ctx->log;
            }
            provider = ctx->provider;
            region = ctx->region;
        }

        try
        {
            if (!provider)
            {
                LOG_ERROR(log, "Token refresh callback called without credentials provider context");
                rd_kafka_oauthbearer_set_token_failure(rk, "Internal error: missing credentials provider context");
                return;
            }

            auto credentials = provider->GetAWSCredentials();

            if (credentials.IsEmpty())
            {
                LOG_ERROR(log, "AWS credentials are empty");
                rd_kafka_oauthbearer_set_token_failure(rk, "No AWS credentials available");
                return;
            }

            String token = generateAWSMSKToken(region, credentials);

            auto now = std::chrono::system_clock::now();
            auto expiry_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                (now + TOKEN_LIFETIME).time_since_epoch()).count();

            char errstr[512];
            rd_kafka_resp_err_t err = rd_kafka_oauthbearer_set_token(
                rk, token.c_str(), expiry_ms, credentials.GetAWSAccessKeyId().c_str(),
                nullptr, 0, errstr, sizeof(errstr));

            if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
            {
                LOG_ERROR(log, "Failed to set OAuth token: {}", errstr);
                rd_kafka_oauthbearer_set_token_failure(rk, errstr);
            }
        }
        catch (const std::exception & e)
        {
            LOG_ERROR(log, "Token refresh failed: {}", e.what());
            rd_kafka_oauthbearer_set_token_failure(rk, e.what());
        }
        catch (...)
        {
            tryLogCurrentException(log, "Token refresh failed");
            rd_kafka_oauthbearer_set_token_failure(rk, "Unexpected exception");
        }
    }
}

bool isValidAWSRegion(const String & region)
{
    if (region.empty())
        return false;

    static const RE2 region_format_pattern(R"(^[a-z]{2,3}-[a-z]+-\d+$)");
    return RE2::FullMatch(region, region_format_pattern);
}

String extractRegionFromBroker(const String & broker_address)
{
    if (broker_address.empty())
        return "";

    String broker_host = broker_address;
    if (size_t colon_pos = broker_host.find(':'); colon_pos != String::npos)
    {
        if (colon_pos == 0)
            return "";
        broker_host = broker_host.substr(0, colon_pos);
    }

    static const RE2 region_pattern(R"((?i)\.kafka(?:-serverless)?\.([a-z0-9-]+)\.(?:vpce\.)?amazonaws\.com$)");
    std::string region;
    if (RE2::PartialMatch(broker_host, region_pattern, &region))
        return region;

    return "";
}

void setupAuthentication(
    cppkafka::Configuration & kafka_config,
    const Poco::Util::AbstractConfiguration & config,
    const String & region,
    const String & broker_list,
    LoggerPtr log,
    std::shared_ptr<OAuthBearerTokenRefreshContext> & context_holder)
{
    String effective_region = region;

    if (effective_region.empty() && !broker_list.empty())
    {
        size_t comma_pos = broker_list.find(',');
        String first_broker = (comma_pos != String::npos) ? broker_list.substr(0, comma_pos) : broker_list;
        boost::trim(first_broker);
        effective_region = extractRegionFromBroker(first_broker);
        if (!effective_region.empty())
            LOG_DEBUG(log, "Auto-detected AWS region '{}' from broker address '{}'", effective_region, first_broker);
    }

    if (effective_region.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot auto-detect AWS region from broker list '{}'. "
            "Or set kafka_aws_region explicitly in table SETTINGS.", broker_list);

    if (!isValidAWSRegion(effective_region))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid AWS region format: '{}'", effective_region);

    bool use_environment_credentials = config.getBool("kafka.use_environment_credentials", false);

    Poco::URI::QueryParameters params;
    params.emplace_back("region", effective_region);
    params.emplace_back("use_environment_credentials", use_environment_credentials ? "1" : "0");

    Poco::URI uri;
    uri.setQueryParameters(params);

    kafka_config.set("sasl.oauthbearer.config", uri.getRawQuery());
    kafka_config.set("sasl.mechanism", "OAUTHBEARER");
    kafka_config.set("security.protocol", "SASL_SSL");

    // Enable SASL queue to allow background authentication callbacks
    rd_kafka_conf_enable_sasl_queue(kafka_config.get_handle(), 1);

    if (!context_holder)
    {
        auto aws_client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
            effective_region, {}, 0, {}, false, false, false, false, {}, {});

        S3::CredentialsConfiguration credentials_configuration;
        credentials_configuration.use_environment_credentials = use_environment_credentials;

        auto provider = std::make_shared<S3::S3CredentialsProviderChain>(
            aws_client_configuration, Aws::Auth::AWSCredentials{}, credentials_configuration);

        context_holder = std::make_shared<OAuthBearerTokenRefreshContext>();
        context_holder->log = log;
        context_holder->provider = provider;
        context_holder->region = effective_region;
    }

    // Pass context pointer in the configuration string to avoid using 'opaque' which is used by cppkafka.
    WriteBufferFromOwnString wb;
    writeText(reinterpret_cast<uintptr_t>(context_holder.get()), wb);
    params.emplace_back("context_ptr", wb.str());

    // Update query parameters with the new context_ptr
    uri.setQueryParameters(params);
    kafka_config.set("sasl.oauthbearer.config", uri.getRawQuery());

    rd_kafka_conf_set_oauthbearer_token_refresh_cb(
        kafka_config.get_handle(), oauthBearerTokenRefreshCallback);
}

}

}

#endif // USE_AWS_S3
