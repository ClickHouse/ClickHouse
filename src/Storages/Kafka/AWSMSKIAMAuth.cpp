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
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <cppkafka/configuration.h>
#include <cppkafka/kafka_handle_base.h>
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

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int AWS_ERROR;
}

namespace AWSMSKIAMAuth
{

bool isValidAWSRegion(const String & region)
{
    if (region.empty())
        return false;
    static const RE2 region_format_pattern(R"(^[a-z]{2,3}-[a-z-]+-\d+$)");
    return RE2::FullMatch(region, region_format_pattern);
}

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

            presigned_url += "&User-Agent=clickhouse-msk-iam";

            return base64Encode(presigned_url, /* url_encoding */ true, /* no_padding */ true);
        }
        catch (const std::exception & e)
        {
            throw Exception(ErrorCodes::AWS_ERROR, "Failed to generate AWS MSK token: {}", e.what());
        }
    }
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

    boost::algorithm::to_lower(broker_host);

    static const RE2 region_pattern(R"(\.kafka(?:-serverless)?\.([a-z0-9-]+)\.(?:vpce\.)?amazonaws\.com(?:\.cn)?$)");
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
        size_t start = 0;
        while (start < broker_list.size())
        {
            size_t comma = broker_list.find(',', start);
            String broker = broker_list.substr(start, comma == String::npos ? String::npos : comma - start);
            boost::trim(broker);
            effective_region = extractRegionFromBroker(broker);
            if (!effective_region.empty())
            {
                if (log)
                    LOG_DEBUG(log, "Auto-detected AWS region '{}' from broker address '{}'", effective_region, broker);
                break;
            }
            if (comma == String::npos)
                break;
            start = comma + 1;
        }
    }

    if (effective_region.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Cannot auto-detect AWS region from broker list '{}'. "
            "Or set kafka_aws_region explicitly in table SETTINGS.", broker_list);

    if (!isValidAWSRegion(effective_region))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid AWS region format: '{}'", effective_region);

    bool use_environment_credentials = config.getBool("kafka.use_environment_credentials", false);

    kafka_config.set("sasl.mechanism", "OAUTHBEARER");
    kafka_config.set("security.protocol", "SASL_SSL");

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
    else if (!boost::iequals(context_holder->region, effective_region))
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "AWS MSK IAM: Different regions used: already configured region is '{}' but got another region '{}'. "
            "Consumer and producer of the same table must connect to MSK clusters in the same AWS region.",
            context_holder->region, effective_region);
    }

    kafka_config.set_oauthbearer_token_refresh_callback(
        [context = context_holder](cppkafka::KafkaHandleBase & handle, const std::string* /* oauthbearer_config */)
        {
            try
            {
                if (!context->provider)
                {
                    LOG_ERROR(context->log, "Missing credentials provider");
                    rd_kafka_oauthbearer_set_token_failure(handle.get_handle(), "Missing credentials provider");
                    return;
                }

                auto credentials = context->provider->GetAWSCredentials();

                if (credentials.IsExpiredOrEmpty())
                {
                    LOG_ERROR(context->log, "AWS credentials are empty or expired");
                    rd_kafka_oauthbearer_set_token_failure(handle.get_handle(), "No AWS credentials available");
                    return;
                }

                auto now = std::chrono::system_clock::now();

                String token = generateAWSMSKToken(context->region, credentials);

                auto expiry_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    (now + TOKEN_LIFETIME).time_since_epoch()).count();

                char errstr[512];
                rd_kafka_resp_err_t err = rd_kafka_oauthbearer_set_token(
                    handle.get_handle(), token.c_str(), expiry_ms, "aws-msk-iam",
                    nullptr, 0, errstr, sizeof(errstr));

                if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
                {
                    LOG_ERROR(context->log, "Failed to set OAuth token: {}", errstr);
                    rd_kafka_oauthbearer_set_token_failure(handle.get_handle(), errstr);
                }
            }
            catch (const std::exception & e)
            {
                LOG_ERROR(context->log, "Token refresh failed: {}", e.what());
                rd_kafka_oauthbearer_set_token_failure(handle.get_handle(), e.what());
            }
            catch (...)
            {
                tryLogCurrentException(context->log, "Token refresh failed");
                rd_kafka_oauthbearer_set_token_failure(handle.get_handle(), "Unexpected exception");
            }
        });
}

}

}

#endif // USE_AWS_S3
