#include <Common/config.h>

#if USE_AWS_S3

#    include <IO/S3Common.h>
#    include <IO/WriteBufferFromString.h>
#    include <Storages/StorageS3Settings.h>

#    include <aws/core/auth/AWSCredentialsProvider.h>
#    include <aws/core/utils/logging/LogMacros.h>
#    include <aws/core/utils/logging/LogSystemInterface.h>
#    include <aws/s3/S3Client.h>
#    include <aws/core/http/HttpClientFactory.h>
#    include <IO/S3/PocoHTTPClientFactory.h>
#    include <IO/S3/PocoHTTPClientFactory.cpp>
#    include <IO/S3/PocoHTTPClient.h>
#    include <IO/S3/PocoHTTPClient.cpp>
#    include <boost/algorithm/string.hpp>
#    include <Poco/URI.h>
#    include <re2/re2.h>
#    include <common/logger_useful.h>

namespace
{

const char * S3_LOGGER_TAG_NAMES[][2] = {
    {"AWSClient", "AWSClient"},
    {"AWSAuthV4Signer", "AWSClient (AWSAuthV4Signer)"},
};

const std::pair<DB::LogsLevel, Poco::Message::Priority> & convertLogLevel(Aws::Utils::Logging::LogLevel log_level)
{
    static const std::unordered_map<Aws::Utils::Logging::LogLevel, std::pair<DB::LogsLevel, Poco::Message::Priority>> mapping =
    {
        {Aws::Utils::Logging::LogLevel::Off, {DB::LogsLevel::none, Poco::Message::PRIO_FATAL}},
        {Aws::Utils::Logging::LogLevel::Fatal, {DB::LogsLevel::error, Poco::Message::PRIO_FATAL}},
        {Aws::Utils::Logging::LogLevel::Error, {DB::LogsLevel::error, Poco::Message::PRIO_ERROR}},
        {Aws::Utils::Logging::LogLevel::Warn, {DB::LogsLevel::warning, Poco::Message::PRIO_WARNING}},
        {Aws::Utils::Logging::LogLevel::Info, {DB::LogsLevel::information, Poco::Message::PRIO_INFORMATION}},
        {Aws::Utils::Logging::LogLevel::Debug, {DB::LogsLevel::trace, Poco::Message::PRIO_TRACE}},
        {Aws::Utils::Logging::LogLevel::Trace, {DB::LogsLevel::trace, Poco::Message::PRIO_TRACE}},
    };
    return mapping.at(log_level);
}

class AWSLogger final : public Aws::Utils::Logging::LogSystemInterface
{
public:
    AWSLogger()
    {
        for (auto [tag, name] : S3_LOGGER_TAG_NAMES)
            tag_loggers[tag] = &Poco::Logger::get(name);

        default_logger = tag_loggers[S3_LOGGER_TAG_NAMES[0][0]];
    }

    ~AWSLogger() final = default;

    Aws::Utils::Logging::LogLevel GetLogLevel() const final { return Aws::Utils::Logging::LogLevel::Trace; }

    void Log(Aws::Utils::Logging::LogLevel log_level, const char * tag, const char * format_str, ...) final // NOLINT
    {
        callLogImpl(log_level, tag, format_str); /// FIXME. Variadic arguments?
    }

    void LogStream(Aws::Utils::Logging::LogLevel log_level, const char * tag, const Aws::OStringStream & message_stream) final
    {
        callLogImpl(log_level, tag, message_stream.str().c_str());
    }

    void callLogImpl(Aws::Utils::Logging::LogLevel log_level, const char * tag, const char * message)
    {
        const auto & [level, prio] = convertLogLevel(log_level);
        if (tag_loggers.count(tag) > 0)
        {
            LOG_IMPL(tag_loggers[tag], level, prio, "{}", message);
        }
        else
        {
            LOG_IMPL(default_logger, level, prio, "{}: {}", tag, message);
        }
    }

    void Flush() final {}

private:
    Poco::Logger * default_logger;
    std::unordered_map<String, Poco::Logger *> tag_loggers;
};

class S3AuthSigner : public Aws::Client::AWSAuthV4Signer
{
public:
    S3AuthSigner(
        const Aws::Client::ClientConfiguration & client_configuration,
        const Aws::Auth::AWSCredentials & credentials,
        const DB::HeaderCollection & headers_)
        : Aws::Client::AWSAuthV4Signer(
            std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(credentials),
            "s3",
            client_configuration.region,
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
            false)
        , headers(headers_)
    {
    }

    bool SignRequest(Aws::Http::HttpRequest & request, const char * region, bool sign_body) const override
    {
        auto result = Aws::Client::AWSAuthV4Signer::SignRequest(request, region, sign_body);
        for (const auto & header : headers)
            request.SetHeaderValue(header.name, header.value);
        return result;
    }

    bool PresignRequest(
        Aws::Http::HttpRequest & request,
        const char * region,
        const char * serviceName,
        long long expiration_time_sec) const override // NOLINT
    {
        auto result = Aws::Client::AWSAuthV4Signer::PresignRequest(request, region, serviceName, expiration_time_sec);
        for (const auto & header : headers)
            request.SetHeaderValue(header.name, header.value);
        return result;
    }

private:
    const DB::HeaderCollection headers;
};

}


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace S3
{
    ClientFactory::ClientFactory()
    {
        aws_options = Aws::SDKOptions{};
        Aws::InitAPI(aws_options);
        Aws::Utils::Logging::InitializeAWSLogging(std::make_shared<AWSLogger>());
        Aws::Http::SetHttpClientFactory(std::make_shared<PocoHTTPClientFactory>());
    }

    ClientFactory::~ClientFactory()
    {
        Aws::Utils::Logging::ShutdownAWSLogging();
        Aws::ShutdownAPI(aws_options);
    }

    ClientFactory & ClientFactory::instance()
    {
        static ClientFactory ret;
        return ret;
    }

    /// This method is not static because it requires ClientFactory to be initialized.
    std::shared_ptr<Aws::S3::S3Client> ClientFactory::create( // NOLINT
        const String & endpoint,
        bool is_virtual_hosted_style,
        const String & access_key_id,
        const String & secret_access_key)
    {
        Aws::Client::ClientConfiguration cfg;

        if (!endpoint.empty())
            cfg.endpointOverride = endpoint;

        return create(cfg, is_virtual_hosted_style, access_key_id, secret_access_key);
    }

    std::shared_ptr<Aws::S3::S3Client> ClientFactory::create( // NOLINT
        Aws::Client::ClientConfiguration & cfg,
        bool is_virtual_hosted_style,
        const String & access_key_id,
        const String & secret_access_key)
    {
        Aws::Auth::AWSCredentials credentials(access_key_id, secret_access_key);

        Aws::Client::ClientConfiguration client_configuration = cfg;

        if (!client_configuration.endpointOverride.empty())
        {
            static const RE2 region_pattern(R"(^s3[.\-]([a-z0-9\-]+)\.amazonaws\.)");
            Poco::URI uri(client_configuration.endpointOverride);
            if (uri.getScheme() == "http")
                client_configuration.scheme = Aws::Http::Scheme::HTTP;

            String region;
            if (re2::RE2::PartialMatch(uri.getHost(), region_pattern, &region))
            {
                boost::algorithm::to_lower(region);
                client_configuration.region = region;
            }
        }

        return std::make_shared<Aws::S3::S3Client>(
            credentials, // Aws credentials.
            std::move(client_configuration), // Client configuration.
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, // Sign policy.
            is_virtual_hosted_style || cfg.endpointOverride.empty() // Use virtual addressing if endpoint is not specified.
        );
    }

    std::shared_ptr<Aws::S3::S3Client> ClientFactory::create( // NOLINT
        const String & endpoint,
        bool is_virtual_hosted_style,
        const String & access_key_id,
        const String & secret_access_key,
        HeaderCollection headers)
    {
        Aws::Client::ClientConfiguration cfg;
        if (!endpoint.empty())
            cfg.endpointOverride = endpoint;

        Aws::Auth::AWSCredentials credentials(access_key_id, secret_access_key);
        return std::make_shared<Aws::S3::S3Client>(
            std::make_shared<S3AuthSigner>(cfg, std::move(credentials), std::move(headers)),
            std::move(cfg), // Client configuration.
            is_virtual_hosted_style || cfg.endpointOverride.empty() // Use virtual addressing only if endpoint is not specified.
        );
    }

    URI::URI(const Poco::URI & uri_)
    {
        /// Case when bucket name represented in domain name of S3 URL.
        /// E.g. (https://bucket-name.s3.Region.amazonaws.com/key)
        /// https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html#virtual-hosted-style-access
        static const RE2 virtual_hosted_style_pattern(R"((.+)\.(s3|cos)([.\-][a-z0-9\-.:]+))");

        /// Case when bucket name and key represented in path of S3 URL.
        /// E.g. (https://s3.Region.amazonaws.com/bucket-name/key)
        /// https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html#path-style-access
        static const RE2 path_style_pattern("^/([^/]*)/(.*)");

        static constexpr auto S3 = "S3";
        static constexpr auto COSN = "COSN";
        static constexpr auto COS = "COS";

        uri = uri_;
        storage_name = S3;

        if (uri.getHost().empty())
            throw Exception("Host is empty in S3 URI: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);

        String name;
        String endpoint_authority_from_uri;

        if (re2::RE2::FullMatch(uri.getAuthority(), virtual_hosted_style_pattern, &bucket, &name, &endpoint_authority_from_uri))
        {
            is_virtual_hosted_style = true;
            endpoint = uri.getScheme() + "://" + name + endpoint_authority_from_uri;

            /// S3 specification requires at least 3 and at most 63 characters in bucket name.
            /// https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-s3-bucket-naming-requirements.html
            if (bucket.length() < 3 || bucket.length() > 63)
                throw Exception(
                    "Bucket name length is out of bounds in virtual hosted style S3 URI: " + bucket + " (" + uri.toString() + ")", ErrorCodes::BAD_ARGUMENTS);

            /// Remove leading '/' from path to extract key.
            key = uri.getPath().substr(1);
            if (key.empty() || key == "/")
                throw Exception("Key name is empty in virtual hosted style S3 URI: " + key + " (" + uri.toString() + ")", ErrorCodes::BAD_ARGUMENTS);
            boost::to_upper(name);
            if (name != S3 && name != COS)
            {
                throw Exception("Object storage system name is unrecognized in virtual hosted style S3 URI: " + name + " (" + uri.toString() + ")", ErrorCodes::BAD_ARGUMENTS);
            }
            if (name == S3)
            {
                storage_name = name;
            }
            else
            {
                storage_name = COSN;
            }
        }
        else if (re2::RE2::PartialMatch(uri.getPath(), path_style_pattern, &bucket, &key))
        {
            is_virtual_hosted_style = false;
            endpoint = uri.getScheme() + "://" + uri.getAuthority();

            /// S3 specification requires at least 3 and at most 63 characters in bucket name.
            /// https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-s3-bucket-naming-requirements.html
            if (bucket.length() < 3 || bucket.length() > 63)
                throw Exception(
                    "Bucket name length is out of bounds in path style S3 URI: " + bucket + " (" + uri.toString() + ")", ErrorCodes::BAD_ARGUMENTS);

            if (key.empty() || key == "/")
                throw Exception("Key name is empty in path style S3 URI: " + key + " (" + uri.toString() + ")", ErrorCodes::BAD_ARGUMENTS);
        }
        else
            throw Exception("Bucket or key name are invalid in S3 URI: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);
    }
}

}

#endif
