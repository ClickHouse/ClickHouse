#include <Common/config.h>

#if USE_AWS_S3

#    include <IO/S3Common.h>
#    include <IO/WriteBufferFromString.h>

#    include <aws/core/auth/AWSCredentialsProvider.h>
#    include <aws/core/utils/logging/LogMacros.h>
#    include <aws/core/utils/logging/LogSystemInterface.h>
#    include <aws/s3/S3Client.h>
#    include <re2/re2.h>
#    include <common/logger_useful.h>


namespace
{
const std::pair<LogsLevel, Message::Priority> & convertLogLevel(Aws::Utils::Logging::LogLevel log_level)
{
    static const std::unordered_map<Aws::Utils::Logging::LogLevel, std::pair<LogsLevel, Message::Priority>> mapping = {
        {Aws::Utils::Logging::LogLevel::Off, {LogsLevel::none, Message::PRIO_FATAL}},
        {Aws::Utils::Logging::LogLevel::Fatal, {LogsLevel::error, Message::PRIO_FATAL}},
        {Aws::Utils::Logging::LogLevel::Error, {LogsLevel::error, Message::PRIO_ERROR}},
        {Aws::Utils::Logging::LogLevel::Warn, {LogsLevel::warning, Message::PRIO_WARNING}},
        {Aws::Utils::Logging::LogLevel::Info, {LogsLevel::information, Message::PRIO_INFORMATION}},
        {Aws::Utils::Logging::LogLevel::Debug, {LogsLevel::debug, Message::PRIO_DEBUG}},
        {Aws::Utils::Logging::LogLevel::Trace, {LogsLevel::trace, Message::PRIO_TRACE}},
    };
    return mapping.at(log_level);
}

class AWSLogger final : public Aws::Utils::Logging::LogSystemInterface
{
public:
    ~AWSLogger() final = default;

    Aws::Utils::Logging::LogLevel GetLogLevel() const final { return Aws::Utils::Logging::LogLevel::Trace; }

    void Log(Aws::Utils::Logging::LogLevel log_level, const char * tag, const char * format_str, ...) final // NOLINT
    {
        const auto & [level, prio] = convertLogLevel(log_level);
        LOG_SIMPLE(log, std::string(tag) + ": " + format_str, level, prio);
    }

    void LogStream(Aws::Utils::Logging::LogLevel log_level, const char * tag, const Aws::OStringStream & message_stream) final
    {
        const auto & [level, prio] = convertLogLevel(log_level);
        LOG_SIMPLE(log, std::string(tag) + ": " + message_stream.str(), level, prio);
    }

    void Flush() final {}

private:
    Poco::Logger * log = &Poco::Logger::get("AWSClient");
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
        aws_options = Aws::SDKOptions {};
        Aws::InitAPI(aws_options);
        Aws::Utils::Logging::InitializeAWSLogging(std::make_shared<AWSLogger>());
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

    std::shared_ptr<Aws::S3::S3Client> ClientFactory::create( // NOLINT
        const String & endpoint,
        const String & access_key_id,
        const String & secret_access_key)
    {
        Aws::Client::ClientConfiguration cfg;
        if (!endpoint.empty())
            cfg.endpointOverride = endpoint;

        Aws::Auth::AWSCredentials credentials(access_key_id, secret_access_key);

        return std::make_shared<Aws::S3::S3Client>(
                credentials, // Aws credentials.
                std::move(cfg), // Client configuration.
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, // Sign policy.
                endpoint.empty() // Use virtual addressing only if endpoint is not specified.
        );
    }


    URI::URI(const Poco::URI & uri_)
    {
        /// Case when bucket name represented in domain name of S3 URL.
        /// E.g. (https://bucket-name.s3.Region.amazonaws.com/key)
        /// https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html#virtual-hosted-style-access
        static const RE2 virtual_hosted_style_pattern(R"((.+\.)?s3[.\-][a-z0-9\-.]+)");
        /// Case when bucket name and key represented in path of S3 URL.
        /// E.g. (https://s3.Region.amazonaws.com/bucket-name/key)
        /// https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html#path-style-access
        static const RE2 path_style_pattern("([^/]+)/(.*)");

        uri = uri_;

        if (uri.getHost().empty())
            throw Exception("Host is empty in S3 URI: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);

        endpoint = uri.getScheme() + "://" + uri.getAuthority();

        if (re2::RE2::FullMatch(uri.getAuthority(), virtual_hosted_style_pattern, &bucket))
        {
            if (!bucket.empty())
                bucket.pop_back(); /// Remove '.' character from the end of the bucket name.

            /// S3 specification requires at least 3 and at most 63 characters in bucket name.
            /// https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-s3-bucket-naming-requirements.html
            if (bucket.length() < 3 || bucket.length() > 63)
                throw Exception(
                    "Bucket name length out of bounds in S3 URI: " + bucket + " (" + uri.toString() + ")", ErrorCodes::BAD_ARGUMENTS);

            /// Remove leading '/' from path to extract key.
            key = uri.getPath().substr(1);
            if (key.empty() || key == "/")
                throw Exception("Key name is empty in S3 URI: " + key + " (" + uri.toString() + ")", ErrorCodes::BAD_ARGUMENTS);
        }
        else if (re2::RE2::PartialMatch(uri.getPath(), path_style_pattern, &bucket, &key))
        {
            /// S3 specification requires at least 3 and at most 63 characters in bucket name.
            /// https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-s3-bucket-naming-requirements.html
            if (bucket.length() < 3 || bucket.length() > 63)
                throw Exception(
                    "Bucket name length out of bounds in S3 URI: " + bucket + " (" + uri.toString() + ")", ErrorCodes::BAD_ARGUMENTS);

            if (key.empty() || key == "/")
                throw Exception("Key name is empty in S3 URI: " + key + " (" + uri.toString() + ")", ErrorCodes::BAD_ARGUMENTS);
        }
        else
            throw Exception("Bucket or key name are invalid in S3 URI: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);
    }
}

}

#endif
