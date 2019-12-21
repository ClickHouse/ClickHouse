#include <Common/config.h>

#if USE_AWS_S3

#include <IO/S3Common.h>
#include <IO/WriteBufferFromString.h>

#include <regex>
#include <aws/s3/S3Client.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/utils/logging/LogSystemInterface.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <common/logger_useful.h>


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

class AWSLogger : public Aws::Utils::Logging::LogSystemInterface
{
public:
    ~AWSLogger() final = default;

    Aws::Utils::Logging::LogLevel GetLogLevel() const final { return Aws::Utils::Logging::LogLevel::Trace; }

    void Log(Aws::Utils::Logging::LogLevel log_level, const char * tag, const char * format_str, ...) final
    {
        auto & [level, prio] = convertLogLevel(log_level);
        LOG_SIMPLE(log, std::string(tag) + ": " + format_str, level, prio);
    }

    void LogStream(Aws::Utils::Logging::LogLevel log_level, const char * tag, const Aws::OStringStream & message_stream) final
    {
        auto & [level, prio] = convertLogLevel(log_level);
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

    std::shared_ptr<Aws::S3::S3Client> ClientFactory::create(
        const String & endpoint,
        const String & access_key_id,
        const String & secret_access_key)
    {
        Aws::Client::ClientConfiguration cfg;
        if (!endpoint.empty())
            cfg.endpointOverride = endpoint;

        auto cred_provider = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(access_key_id,
                secret_access_key);

        return std::make_shared<Aws::S3::S3Client>(
                std::move(cred_provider), // Credentials provider.
                std::move(cfg), // Client configuration.
                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, // Sign policy.
                endpoint.empty() // Use virtual addressing only if endpoint is not specified.
        );
    }


    URI::URI(Poco::URI & uri_)
    {
        static const std::regex BUCKET_KEY_PATTERN("([^/]+)/(.*)");

        uri = uri_;

        // s3://*
        if (uri.getScheme() == "s3" || uri.getScheme() == "S3")
        {
            bucket = uri.getAuthority();
            if (bucket.empty())
                throw Exception ("Invalid S3 URI: no bucket: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);

            const auto & path = uri.getPath();
            // s3://bucket or s3://bucket/
            if (path.length() <= 1)
                throw Exception ("Invalid S3 URI: no key: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);

            key = path.substr(1);
            return;
        }

        if (uri.getHost().empty())
            throw Exception("Invalid S3 URI: no host: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);

        endpoint = uri.getScheme() + "://" + uri.getAuthority();

        // Parse bucket and key from path.
        std::smatch match;
        std::regex_search(uri.getPath(), match, BUCKET_KEY_PATTERN);
        if (!match.empty())
        {
            bucket = match.str(1);
            if (bucket.empty())
                throw Exception ("Invalid S3 URI: no bucket: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);

            key = match.str(2);
            if (key.empty())
                throw Exception ("Invalid S3 URI: no key: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);
        }
        else
            throw Exception("Invalid S3 URI: no bucket or key: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);
    }
}

}

#endif
