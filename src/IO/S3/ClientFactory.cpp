#include <Common/config.h>


#if USE_AWS_S3

#include <IO/S3/ClientFactory.h>

#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/S3/Client.h>
#include <IO/S3/PocoHTTPClientFactory.h>
#include <IO/S3/PocoHTTPClient.h>
#include <Storages/StorageS3Settings.h>

#include <aws/core/Version.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/STSCredentialsProvider.h>
#include <aws/core/client/DefaultRetryStrategy.h>
#include <aws/core/client/SpecifiedRetryableErrorsRetryStrategy.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/platform/OSVersionInfo.h>
#include <aws/core/utils/json/JsonSerializer.h>
#include <aws/core/utils/logging/LogMacros.h>
#include <aws/core/utils/logging/LogSystemInterface.h>
#include <aws/core/utils/UUID.h>
#include <aws/core/http/HttpClientFactory.h>

#include <Poco/URI.h>
#include <re2/re2.h>
#include <boost/algorithm/string/case_conv.hpp>

#include <fstream>


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
        {Aws::Utils::Logging::LogLevel::Debug, {DB::LogsLevel::debug, Poco::Message::PRIO_TEST}},
        {Aws::Utils::Logging::LogLevel::Trace, {DB::LogsLevel::trace, Poco::Message::PRIO_TEST}},
    };
    return mapping.at(log_level);
}

class AWSLogger final : public Aws::Utils::Logging::LogSystemInterface
{
public:
    explicit AWSLogger(bool enable_s3_requests_logging_)
        :enable_s3_requests_logging(enable_s3_requests_logging_)
    {
        for (auto [tag, name] : S3_LOGGER_TAG_NAMES)
            tag_loggers[tag] = &Poco::Logger::get(name);

        default_logger = tag_loggers[S3_LOGGER_TAG_NAMES[0][0]];
    }

    ~AWSLogger() final = default;

    Aws::Utils::Logging::LogLevel GetLogLevel() const final
    {
        if (enable_s3_requests_logging)
            return Aws::Utils::Logging::LogLevel::Trace;
        else
            return Aws::Utils::Logging::LogLevel::Info;
    }

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
        if (tag_loggers.contains(tag))
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
    bool enable_s3_requests_logging;
    std::unordered_map<String, Poco::Logger *> tag_loggers;
};

}


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int S3_ERROR;
}

namespace S3
{
    ClientFactory::ClientFactory()
    {
        aws_options = Aws::SDKOptions{};
        Aws::InitAPI(aws_options);
        Aws::Utils::Logging::InitializeAWSLogging(std::make_shared<AWSLogger>(false));
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

    std::unique_ptr<Client> ClientFactory::create(const ClientConfiguration & client_configuration, const URI & uri) // NOLINT
    {
        return std::make_unique<Client>(client_configuration, uri);
    }

    ClientConfiguration ClientFactory::createClientConfiguration(const RemoteHostFilter & remote_host_filter) // NOLINT
    {
        return ClientConfiguration(remote_host_filter);
    }
}

}

#endif
