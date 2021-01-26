#include <Common/config.h>

#if USE_AWS_S3

#    include <IO/S3Common.h>
#    include <IO/WriteBufferFromString.h>
#    include <Storages/StorageS3Settings.h>

#    include <aws/core/auth/AWSCredentialsProvider.h>
#    include <aws/core/auth/AWSCredentialsProviderChain.h>
#    include <aws/core/auth/STSCredentialsProvider.h>
#    include <aws/core/client/DefaultRetryStrategy.h>
#    include <aws/core/platform/Environment.h>
#    include <aws/core/utils/logging/LogMacros.h>
#    include <aws/core/utils/logging/LogSystemInterface.h>
#    include <aws/s3/S3Client.h>
#    include <aws/core/http/HttpClientFactory.h>
#    include <IO/S3/PocoHTTPClientFactory.h>
#    include <IO/S3/PocoHTTPClient.h>
#    include <Poco/URI.h>
#    include <re2/re2.h>
#    include <boost/algorithm/string/case_conv.hpp>
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

class S3CredentialsProviderChain : public Aws::Auth::AWSCredentialsProviderChain
{
public:
    explicit S3CredentialsProviderChain(const DB::S3::PocoHTTPClientConfiguration & configuration, const Aws::Auth::AWSCredentials & credentials, bool use_environment_credentials)
    {
        if (use_environment_credentials)
        {
            const DB::RemoteHostFilter & remote_host_filter = configuration.remote_host_filter;
            const unsigned int s3_max_redirects = configuration.s3_max_redirects;

            static const char AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI[] = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
            static const char AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI[] = "AWS_CONTAINER_CREDENTIALS_FULL_URI";
            static const char AWS_ECS_CONTAINER_AUTHORIZATION_TOKEN[] = "AWS_CONTAINER_AUTHORIZATION_TOKEN";
            static const char AWS_EC2_METADATA_DISABLED[] = "AWS_EC2_METADATA_DISABLED";

            auto * logger = &Poco::Logger::get("S3CredentialsProviderChain");

            /// The only difference from DefaultAWSCredentialsProviderChain::DefaultAWSCredentialsProviderChain()
            /// is that this chain uses custom ClientConfiguration.

            AddProvider(std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>());
            AddProvider(std::make_shared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>());
            AddProvider(std::make_shared<Aws::Auth::STSAssumeRoleWebIdentityCredentialsProvider>());

            /// ECS TaskRole Credentials only available when ENVIRONMENT VARIABLE is set.
            const auto relative_uri = Aws::Environment::GetEnv(AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI);
            LOG_DEBUG(logger, "The environment variable value {} is {}", AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI,
                    relative_uri);

            const auto absolute_uri = Aws::Environment::GetEnv(AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI);
            LOG_DEBUG(logger, "The environment variable value {} is {}", AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI,
                    absolute_uri);

            const auto ec2_metadata_disabled = Aws::Environment::GetEnv(AWS_EC2_METADATA_DISABLED);
            LOG_DEBUG(logger, "The environment variable value {} is {}", AWS_EC2_METADATA_DISABLED,
                    ec2_metadata_disabled);

            if (!relative_uri.empty())
            {
                AddProvider(std::make_shared<Aws::Auth::TaskRoleCredentialsProvider>(relative_uri.c_str()));
                LOG_INFO(logger, "Added ECS metadata service credentials provider with relative path: [{}] to the provider chain.",
                        relative_uri);
            }
            else if (!absolute_uri.empty())
            {
                const auto token = Aws::Environment::GetEnv(AWS_ECS_CONTAINER_AUTHORIZATION_TOKEN);
                AddProvider(std::make_shared<Aws::Auth::TaskRoleCredentialsProvider>(absolute_uri.c_str(), token.c_str()));

                /// DO NOT log the value of the authorization token for security purposes.
                LOG_INFO(logger, "Added ECS credentials provider with URI: [{}] to the provider chain with a{} authorization token.",
                        absolute_uri, token.empty() ? "n empty" : " non-empty");
            }
            else if (Aws::Utils::StringUtils::ToLower(ec2_metadata_disabled.c_str()) != "true")
            {
                Aws::Client::ClientConfiguration aws_client_configuration;

                /// See MakeDefaultHttpResourceClientConfiguration().
                /// This is part of EC2 metadata client, but unfortunately it can't be accessed from outside
                /// of contrib/aws/aws-cpp-sdk-core/source/internal/AWSHttpResourceClient.cpp
                aws_client_configuration.maxConnections = 2;
                aws_client_configuration.scheme = Aws::Http::Scheme::HTTP;

                /// Explicitly set the proxy settings to empty/zero to avoid relying on defaults that could potentially change
                /// in the future.
                aws_client_configuration.proxyHost = "";
                aws_client_configuration.proxyUserName = "";
                aws_client_configuration.proxyPassword = "";
                aws_client_configuration.proxyPort = 0;

                /// EC2MetadataService throttles by delaying the response so the service client should set a large read timeout.
                /// EC2MetadataService delay is in order of seconds so it only make sense to retry after a couple of seconds.
                aws_client_configuration.connectTimeoutMs = 1000;
                aws_client_configuration.requestTimeoutMs = 1000;
                aws_client_configuration.retryStrategy = std::make_shared<Aws::Client::DefaultRetryStrategy>(1, 1000);

                DB::S3::PocoHTTPClientConfiguration client_configuration(aws_client_configuration, remote_host_filter, s3_max_redirects);
                auto ec2_metadata_client = std::make_shared<Aws::Internal::EC2MetadataClient>(client_configuration);
                auto config_loader = std::make_shared<Aws::Config::EC2InstanceProfileConfigLoader>(ec2_metadata_client);

                AddProvider(std::make_shared<Aws::Auth::InstanceProfileCredentialsProvider>(config_loader));
                LOG_INFO(logger, "Added EC2 metadata service credentials provider to the provider chain.");
            }
        }

        AddProvider(std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(credentials));
    }
};

class S3AuthSigner : public Aws::Client::AWSAuthV4Signer
{
public:
    S3AuthSigner(
        const Aws::Client::ClientConfiguration & client_configuration,
        const Aws::Auth::AWSCredentials & credentials,
        const DB::HeaderCollection & headers_,
        bool use_environment_credentials)
        : Aws::Client::AWSAuthV4Signer(
            std::make_shared<S3CredentialsProviderChain>(
                static_cast<const DB::S3::PocoHTTPClientConfiguration &>(client_configuration),
                credentials,
                use_environment_credentials),
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
    extern const int S3_ERROR;
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
        const String & secret_access_key,
        bool use_environment_credentials,
        const RemoteHostFilter & remote_host_filter,
        unsigned int s3_max_redirects)
    {
        Aws::Client::ClientConfiguration cfg;

        if (!endpoint.empty())
            cfg.endpointOverride = endpoint;

        return create(
            cfg,
            is_virtual_hosted_style,
            access_key_id,
            secret_access_key,
            use_environment_credentials,
            remote_host_filter,
            s3_max_redirects);
    }

    std::shared_ptr<Aws::S3::S3Client> ClientFactory::create( // NOLINT
        const Aws::Client::ClientConfiguration & cfg,
        bool is_virtual_hosted_style,
        const String & access_key_id,
        const String & secret_access_key,
        bool use_environment_credentials,
        const RemoteHostFilter & remote_host_filter,
        unsigned int s3_max_redirects)
    {
        Aws::Auth::AWSCredentials credentials(access_key_id, secret_access_key);

        PocoHTTPClientConfiguration client_configuration(cfg, remote_host_filter, s3_max_redirects);

        client_configuration.updateSchemeAndRegion();

        return std::make_shared<Aws::S3::S3Client>(
            std::make_shared<S3CredentialsProviderChain>(
                client_configuration,
                credentials,
                use_environment_credentials), // AWS credentials provider.
            std::move(client_configuration), // Client configuration.
            Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, // Sign policy.
            is_virtual_hosted_style || cfg.endpointOverride.empty() // Use virtual addressing if endpoint is not specified.
        );
    }

    std::shared_ptr<Aws::S3::S3Client> ClientFactory::create( // NOLINT
        const Aws::Client::ClientConfiguration & cfg,
        bool is_virtual_hosted_style,
        const String & access_key_id,
        const String & secret_access_key,
        HeaderCollection headers,
        bool use_environment_credentials,
        const RemoteHostFilter & remote_host_filter,
        unsigned int s3_max_redirects)
    {
        PocoHTTPClientConfiguration client_configuration(cfg, remote_host_filter, s3_max_redirects);

        client_configuration.updateSchemeAndRegion();

        Aws::Auth::AWSCredentials credentials(access_key_id, secret_access_key);

        auto auth_signer = std::make_shared<S3AuthSigner>(client_configuration, std::move(credentials), std::move(headers), use_environment_credentials);
        return std::make_shared<Aws::S3::S3Client>(
            std::move(auth_signer),
            std::move(client_configuration), // Client configuration.
            is_virtual_hosted_style || client_configuration.endpointOverride.empty() // Use virtual addressing only if endpoint is not specified.
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
                    "Bucket name length is out of bounds in virtual hosted style S3 URI: " + bucket + " (" + uri.toString() + ")",
                    ErrorCodes::BAD_ARGUMENTS);

            if (!uri.getPath().empty())
            {
                /// Remove leading '/' from path to extract key.
                key = uri.getPath().substr(1);
            }

            if (key.empty() || key == "/")
                throw Exception(
                    "Key name is empty in virtual hosted style S3 URI: " + key + " (" + uri.toString() + ")", ErrorCodes::BAD_ARGUMENTS);
            boost::to_upper(name);
            if (name != S3 && name != COS)
            {
                throw Exception(
                    "Object storage system name is unrecognized in virtual hosted style S3 URI: " + name + " (" + uri.toString() + ")",
                    ErrorCodes::BAD_ARGUMENTS);
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
                    "Bucket name length is out of bounds in path style S3 URI: " + bucket + " (" + uri.toString() + ")",
                    ErrorCodes::BAD_ARGUMENTS);

            if (key.empty() || key == "/")
                throw Exception("Key name is empty in path style S3 URI: " + key + " (" + uri.toString() + ")", ErrorCodes::BAD_ARGUMENTS);
        }
        else
            throw Exception("Bucket or key name are invalid in S3 URI: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);
    }

    size_t getObjectSize(std::shared_ptr<Aws::S3::S3Client> client_ptr, const String & bucket, const String & key)
    {
        Aws::S3::Model::HeadObjectRequest req;
        req.SetBucket(bucket);
        req.SetKey(key);

        Aws::S3::Model::HeadObjectOutcome outcome = client_ptr->HeadObject(req);

        if (outcome.IsSuccess())
        {
            auto read_result = outcome.GetResultWithOwnership();
            return static_cast<size_t>(read_result.GetContentLength());
        }
        else
            throw DB::Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
    }
}

}

#endif
