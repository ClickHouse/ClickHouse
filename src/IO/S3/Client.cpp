#include <IO/S3/Client.h>


#if USE_AWS_S3

#include <fstream>

#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>
#include <IO/S3/ClientFactory.h>
#include <Storages/StorageS3Settings.h>

#include <Poco/URI.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <re2/re2.h>

#include <aws/core/Version.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/client/SpecifiedRetryableErrorsRetryStrategy.h>
#include <aws/core/platform/Environment.h>
#include <aws/core/platform/OSVersionInfo.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/UUID.h>
#include <aws/s3/model/ServerSideEncryption.h>


namespace DB::ErrorCodes
{
    extern const int S3_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{

class STSAssumeRoleWebIdentityCredentialsProvider : public Aws::Auth::AWSCredentialsProvider
{
    /// See Aws::Auth::STSAssumeRoleWebIdentityCredentialsProvider.

public:
    explicit STSAssumeRoleWebIdentityCredentialsProvider(DB::S3::ClientConfiguration & aws_client_configuration)
        : logger(&Poco::Logger::get("STSAssumeRoleWebIdentityCredentialsProvider"))
    {
        // check environment variables
        String tmp_region = Aws::Environment::GetEnv("AWS_DEFAULT_REGION");
        role_arn = Aws::Environment::GetEnv("AWS_ROLE_ARN");
        token_file = Aws::Environment::GetEnv("AWS_WEB_IDENTITY_TOKEN_FILE");
        session_name = Aws::Environment::GetEnv("AWS_ROLE_SESSION_NAME");

        // check profile_config if either m_roleArn or m_tokenFile is not loaded from environment variable
        // region source is not enforced, but we need it to construct sts endpoint, if we can't find from environment, we should check if it's set in config file.
        if (role_arn.empty() || token_file.empty() || tmp_region.empty())
        {
            auto profile = Aws::Config::GetCachedConfigProfile(Aws::Auth::GetConfigProfileName());
            if (tmp_region.empty())
            {
                tmp_region = profile.GetRegion();
            }
            // If either of these two were not found from environment, use whatever found for all three in config file
            if (role_arn.empty() || token_file.empty())
            {
                role_arn = profile.GetRoleArn();
                token_file = profile.GetValue("web_identity_token_file");
                session_name = profile.GetValue("role_session_name");
            }
        }

        if (token_file.empty())
        {
            LOG_WARNING(logger, "Token file must be specified to use STS AssumeRole web identity creds provider.");
            return; // No need to do further constructing
        }
        else
        {
            LOG_DEBUG(logger, "Resolved token_file from profile_config or environment variable to be {}", token_file);
        }

        if (role_arn.empty())
        {
            LOG_WARNING(logger, "RoleArn must be specified to use STS AssumeRole web identity creds provider.");
            return; // No need to do further constructing
        }
        else
        {
            LOG_DEBUG(logger, "Resolved role_arn from profile_config or environment variable to be {}", role_arn);
        }

        if (tmp_region.empty())
        {
            tmp_region = Aws::Region::US_EAST_1;
        }
        else
        {
            LOG_DEBUG(logger, "Resolved region from profile_config or environment variable to be {}", tmp_region);
        }

        if (session_name.empty())
        {
            session_name = Aws::Utils::UUID::RandomUUID();
        }
        else
        {
            LOG_DEBUG(logger, "Resolved session_name from profile_config or environment variable to be {}", session_name);
        }

        aws_client_configuration.setScheme(Aws::Http::Scheme::HTTPS);
        aws_client_configuration.setRegion(tmp_region);

        std::vector<String> retryable_errors;
        retryable_errors.push_back("IDPCommunicationError");
        retryable_errors.push_back("InvalidIdentityToken");

        aws_client_configuration.setRetryStrategy(std::make_shared<Aws::Client::SpecifiedRetryableErrorsRetryStrategy>(
            retryable_errors, /* maxRetries = */3));

        client = std::make_unique<Aws::Internal::STSCredentialsClient>(aws_client_configuration.getLegacyConfiguration());
        initialized = true;
        LOG_INFO(logger, "Creating STS AssumeRole with web identity creds provider.");
    }

    Aws::Auth::AWSCredentials GetAWSCredentials() override
    {
        // A valid client means required information like role arn and token file were constructed correctly.
        // We can use this provider to load creds, otherwise, we can just return empty creds.
        if (!initialized)
        {
            return Aws::Auth::AWSCredentials();
        }
        refreshIfExpired();
        Aws::Utils::Threading::ReaderLockGuard guard(m_reloadLock);
        return credentials;
    }

protected:
    void Reload() override
    {
        LOG_INFO(logger, "Credentials have expired, attempting to renew from STS.");

        std::ifstream token_stream(token_file.data());
        if (token_stream)
        {
            String token_string((std::istreambuf_iterator<char>(token_stream)), std::istreambuf_iterator<char>());
            token = token_string;
        }
        else
        {
            LOG_INFO(logger, "Can't open token file: {}", token_file);
            return;
        }
        Aws::Internal::STSCredentialsClient::STSAssumeRoleWithWebIdentityRequest request{session_name, role_arn, token};

        auto result = client->GetAssumeRoleWithWebIdentityCredentials(request);
        LOG_TRACE(logger, "Successfully retrieved credentials.");
        credentials = result.creds;
    }

private:
    void refreshIfExpired()
    {
        Aws::Utils::Threading::ReaderLockGuard guard(m_reloadLock);
        if (!credentials.IsExpiredOrEmpty())
        {
            return;
        }

        guard.UpgradeToWriterLock();
        if (!credentials.IsExpiredOrEmpty()) // double-checked lock to avoid refreshing twice
        {
            return;
        }

        Reload();
    }

    std::unique_ptr<Aws::Internal::STSCredentialsClient> client;
    Aws::Auth::AWSCredentials credentials;
    Aws::String role_arn;
    Aws::String token_file;
    Aws::String session_name;
    Aws::String token;
    bool initialized = false;
    Poco::Logger * logger;
};

class EC2MetadataClient : public Aws::Internal::AWSHttpResourceClient
{
    static constexpr char EC2_SECURITY_CREDENTIALS_RESOURCE[] = "/latest/meta-data/iam/security-credentials";
    static constexpr char EC2_IMDS_TOKEN_RESOURCE[] = "/latest/api/token";
    static constexpr char EC2_IMDS_TOKEN_HEADER[] = "x-aws-ec2-metadata-token";
    static constexpr char EC2_IMDS_TOKEN_TTL_DEFAULT_VALUE[] = "21600";
    static constexpr char EC2_IMDS_TOKEN_TTL_HEADER[] = "x-aws-ec2-metadata-token-ttl-seconds";

    static constexpr char EC2_DEFAULT_METADATA_ENDPOINT[] = "http://169.254.169.254";

public:
    /// See Aws::EC2MetadataClient.

    explicit EC2MetadataClient(const DB::S3::ClientConfiguration & client_configuration)
        : Aws::Internal::AWSHttpResourceClient(client_configuration.getLegacyConfiguration())
        , logger(&Poco::Logger::get("EC2MetadataClient"))
    {
    }

    EC2MetadataClient& operator =(const EC2MetadataClient & rhs) = delete;
    EC2MetadataClient(const EC2MetadataClient & rhs) = delete;
    EC2MetadataClient& operator =(const EC2MetadataClient && rhs) = delete;
    EC2MetadataClient(const EC2MetadataClient && rhs) = delete;

    ~EC2MetadataClient() override = default;

    using Aws::Internal::AWSHttpResourceClient::GetResource;

    virtual Aws::String GetResource(const char * resource_path) const
    {
        return GetResource(endpoint.c_str(), resource_path, nullptr/*authToken*/);
    }

    virtual Aws::String getDefaultCredentials() const
    {
        String credentials_string;
        {
            std::unique_lock<std::recursive_mutex> locker(token_mutex);

            LOG_TRACE(logger, "Getting default credentials for EC2 instance.");
            auto result = GetResourceWithAWSWebServiceResult(endpoint.c_str(), EC2_SECURITY_CREDENTIALS_RESOURCE, nullptr);
            credentials_string = result.GetPayload();
            if (result.GetResponseCode() == Aws::Http::HttpResponseCode::UNAUTHORIZED)
            {
                return {};
            }
        }

        String trimmed_credentials_string = Aws::Utils::StringUtils::Trim(credentials_string.c_str());
        if (trimmed_credentials_string.empty())
            return {};

        std::vector<String> security_credentials = Aws::Utils::StringUtils::Split(trimmed_credentials_string, '\n');

        LOG_DEBUG(logger, "Calling EC2MetadataService resource, {} returned credential string {}.",
                EC2_SECURITY_CREDENTIALS_RESOURCE, trimmed_credentials_string);

        if (security_credentials.empty())
        {
            LOG_WARNING(logger, "Initial call to EC2MetadataService to get credentials failed.");
            return {};
        }

        Aws::StringStream ss;
        ss << EC2_SECURITY_CREDENTIALS_RESOURCE << "/" << security_credentials[0];
        LOG_DEBUG(logger, "Calling EC2MetadataService resource {}.", ss.str());
        return GetResource(ss.str().c_str());
    }

    static Aws::String awsComputeUserAgentString()
    {
        Aws::StringStream ss;
        ss << "aws-sdk-cpp/" << Aws::Version::GetVersionString() << " " << Aws::OSVersionInfo::ComputeOSVersionString()
                << " " << Aws::Version::GetCompilerVersionString();
        return ss.str();
    }

    virtual Aws::String getDefaultCredentialsSecurely() const
    {
        String user_agent_string = awsComputeUserAgentString();
        String new_token;

        {
            std::unique_lock<std::recursive_mutex> locker(token_mutex);

            Aws::StringStream ss;
            ss << endpoint << EC2_IMDS_TOKEN_RESOURCE;
            std::shared_ptr<Aws::Http::HttpRequest> token_request(Aws::Http::CreateHttpRequest(ss.str(), Aws::Http::HttpMethod::HTTP_PUT,
                                                                        Aws::Utils::Stream::DefaultResponseStreamFactoryMethod));
            token_request->SetHeaderValue(EC2_IMDS_TOKEN_TTL_HEADER, EC2_IMDS_TOKEN_TTL_DEFAULT_VALUE);
            token_request->SetUserAgent(user_agent_string);
            LOG_TRACE(logger, "Calling EC2MetadataService to get token.");
            auto result = GetResourceWithAWSWebServiceResult(token_request);
            const String & token_string = result.GetPayload();
            new_token = Aws::Utils::StringUtils::Trim(token_string.c_str());

            if (result.GetResponseCode() == Aws::Http::HttpResponseCode::BAD_REQUEST)
            {
                return {};
            }
            else if (result.GetResponseCode() != Aws::Http::HttpResponseCode::OK || new_token.empty())
            {
                LOG_TRACE(logger, "Calling EC2MetadataService to get token failed, falling back to less secure way.");
                return getDefaultCredentials();
            }
            token = new_token;
        }

        String url = endpoint + EC2_SECURITY_CREDENTIALS_RESOURCE;
        std::shared_ptr<Aws::Http::HttpRequest> profile_request(Aws::Http::CreateHttpRequest(url,
                Aws::Http::HttpMethod::HTTP_GET,
                Aws::Utils::Stream::DefaultResponseStreamFactoryMethod));
        profile_request->SetHeaderValue(EC2_IMDS_TOKEN_HEADER, new_token);
        profile_request->SetUserAgent(user_agent_string);
        String profile_string = GetResourceWithAWSWebServiceResult(profile_request).GetPayload();

        String trimmed_profile_string = Aws::Utils::StringUtils::Trim(profile_string.c_str());
        std::vector<String> security_credentials = Aws::Utils::StringUtils::Split(trimmed_profile_string, '\n');

        LOG_DEBUG(logger, "Calling EC2MetadataService resource, {} with token returned profile string {}.",
                EC2_SECURITY_CREDENTIALS_RESOURCE, trimmed_profile_string);

        if (security_credentials.empty())
        {
            LOG_WARNING(logger, "Calling EC2Metadataservice to get profiles failed.");
            return {};
        }

        Aws::StringStream ss;
        ss << endpoint << EC2_SECURITY_CREDENTIALS_RESOURCE << "/" << security_credentials[0];
        std::shared_ptr<Aws::Http::HttpRequest> credentials_request(Aws::Http::CreateHttpRequest(ss.str(),
                Aws::Http::HttpMethod::HTTP_GET,
                Aws::Utils::Stream::DefaultResponseStreamFactoryMethod));
        credentials_request->SetHeaderValue(EC2_IMDS_TOKEN_HEADER, new_token);
        credentials_request->SetUserAgent(user_agent_string);
        LOG_DEBUG(logger, "Calling EC2MetadataService resource {} with token.", ss.str());
        return GetResourceWithAWSWebServiceResult(credentials_request).GetPayload();
    }

    virtual Aws::String getCurrentRegion() const
    {
        return Aws::Region::AWS_GLOBAL;
    }

private:
    const Aws::String endpoint = EC2_DEFAULT_METADATA_ENDPOINT;
    mutable std::recursive_mutex token_mutex;
    mutable Aws::String token;
    Poco::Logger * logger;
};

class EC2InstanceProfileConfigLoader : public Aws::Config::AWSProfileConfigLoader
{
public:
    explicit EC2InstanceProfileConfigLoader(const std::shared_ptr<EC2MetadataClient> & client_, bool use_secure_pull_)
        : client(client_)
        , use_secure_pull(use_secure_pull_)
        , logger(&Poco::Logger::get("EC2InstanceProfileConfigLoader"))
    {
    }

    ~EC2InstanceProfileConfigLoader() override = default;

protected:
    bool LoadInternal() override
    {
        auto credentials_str = use_secure_pull ? client->getDefaultCredentialsSecurely() : client->getDefaultCredentials();

        /// See EC2InstanceProfileConfigLoader.
        if (credentials_str.empty())
            return false;

        Aws::Utils::Json::JsonValue credentials_doc(credentials_str);
        if (!credentials_doc.WasParseSuccessful())
        {
            LOG_ERROR(logger, "Failed to parse output from EC2MetadataService.");
            return false;
        }
        String access_key, secret_key, token;

        auto credentials_view = credentials_doc.View();
        access_key = credentials_view.GetString("AccessKeyId");
        LOG_TRACE(logger, "Successfully pulled credentials from EC2MetadataService with access key.");

        secret_key = credentials_view.GetString("SecretAccessKey");
        token = credentials_view.GetString("Token");

        auto region = client->getCurrentRegion();

        Aws::Config::Profile profile;
        profile.SetCredentials(Aws::Auth::AWSCredentials(access_key, secret_key, token));
        profile.SetRegion(region);
        profile.SetName(Aws::Config::INSTANCE_PROFILE_KEY);

        m_profiles[Aws::Config::INSTANCE_PROFILE_KEY] = profile;

        return true;
    }

private:
    std::shared_ptr<EC2MetadataClient> client;
    bool use_secure_pull;
    Poco::Logger * logger;
};

class InstanceProfileCredentialsProvider : public Aws::Auth::AWSCredentialsProvider
{
public:
    /// See Aws::InstanceProfileCredentialsProvider.

    explicit InstanceProfileCredentialsProvider(const std::shared_ptr<EC2InstanceProfileConfigLoader> & config_loader)
        : ec2_metadata_config_loader(config_loader)
        , load_frequency_ms(Aws::Auth::REFRESH_THRESHOLD)
        , logger(&Poco::Logger::get("AWSInstanceProfileCredentialsProvider"))
    {
        LOG_INFO(logger, "Creating Instance with injected EC2MetadataClient and refresh rate.");
    }

    Aws::Auth::AWSCredentials GetAWSCredentials() override
    {
        refreshIfExpired();
        Aws::Utils::Threading::ReaderLockGuard guard(m_reloadLock);
        auto profile_it = ec2_metadata_config_loader->GetProfiles().find(Aws::Config::INSTANCE_PROFILE_KEY);

        if (profile_it != ec2_metadata_config_loader->GetProfiles().end())
        {
            return profile_it->second.GetCredentials();
        }

        return Aws::Auth::AWSCredentials();
    }

protected:
    void Reload() override
    {
        LOG_INFO(logger, "Credentials have expired attempting to repull from EC2 Metadata Service.");
        ec2_metadata_config_loader->Load();
        AWSCredentialsProvider::Reload();
    }

private:
    void refreshIfExpired()
    {
        LOG_DEBUG(logger, "Checking if latest credential pull has expired.");
        Aws::Utils::Threading::ReaderLockGuard guard(m_reloadLock);
        if (!IsTimeToRefresh(load_frequency_ms))
        {
            return;
        }

        guard.UpgradeToWriterLock();
        if (!IsTimeToRefresh(load_frequency_ms)) // double-checked lock to avoid refreshing twice
        {
            return;
        }
        Reload();
    }

    std::shared_ptr<EC2InstanceProfileConfigLoader> ec2_metadata_config_loader;
    Int64 load_frequency_ms;
    Poco::Logger * logger;
};

class CredentialsProviderChain : public Aws::Auth::AWSCredentialsProviderChain
{
public:
    explicit CredentialsProviderChain(const DB::S3::ClientConfiguration & configuration)
    {
        auto * logger = &Poco::Logger::get("CredentialsProviderChain");

        if (configuration.getUseEnvironmentCredentials())
        {
            static const char AWS_ECS_CONTAINER_CREDENTIALS_RELATIVE_URI[] = "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI";
            static const char AWS_ECS_CONTAINER_CREDENTIALS_FULL_URI[] = "AWS_CONTAINER_CREDENTIALS_FULL_URI";
            static const char AWS_ECS_CONTAINER_AUTHORIZATION_TOKEN[] = "AWS_CONTAINER_AUTHORIZATION_TOKEN";
            static const char AWS_EC2_METADATA_DISABLED[] = "AWS_EC2_METADATA_DISABLED";

            /// The only difference from DefaultAWSCredentialsProviderChain::DefaultAWSCredentialsProviderChain()
            /// is that this chain uses custom ClientConfiguration. Also we removed process provider because it's useless in our case.
            ///
            /// AWS API tries credentials providers one by one. Some of providers (like ProfileConfigFileAWSCredentialsProvider) can be
            /// quite verbose even if nobody configured them. So we use our provider first and only after it use default providers.
            {
                DB::S3::ClientConfiguration aws_client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(configuration.getRemoteHostFilter());
                aws_client_configuration.setEnableRequestsLogging(configuration.getEnableRequestsLogging());
                AddProvider(std::make_shared<STSAssumeRoleWebIdentityCredentialsProvider>(aws_client_configuration));
            }

            AddProvider(std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>());


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
                /// See `MakeDefaultHttpResourceClientConfiguration()`.
                /// This is part of EC2 metadata client, but unfortunately it can't be accessed from outside
                /// of contrib/aws/aws-cpp-sdk-core/source/internal/AWSHttpResourceClient.cpp

                DB::S3::ClientConfiguration aws_client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(configuration.getRemoteHostFilter());

                aws_client_configuration.setEnableRequestsLogging(configuration.getEnableRequestsLogging());
                aws_client_configuration.setMaxConnections(2);
                aws_client_configuration.setScheme(Aws::Http::Scheme::HTTP);

                /// Explicitly set the proxy settings to empty/zero to avoid relying on defaults that could potentially change
                /// in the future.
                aws_client_configuration.setProxyHost("");
                aws_client_configuration.setProxyUserName("");
                aws_client_configuration.setProxyPassword("");
                aws_client_configuration.setProxyPort(0);

                /// EC2MetadataService throttles by delaying the response so the service client should set a large read timeout.
                /// EC2MetadataService delay is in order of seconds so it only make sense to retry after a couple of seconds.
                aws_client_configuration.setConnectTimeoutMs(1000);
                aws_client_configuration.setRequestTimeoutMs(1000);

                aws_client_configuration.setRetryStrategy(std::make_shared<Aws::Client::DefaultRetryStrategy>(1, 1000));

                auto ec2_metadata_client = std::make_shared<EC2MetadataClient>(aws_client_configuration);
                auto config_loader = std::make_shared<EC2InstanceProfileConfigLoader>(ec2_metadata_client, !configuration.getUseInsecureImdsRequest());

                AddProvider(std::make_shared<InstanceProfileCredentialsProvider>(config_loader));
                LOG_INFO(logger, "Added EC2 metadata service credentials provider to the provider chain.");
            }
        }

        AddProvider(std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(configuration.getCredentials()));

        /// Quite verbose provider (argues if file with credentials doesn't exist) so iut's the last one
        /// in chain.
        AddProvider(std::make_shared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>());
    }
};

}


namespace DB::S3
{

URI::URI(const String & uri_):
    URI(Poco::URI(uri_))
{
}

URI::URI(const Poco::URI & uri_)
{
    /// Case when bucket name represented in domain name of S3 URL.
    /// E.g. (https://bucket-name.s3.Region.amazonaws.com/key)
    /// https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html#virtual-hosted-style-access
    static const RE2 virtual_hosted_style_pattern(R"((.+)\.(s3|cos|obs|oss)([.\-][a-z0-9\-.:]+))");

    /// Case when bucket name and key represented in path of S3 URL.
    /// E.g. (https://s3.Region.amazonaws.com/bucket-name/key)
    /// https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html#path-style-access
    static const RE2 path_style_pattern("^/([^/]*)/(.*)");

    static constexpr auto S3 = "S3";
    static constexpr auto COSN = "COSN";
    static constexpr auto COS = "COS";
    static constexpr auto OBS = "OBS";
    static constexpr auto OSS = "OSS";

    uri = uri_;
    storage_name = S3;

    if (uri.getHost().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Host is empty in S3 URI.");

    /// Extract object version ID from query string.
    {
        version_id = "";
        const String version_key = "versionId=";
        const auto query_string = uri.getQuery();

        auto start = query_string.rfind(version_key);
        if (start != std::string::npos)
        {
            start += version_key.length();
            auto end = query_string.find_first_of('&', start);
            version_id = query_string.substr(start, end == std::string::npos ? std::string::npos : end - start);
        }
    }

    {
        static const RE2 aws_region_pattern(R"(s3[.\-]([a-z0-9\-]+)\.amazonaws\.)");
        static const RE2 cos_region_pattern(R"(cos[.\-]([a-z0-9\-]+)\.myqcloud\.)");

        String matched_region;
        if (re2::RE2::PartialMatch(uri.getHost(), aws_region_pattern, &matched_region))
        {
            region = boost::algorithm::to_lower_copy(matched_region);
        }
        else if (re2::RE2::PartialMatch(uri.getHost(), cos_region_pattern, &matched_region))
        {
            region = boost::algorithm::to_lower_copy(matched_region);
        }
    }

    String name;
    String endpoint_authority_from_uri;

    if (re2::RE2::FullMatch(uri.getAuthority(), virtual_hosted_style_pattern, &bucket, &name, &endpoint_authority_from_uri))
    {
        is_virtual_hosted_style = true;
        endpoint = uri.getScheme() + "://" + name + endpoint_authority_from_uri;
        validateBucket(bucket, uri);

        if (!uri.getPath().empty())
        {
            /// Remove leading '/' from path to extract key.
            key = uri.getPath().substr(1);
        }

        boost::to_upper(name);
        if (name != S3 && name != COS && name != OBS && name != OSS)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Object storage system name is unrecognized in virtual hosted style S3 URI: {}", quoteString(name));
        }
        if (name == S3)
        {
            storage_name = name;
        }
        else if (name == OBS)
        {
            storage_name = OBS;
        }
        else if (name == OSS)
        {
            storage_name = OSS;
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
        validateBucket(bucket, uri);
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bucket or key name are invalid in S3 URI.");
}

void URI::validateBucket(const String & bucket, const Poco::URI & uri)
{
    /// S3 specification requires at least 3 and at most 63 characters in bucket name.
    /// https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-s3-bucket-naming-requirements.html
    if (bucket.length() < 3 || bucket.length() > 63)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bucket name length is out of bounds in virtual hosted style S3 URI:     {}{}",
                        quoteString(bucket), !uri.empty() ? " (" + uri.toString() + ")" : "");
}


ClientConfiguration::ClientConfiguration(const RemoteHostFilter & remote_host_filter_)
    : remote_host_filter(remote_host_filter_)
{
}

const Aws::Client::ClientConfiguration & ClientConfiguration::getLegacyConfiguration() const
{
    /// This reference will be later downcasted back to `ClientConfiguration`.
    return *this;
}

void ClientConfiguration::setUseInsecureImdsRequest(bool value)
{
    use_insecure_imds_request = value;
}

void ClientConfiguration::setUseEnvironmentCredentials(bool value)
{
    use_environment_credentials = value;
}

void ClientConfiguration::setEnableRequestsLogging(bool value)
{
    enable_requests_logging = value;
}

void ClientConfiguration::setExtraHeaders(HeaderCollection extra_headers_)
{
    extra_headers = std::move(extra_headers_);
}

void ClientConfiguration::setCredentials(String access_key_id_, String secret_access_key_)
{
    access_key_id = std::move(access_key_id_);
    secret_access_key = std::move(secret_access_key_);
}

void ClientConfiguration::setMaxConnections(size_t max_connections)
{
    maxConnections = max_connections;
}

void ClientConfiguration::setMaxRedirects(size_t max_redirects_)
{
    max_redirects = max_redirects_;
}

void ClientConfiguration::setRegion(String region_)
{
    region = std::move(region_);
}

void ClientConfiguration::setRegionOverride(String region_override_)
{
    region_override = std::move(region_override_);
}

void ClientConfiguration::setEndpointOverride(String endpoint_override)
{
    endpointOverride = std::move(endpoint_override);
}

void ClientConfiguration::setConnectTimeoutMs(size_t value)
{
    connectTimeoutMs = value;
}

void ClientConfiguration::setRequestTimeoutMs(size_t value)
{
    requestTimeoutMs = value;
}

void ClientConfiguration::setServerSideEncryptionCustomerKeyBase64(String server_side_encryption_customer_key_base64_)
{
    server_side_encryption_customer_key_base64 = server_side_encryption_customer_key_base64_;
}

void ClientConfiguration::setErrorReport(ReportProvider error_report_)
{
    error_report = error_report_;
}

void ClientConfiguration::setRetryStrategy(std::shared_ptr<Aws::Client::RetryStrategy> retry_strategy_)
{
    retryStrategy = retry_strategy_;
}

void ClientConfiguration::setPerRequestConfiguration(PerRequestConfigurationFetcher per_request_configuration_)
{
    per_request_configuration = per_request_configuration_;
}

void ClientConfiguration::setScheme(Aws::Http::Scheme scheme_)
{
    scheme = scheme_;
}

void ClientConfiguration::setProxyHost(String proxy_host)
{
    proxyHost = std::move(proxy_host);
}

void ClientConfiguration::setProxyUserName(String proxy_user_name)
{
    proxyUserName = std::move(proxy_user_name);
}

void ClientConfiguration::setProxyPassword(String proxy_password)
{
    proxyPassword = std::move(proxy_password);
}

void ClientConfiguration::setProxyPort(unsigned short port)
{
    proxyPort = port;
}

bool ClientConfiguration::getUseInsecureImdsRequest() const
{
    return use_insecure_imds_request;
}

bool ClientConfiguration::getUseEnvironmentCredentials() const
{
    return use_environment_credentials;
}

ClientConfiguration::ReportProvider ClientConfiguration::getErrorReport() const
{
    return error_report;
}

ClientConfiguration::PerRequestConfigurationFetcher ClientConfiguration::getPerRequestConfiguration() const
{
    return per_request_configuration;
}

String ClientConfiguration::getRegion() const
{
    return region;
}

String ClientConfiguration::getRegionOverride() const
{
    return region_override;
}

bool ClientConfiguration::getEnableRequestsLogging() const
{
    return enable_requests_logging;
}

size_t ClientConfiguration::getMaxRedirects() const
{
    return max_redirects;
}

size_t ClientConfiguration::getMaxConnections() const
{
    return maxConnections;
}

size_t ClientConfiguration::getConnectTimeoutMs() const
{
    return connectTimeoutMs;
}

size_t ClientConfiguration::getRequestTimeoutMs() const
{
    return requestTimeoutMs;
}

const RemoteHostFilter & ClientConfiguration::getRemoteHostFilter() const
{
    return remote_host_filter;
}

HeaderCollection ClientConfiguration::getExtraHeaders() const
{
    return extra_headers;
}

String ClientConfiguration::getServerSideEncryptionCustomerKeyBase64() const
{
    return server_side_encryption_customer_key_base64;
}

Aws::Auth::AWSCredentials ClientConfiguration::getCredentials() const
{
    return Aws::Auth::AWSCredentials(access_key_id, secret_access_key);
}

Aws::Http::Scheme ClientConfiguration::getScheme() const
{
    return scheme;
}


Client::Client(const ClientConfiguration & client_configuration_, const URI & uri)
    : client_configuration(client_configuration_)
{
    if (uri.uri.getScheme() == "http")
        client_configuration.setScheme(Aws::Http::Scheme::HTTP);
    else
        client_configuration.setScheme(Aws::Http::Scheme::HTTPS);

    client_configuration.setEndpointOverride(uri.endpoint);

    if (client_configuration.getRegionOverride().empty())
    {
        if (!uri.region.empty())
            client_configuration.setRegion(uri.region);
        else
            /// In global mode AWS C++ SDK send `us-east-1` but accept switching to another one if being suggested.
            client_configuration.setRegion(Aws::Region::AWS_GLOBAL);
    }
    else
    {
        client_configuration.setRegion(client_configuration.getRegionOverride());
    }

    if (!client_configuration.getServerSideEncryptionCustomerKeyBase64().empty())
    {
        if (client_configuration.getScheme() == Aws::Http::Scheme::HTTP)
        {
            //throw Exception(ErrorCodes::BAD_ARGUMENTS, "Using server-side encryption with HTTP scheme is discouraged");
        }

        Aws::Utils::ByteBuffer buffer = Aws::Utils::HashingUtils::Base64Decode(client_configuration.getServerSideEncryptionCustomerKeyBase64());
        String server_side_encryption_customer_key = String(reinterpret_cast<char *>(buffer.GetUnderlyingData()), buffer.GetLength());
        server_side_encryption_customer_key_base64 = Aws::Utils::HashingUtils::Base64Encode(buffer);
        server_side_encryption_customer_key_md5_base64 = Aws::Utils::HashingUtils::Base64Encode(Aws::Utils::HashingUtils::CalculateMD5(server_side_encryption_customer_key));
        server_side_encryption_customer_algorithm = Aws::S3::Model::ServerSideEncryptionMapper::GetNameForServerSideEncryption(Aws::S3::Model::ServerSideEncryption::AES256);
    }

    auto credentials_provider = std::make_shared<CredentialsProviderChain>(client_configuration);
    s3 = std::make_unique<Aws::S3::S3Client>(std::move(credentials_provider), client_configuration.getLegacyConfiguration(), Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, uri.is_virtual_hosted_style);
}

template<typename Request>
void Client::setServerSideEncryptionIfNeeded(Request & request) const
{
    if (!server_side_encryption_customer_key_base64.empty())
    {
        request.SetSSECustomerKey(server_side_encryption_customer_key_base64);
        request.SetSSECustomerKeyMD5(server_side_encryption_customer_key_md5_base64);
        request.SetSSECustomerAlgorithm(server_side_encryption_customer_algorithm);
    }
}

template<typename Request>
void Client::setServerSideEncryptionIfNeeded(Request & request, std::shared_ptr<const Client> src_client) const
{
    if (!server_side_encryption_customer_key_base64.empty())
    {
        request.SetSSECustomerKey(server_side_encryption_customer_key_base64);
        request.SetSSECustomerKeyMD5(server_side_encryption_customer_key_md5_base64);
        request.SetSSECustomerAlgorithm(server_side_encryption_customer_algorithm);
        if (src_client)
        {
            request.SetCopySourceSSECustomerKey(src_client->server_side_encryption_customer_key_base64);
            request.SetCopySourceSSECustomerKeyMD5(src_client->server_side_encryption_customer_key_md5_base64);
            request.SetCopySourceSSECustomerAlgorithm(src_client->server_side_encryption_customer_algorithm);
        }
        else
        {
            request.SetCopySourceSSECustomerKey(server_side_encryption_customer_key_base64);
            request.SetCopySourceSSECustomerKeyMD5(server_side_encryption_customer_key_md5_base64);
            request.SetCopySourceSSECustomerAlgorithm(server_side_encryption_customer_algorithm);
        }
    }
}

size_t Client::getObjectSize(const String & bucket, const String & key, const String & version_id, bool throw_on_error) const
{
    Aws::S3::Model::HeadObjectRequest req;
    req.SetBucket(bucket);
    req.SetKey(key);

    if (!version_id.empty())
        req.SetVersionId(version_id);

    Aws::S3::Model::HeadObjectOutcome outcome = headObject(req);

    if (outcome.IsSuccess())
    {
        auto read_result = outcome.GetResultWithOwnership();
        return static_cast<size_t>(read_result.GetContentLength());
    }
    else if (throw_on_error)
    {
        throw Exception(outcome.GetError().GetMessage(), ErrorCodes::S3_ERROR);
    }
    return 0;
}

Aws::S3::Model::CopyObjectOutcome Client::copyObject(Aws::S3::Model::CopyObjectRequest & request, std::shared_ptr<const Client> src_client) const
{
    setServerSideEncryptionIfNeeded(request, src_client);
    return s3->CopyObject(request);
}

Aws::S3::Model::DeleteObjectOutcome Client::deleteObject(Aws::S3::Model::DeleteObjectRequest & request) const
{
    return s3->DeleteObject(request);
}

Aws::S3::Model::DeleteObjectsOutcome Client::deleteObjects(Aws::S3::Model::DeleteObjectsRequest & request) const
{
    return s3->DeleteObjects(request);
}

Aws::S3::Model::GetObjectOutcome Client::getObject(Aws::S3::Model::GetObjectRequest & request) const
{
    setServerSideEncryptionIfNeeded(request);
    return s3->GetObject(request);
}

Aws::S3::Model::HeadObjectOutcome Client::headObject(Aws::S3::Model::HeadObjectRequest & request) const
{
    setServerSideEncryptionIfNeeded(request);
    return s3->HeadObject(request);
}

Aws::S3::Model::ListObjectsV2Outcome Client::listObjectsV2(Aws::S3::Model::ListObjectsV2Request & request) const
{
    return s3->ListObjectsV2(request);
}

Aws::S3::Model::PutObjectOutcome Client::putObject(Aws::S3::Model::PutObjectRequest & request) const
{
    setServerSideEncryptionIfNeeded(request);
    return s3->PutObject(request);
}

Aws::S3::Model::CreateMultipartUploadOutcome Client::createMultipartUpload(Aws::S3::Model::CreateMultipartUploadRequest & request) const
{
    setServerSideEncryptionIfNeeded(request);
    return s3->CreateMultipartUpload(request);
}

Aws::S3::Model::UploadPartOutcome Client::uploadPart(Aws::S3::Model::UploadPartRequest & request) const
{
    setServerSideEncryptionIfNeeded(request);
    return s3->UploadPart(request);
}

Aws::S3::Model::UploadPartCopyOutcome Client::uploadPartCopy(Aws::S3::Model::UploadPartCopyRequest & request, std::shared_ptr<const Client> src_client) const
{
    setServerSideEncryptionIfNeeded(request, src_client);
    return s3->UploadPartCopy(request);
}

Aws::S3::Model::CompleteMultipartUploadOutcome Client::completeMultipartUpload(const Aws::S3::Model::CompleteMultipartUploadRequest & request) const
{
    return s3->CompleteMultipartUpload(request);
}

Aws::S3::Model::AbortMultipartUploadOutcome Client::abortMultipartUpload(const Aws::S3::Model::AbortMultipartUploadRequest & request) const
{
    return s3->AbortMultipartUpload(request);
}

void Client::disableRequestProcessing()
{
    s3->DisableRequestProcessing();
}

void Client::enableRequestProcessing()
{
    s3->EnableRequestProcessing();
}

}

#endif
