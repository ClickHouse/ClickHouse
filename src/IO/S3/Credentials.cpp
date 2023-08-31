#include <IO/S3/Credentials.h>

#if USE_AWS_S3

#    include <aws/core/Version.h>
#    include <aws/core/platform/OSVersionInfo.h>
#    include <aws/core/auth/STSCredentialsProvider.h>
#    include <aws/core/platform/Environment.h>
#    include <aws/core/client/SpecifiedRetryableErrorsRetryStrategy.h>
#    include <aws/core/utils/json/JsonSerializer.h>
#    include <aws/core/utils/UUID.h>
#    include <aws/core/http/HttpClientFactory.h>

#    include <Common/logger_useful.h>

#    include <IO/S3/PocoHTTPClient.h>
#    include <IO/S3/Client.h>

#    include <fstream>
#    include <base/EnumReflection.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int AWS_ERROR;
}

namespace S3
{

namespace
{

bool areCredentialsEmptyOrExpired(const Aws::Auth::AWSCredentials & credentials, uint64_t expiration_window_seconds)
{
    if (credentials.IsEmpty())
        return true;

    const Aws::Utils::DateTime now = Aws::Utils::DateTime::Now();
    return now >= credentials.GetExpiration() - std::chrono::seconds(expiration_window_seconds);
}

}

AWSEC2MetadataClient::AWSEC2MetadataClient(const Aws::Client::ClientConfiguration & client_configuration, const char * endpoint_)
    : Aws::Internal::AWSHttpResourceClient(client_configuration)
    , endpoint(endpoint_)
    , logger(&Poco::Logger::get("AWSEC2InstanceProfileConfigLoader"))
{
}

Aws::String AWSEC2MetadataClient::GetResource(const char * resource_path) const
{
    return GetResource(endpoint.c_str(), resource_path, nullptr/*authToken*/);
}

Aws::String AWSEC2MetadataClient::getDefaultCredentials() const
{
    String credentials_string;
    {
        std::lock_guard locker(token_mutex);

        LOG_TRACE(logger, "Getting default credentials for ec2 instance from {}", endpoint);
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

Aws::String AWSEC2MetadataClient::awsComputeUserAgentString()
{
    Aws::StringStream ss;
    ss << "aws-sdk-cpp/" << Aws::Version::GetVersionString() << " " << Aws::OSVersionInfo::ComputeOSVersionString()
            << " " << Aws::Version::GetCompilerVersionString();
    return ss.str();
}

Aws::String AWSEC2MetadataClient::getDefaultCredentialsSecurely() const
{
    String user_agent_string = awsComputeUserAgentString();
    auto [new_token, response_code] = getEC2MetadataToken(user_agent_string);
    if (response_code == Aws::Http::HttpResponseCode::BAD_REQUEST)
        return {};
    else if (response_code != Aws::Http::HttpResponseCode::OK || new_token.empty())
    {
        LOG_TRACE(logger, "Calling EC2MetadataService to get token failed, "
                  "falling back to less secure way. HTTP response code: {}", response_code);
        return getDefaultCredentials();
    }

    token = std::move(new_token);
    String url = endpoint + EC2_SECURITY_CREDENTIALS_RESOURCE;
    std::shared_ptr<Aws::Http::HttpRequest> profile_request(Aws::Http::CreateHttpRequest(url,
            Aws::Http::HttpMethod::HTTP_GET,
            Aws::Utils::Stream::DefaultResponseStreamFactoryMethod));
    profile_request->SetHeaderValue(EC2_IMDS_TOKEN_HEADER, token);
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
    credentials_request->SetHeaderValue(EC2_IMDS_TOKEN_HEADER, token);
    credentials_request->SetUserAgent(user_agent_string);
    LOG_DEBUG(logger, "Calling EC2MetadataService resource {} with token.", ss.str());
    return GetResourceWithAWSWebServiceResult(credentials_request).GetPayload();
}

Aws::String AWSEC2MetadataClient::getCurrentAvailabilityZone() const
{
    String user_agent_string = awsComputeUserAgentString();
    auto [new_token, response_code] = getEC2MetadataToken(user_agent_string);
    if (response_code != Aws::Http::HttpResponseCode::OK || new_token.empty())
        throw DB::Exception(ErrorCodes::AWS_ERROR,
            "Failed to make token request. HTTP response code: {}", response_code);

    token = std::move(new_token);
    const String url = endpoint + EC2_AVAILABILITY_ZONE_RESOURCE;
    std::shared_ptr<Aws::Http::HttpRequest> profile_request(
        Aws::Http::CreateHttpRequest(url, Aws::Http::HttpMethod::HTTP_GET, Aws::Utils::Stream::DefaultResponseStreamFactoryMethod));

    profile_request->SetHeaderValue(EC2_IMDS_TOKEN_HEADER, token);
    profile_request->SetUserAgent(user_agent_string);

    const auto result = GetResourceWithAWSWebServiceResult(profile_request);
    if (result.GetResponseCode() != Aws::Http::HttpResponseCode::OK)
        throw DB::Exception(ErrorCodes::AWS_ERROR,
            "Failed to get availability zone. HTTP response code: {}", result.GetResponseCode());

    return Aws::Utils::StringUtils::Trim(result.GetPayload().c_str());
}

std::pair<Aws::String, Aws::Http::HttpResponseCode> AWSEC2MetadataClient::getEC2MetadataToken(const std::string & user_agent_string) const
{
    std::lock_guard locker(token_mutex);

    Aws::StringStream ss;
    ss << endpoint << EC2_IMDS_TOKEN_RESOURCE;
    std::shared_ptr<Aws::Http::HttpRequest> token_request(
        Aws::Http::CreateHttpRequest(
            ss.str(), Aws::Http::HttpMethod::HTTP_PUT,
            Aws::Utils::Stream::DefaultResponseStreamFactoryMethod));
    token_request->SetHeaderValue(EC2_IMDS_TOKEN_TTL_HEADER, EC2_IMDS_TOKEN_TTL_DEFAULT_VALUE);
    token_request->SetUserAgent(user_agent_string);

    LOG_TRACE(logger, "Calling EC2MetadataService to get token.");
    const auto result = GetResourceWithAWSWebServiceResult(token_request);
    const auto & token_string = result.GetPayload();
    return { Aws::Utils::StringUtils::Trim(token_string.c_str()), result.GetResponseCode() };
}

Aws::String AWSEC2MetadataClient::getCurrentRegion() const
{
    return Aws::Region::AWS_GLOBAL;
}

std::shared_ptr<AWSEC2MetadataClient> InitEC2MetadataClient(const Aws::Client::ClientConfiguration & client_configuration)
{
    Aws::String ec2_metadata_service_endpoint = Aws::Environment::GetEnv("AWS_EC2_METADATA_SERVICE_ENDPOINT");
    auto * logger = &Poco::Logger::get("AWSEC2InstanceProfileConfigLoader");
    if (ec2_metadata_service_endpoint.empty())
    {
        Aws::String ec2_metadata_service_endpoint_mode = Aws::Environment::GetEnv("AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE");
        if (ec2_metadata_service_endpoint_mode.length() == 0)
        {
            ec2_metadata_service_endpoint = "http://169.254.169.254"; //default to IPv4 default endpoint
        }
        else
        {
            if (ec2_metadata_service_endpoint_mode.length() == 4)
            {
                if (Aws::Utils::StringUtils::CaselessCompare(ec2_metadata_service_endpoint_mode.c_str(), "ipv4"))
                {
                    ec2_metadata_service_endpoint = "http://169.254.169.254"; //default to IPv4 default endpoint
                }
                else if (Aws::Utils::StringUtils::CaselessCompare(ec2_metadata_service_endpoint_mode.c_str(), "ipv6"))
                {
                    ec2_metadata_service_endpoint = "http://[fd00:ec2::254]";
                }
                else
                {
                    LOG_ERROR(logger, "AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE can only be set to ipv4 or ipv6, received: {}", ec2_metadata_service_endpoint_mode);
                }
            }
            else
            {
                LOG_ERROR(logger, "AWS_EC2_METADATA_SERVICE_ENDPOINT_MODE can only be set to ipv4 or ipv6, received: {}", ec2_metadata_service_endpoint_mode);
            }
        }
    }
    LOG_INFO(logger, "Using IMDS endpoint: {}", ec2_metadata_service_endpoint);
    return std::make_shared<AWSEC2MetadataClient>(client_configuration, ec2_metadata_service_endpoint.c_str());
}

AWSEC2InstanceProfileConfigLoader::AWSEC2InstanceProfileConfigLoader(const std::shared_ptr<AWSEC2MetadataClient> & client_, bool use_secure_pull_)
    : client(client_)
    , use_secure_pull(use_secure_pull_)
    , logger(&Poco::Logger::get("AWSEC2InstanceProfileConfigLoader"))
{
}

bool AWSEC2InstanceProfileConfigLoader::LoadInternal()
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

AWSInstanceProfileCredentialsProvider::AWSInstanceProfileCredentialsProvider(const std::shared_ptr<AWSEC2InstanceProfileConfigLoader> & config_loader)
    : ec2_metadata_config_loader(config_loader)
    , load_frequency_ms(Aws::Auth::REFRESH_THRESHOLD)
    , logger(&Poco::Logger::get("AWSInstanceProfileCredentialsProvider"))
{
    LOG_INFO(logger, "Creating Instance with injected EC2MetadataClient and refresh rate.");
}

Aws::Auth::AWSCredentials AWSInstanceProfileCredentialsProvider::GetAWSCredentials()
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

void AWSInstanceProfileCredentialsProvider::Reload()
{
    LOG_INFO(logger, "Credentials have expired attempting to repull from EC2 Metadata Service.");
    ec2_metadata_config_loader->Load();
    AWSCredentialsProvider::Reload();
}

void AWSInstanceProfileCredentialsProvider::refreshIfExpired()
{
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

AwsAuthSTSAssumeRoleWebIdentityCredentialsProvider::AwsAuthSTSAssumeRoleWebIdentityCredentialsProvider(
    DB::S3::PocoHTTPClientConfiguration & aws_client_configuration, uint64_t expiration_window_seconds_)
    : logger(&Poco::Logger::get("AwsAuthSTSAssumeRoleWebIdentityCredentialsProvider"))
    , expiration_window_seconds(expiration_window_seconds_)
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

    aws_client_configuration.scheme = Aws::Http::Scheme::HTTPS;
    aws_client_configuration.region = tmp_region;

    std::vector<String> retryable_errors;
    retryable_errors.push_back("IDPCommunicationError");
    retryable_errors.push_back("InvalidIdentityToken");

    aws_client_configuration.retryStrategy = std::make_shared<Aws::Client::SpecifiedRetryableErrorsRetryStrategy>(
        retryable_errors, /* maxRetries = */3);

    client = std::make_unique<Aws::Internal::STSCredentialsClient>(aws_client_configuration);
    initialized = true;
    LOG_INFO(logger, "Creating STS AssumeRole with web identity creds provider.");
}

Aws::Auth::AWSCredentials AwsAuthSTSAssumeRoleWebIdentityCredentialsProvider::GetAWSCredentials()
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

void AwsAuthSTSAssumeRoleWebIdentityCredentialsProvider::Reload()
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

void AwsAuthSTSAssumeRoleWebIdentityCredentialsProvider::refreshIfExpired()
{
    Aws::Utils::Threading::ReaderLockGuard guard(m_reloadLock);
    if (!areCredentialsEmptyOrExpired(credentials, expiration_window_seconds))
        return;

    guard.UpgradeToWriterLock();
    if (!areCredentialsEmptyOrExpired(credentials, expiration_window_seconds)) // double-checked lock to avoid refreshing twice
        return;

    Reload();
}

S3CredentialsProviderChain::S3CredentialsProviderChain(
        const DB::S3::PocoHTTPClientConfiguration & configuration,
        const Aws::Auth::AWSCredentials & credentials,
        CredentialsConfiguration credentials_configuration)
{
    auto * logger = &Poco::Logger::get("S3CredentialsProviderChain");

    /// we don't provide any credentials to avoid signing
    if (credentials_configuration.no_sign_request)
        return;

    /// add explicit credentials to the front of the chain
    /// because it's manually defined by the user
    if (!credentials.IsEmpty())
    {
        AddProvider(std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(credentials));
        return;
    }

    if (credentials_configuration.use_environment_credentials)
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
            DB::S3::PocoHTTPClientConfiguration aws_client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
                configuration.region,
                configuration.remote_host_filter,
                configuration.s3_max_redirects,
                configuration.enable_s3_requests_logging,
                configuration.for_disk_s3,
                configuration.get_request_throttler,
                configuration.put_request_throttler);
            AddProvider(std::make_shared<AwsAuthSTSAssumeRoleWebIdentityCredentialsProvider>(aws_client_configuration, credentials_configuration.expiration_window_seconds));
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
            DB::S3::PocoHTTPClientConfiguration aws_client_configuration = DB::S3::ClientFactory::instance().createClientConfiguration(
                configuration.region,
                configuration.remote_host_filter,
                configuration.s3_max_redirects,
                configuration.enable_s3_requests_logging,
                configuration.for_disk_s3,
                configuration.get_request_throttler,
                configuration.put_request_throttler,
                Aws::Http::SchemeMapper::ToString(Aws::Http::Scheme::HTTP));

            /// See MakeDefaultHttpResourceClientConfiguration().
            /// This is part of EC2 metadata client, but unfortunately it can't be accessed from outside
            /// of contrib/aws/aws-cpp-sdk-core/source/internal/AWSHttpResourceClient.cpp
            aws_client_configuration.maxConnections = 2;

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

            auto ec2_metadata_client = InitEC2MetadataClient(aws_client_configuration);
            auto config_loader = std::make_shared<AWSEC2InstanceProfileConfigLoader>(ec2_metadata_client, !credentials_configuration.use_insecure_imds_request);

            AddProvider(std::make_shared<AWSInstanceProfileCredentialsProvider>(config_loader));
            LOG_INFO(logger, "Added EC2 metadata service credentials provider to the provider chain.");
        }
    }

    /// Quite verbose provider (argues if file with credentials doesn't exist) so iut's the last one
    /// in chain.
    AddProvider(std::make_shared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>());
}

}

}

#endif
