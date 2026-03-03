#pragma once

#include "config.h"

#if USE_AWS_S3

#    include <base/types.h>

#    include <aws/core/auth/AWSCredentials.h>
#    include <aws/core/utils/threading/ReaderWriterLock.h>
#    include <aws/core/http/HttpRequest.h>
#    include <aws/core/endpoint/AWSEndpoint.h>
#    include <aws/core/AmazonSerializableWebServiceRequest.h>
#    include <aws/core/client/AWSErrorMarshaller.h>
#    include <aws/core/NoResult.h>
#    include <aws/s3/S3Errors.h>
#    include <aws/core/client/AWSXmlClient.h>
#    include <aws/core/auth/AWSCredentialsProvider.h>

#    include <aws/core/client/ClientConfiguration.h>
#    include <aws/core/internal/AWSHttpResourceClient.h>
#    include <aws/core/config/AWSProfileConfigLoader.h>
#    include <aws/core/auth/AWSCredentialsProviderChain.h>
#    include <aws/core/auth/bearer-token-provider/SSOBearerTokenProvider.h>

#    include <IO/S3/PocoHTTPClient.h>
#    include <IO/S3Defines.h>

class SipHash;
namespace DB::S3
{

/// In GCP metadata service can be accessed via DNS regardless of IPv4 or IPv6.
static inline constexpr char GCP_METADATA_SERVICE_ENDPOINT[] = "http://metadata.google.internal";

/// getRunningAvailabilityZone returns the availability zone of the underlying compute resources where the current process runs.
std::string getRunningAvailabilityZone();
std::string tryGetRunningAvailabilityZone();

void setCredentialsProviderCacheMaxSize(size_t cache_size);

class AWSEC2MetadataClient : public Aws::Internal::AWSHttpResourceClient
{
    static constexpr char EC2_SECURITY_CREDENTIALS_RESOURCE[] = "/latest/meta-data/iam/security-credentials";
    static constexpr char EC2_AVAILABILITY_ZONE_RESOURCE[] = "/latest/meta-data/placement/availability-zone";
    static constexpr char EC2_IMDS_TOKEN_RESOURCE[] = "/latest/api/token";
    static constexpr char EC2_IMDS_TOKEN_HEADER[] = "x-aws-ec2-metadata-token";
    static constexpr char EC2_IMDS_TOKEN_TTL_DEFAULT_VALUE[] = "21600";
    static constexpr char EC2_IMDS_TOKEN_TTL_HEADER[] = "x-aws-ec2-metadata-token-ttl-seconds";

public:
    /// See EC2MetadataClient.

    explicit AWSEC2MetadataClient(const Aws::Client::ClientConfiguration & client_configuration, const char * endpoint_);

    AWSEC2MetadataClient& operator =(const AWSEC2MetadataClient & rhs) = delete;
    AWSEC2MetadataClient(const AWSEC2MetadataClient & rhs) = delete;
    AWSEC2MetadataClient& operator =(const AWSEC2MetadataClient && rhs) = delete;
    AWSEC2MetadataClient(const AWSEC2MetadataClient && rhs) = delete;

    ~AWSEC2MetadataClient() override = default;

    using Aws::Internal::AWSHttpResourceClient::GetResource;

    virtual Aws::String GetResource(const char * resource_path) const;
    virtual Aws::String getDefaultCredentials() const;

    static Aws::String awsComputeUserAgentString();

    virtual Aws::String getDefaultCredentialsSecurely() const;

    virtual Aws::String getCurrentRegion() const;

    friend String getRunningAvailabilityZone();

private:
    std::pair<Aws::String, Aws::Http::HttpResponseCode> getEC2MetadataToken(const std::string & user_agent_string) const;
    static String getAvailabilityZoneOrException();

    const Aws::String endpoint;
    mutable std::recursive_mutex token_mutex;
    mutable Aws::String token;
    LoggerPtr logger;
};

std::shared_ptr<AWSEC2MetadataClient> createEC2MetadataClient(const Aws::Client::ClientConfiguration & client_configuration);

class AWSEC2InstanceProfileConfigLoader : public Aws::Config::AWSProfileConfigLoader
{
public:
    explicit AWSEC2InstanceProfileConfigLoader(const std::shared_ptr<AWSEC2MetadataClient> & client_, bool use_secure_pull_);

    ~AWSEC2InstanceProfileConfigLoader() override = default;

protected:
    bool LoadInternal() override;

private:
    std::shared_ptr<AWSEC2MetadataClient> client;
    bool use_secure_pull;
    LoggerPtr logger;
};

class AWSInstanceProfileCredentialsProvider : public Aws::Auth::AWSCredentialsProvider
{
public:
    /// See InstanceProfileCredentialsProvider.

    static std::shared_ptr<Aws::Auth::AWSCredentialsProvider>
    create(const Aws::Client::ClientConfiguration & client_configuration, bool use_secure_pull);

    explicit AWSInstanceProfileCredentialsProvider(const std::shared_ptr<AWSEC2InstanceProfileConfigLoader> & config_loader);

    Aws::Auth::AWSCredentials GetAWSCredentials() override;

    struct CacheKey
    {
        String endpoint;
        bool use_secure_pull;

        bool operator==(const CacheKey & rhs) const = default;

        void updateHash(SipHash & hash) const;
    };

protected:
    void Reload() override;

private:
    Aws::Auth::AWSCredentials GetAWSCredentialsImpl();
    void refreshIfExpired();

    std::shared_ptr<AWSEC2InstanceProfileConfigLoader> ec2_metadata_config_loader;
    Int64 load_frequency_ms;
    LoggerPtr logger;
};

class AwsAuthSTSAssumeRoleWebIdentityCredentialsProvider : public Aws::Auth::AWSCredentialsProvider
{
    /// See STSAssumeRoleWebIdentityCredentialsProvider.

public:
    static std::shared_ptr<Aws::Auth::AWSCredentialsProvider>
    create(DB::S3::PocoHTTPClientConfiguration & aws_client_configuration, uint64_t expiration_window_seconds_, String role_arn_ = "");

    explicit AwsAuthSTSAssumeRoleWebIdentityCredentialsProvider(
        DB::S3::PocoHTTPClientConfiguration & aws_client_configuration,
        uint64_t expiration_window_seconds_,
        Aws::String role_arn_,
        Aws::String token_file_,
        Aws::String session_name_,
        String tmp_region);

    Aws::Auth::AWSCredentials GetAWSCredentials() override;

    struct CacheKey
    {
        Aws::String role_arn;
        Aws::String token_file;
        Aws::String session_name;

        bool operator==(const CacheKey & rhs) const = default;

        void updateHash(SipHash & hash) const;
    };

protected:
    void Reload() override;

private:
    std::unique_ptr<Aws::Internal::STSCredentialsClient> client;
    Aws::Auth::AWSCredentials credentials;
    Aws::String role_arn;
    Aws::String token_file;
    Aws::String session_name;
    Aws::String token;
    LoggerPtr logger;
    uint64_t expiration_window_seconds;
};

class SSOCredentialsProvider : public Aws::Auth::AWSCredentialsProvider
{
public:
    SSOCredentialsProvider(DB::S3::PocoHTTPClientConfiguration aws_client_configuration_, uint64_t expiration_window_seconds_);

    Aws::Auth::AWSCredentials GetAWSCredentials() override;

private:
    Aws::UniquePtr<Aws::Internal::SSOCredentialsClient> client;
    Aws::Auth::AWSCredentials credentials;

    // Profile description variables
    Aws::String profile_to_use;

    // The AWS account ID that temporary AWS credentials are resolved for.
    Aws::String sso_account_id;
    // The AWS region where the SSO directory for the given sso_start_url is hosted.
    // This is independent of the general region configuration and MUST NOT be conflated.
    Aws::String sso_region;
    // The expiration time of the accessToken.
    Aws::Utils::DateTime expires_at;
    // The SSO Token Provider
    Aws::Auth::SSOBearerTokenProvider bearer_token_provider;

    DB::S3::PocoHTTPClientConfiguration aws_client_configuration;
    uint64_t expiration_window_seconds;
    LoggerPtr logger;

    void Reload() override;
    void refreshIfExpired();
    Aws::String loadAccessTokenFile(const Aws::String & sso_access_token_path);
};

struct CredentialsConfiguration
{
    bool use_environment_credentials = false;
    bool use_insecure_imds_request = false;
    uint64_t expiration_window_seconds = DEFAULT_EXPIRATION_WINDOW_SECONDS;
    bool no_sign_request = false;
    std::string role_arn{};
    std::string role_session_name{};
    std::string sts_endpoint_override{};
    std::string kms_role_arn{};
};

class S3CredentialsProviderChain : public Aws::Auth::AWSCredentialsProviderChain
{
public:
    S3CredentialsProviderChain(
        const DB::S3::PocoHTTPClientConfiguration & configuration,
        const Aws::Auth::AWSCredentials & credentials,
        const CredentialsConfiguration & credentials_configuration);
};

class AssumeRoleRequest : public Aws::AmazonSerializableWebServiceRequest
{
public:
    AssumeRoleRequest(std::string role_arn_, std::string role_session_name_);

    Aws::Http::HeaderValueCollection GetHeaders() const override;

    const char * GetServiceRequestName() const override { return "AssumeRole"; }

    Aws::String SerializePayload() const override { return {}; }

    void AddQueryStringParameters(Aws::Http::URI & uri) const override;

private:
    std::string role_arn;
    std::string role_session_name;
};
class AssumeRoleResult
{
public:
    /// NOLINTNEXTLINE
    AssumeRoleResult(Aws::AmazonWebServiceResult<Aws::Utils::Xml::XmlDocument> result);

    const std::string & getAccessKeyID() const { return access_key_id; }

    const std::string & getSecretAccessKey() const { return secret_access_key; }

    const std::string & getSessionToken() const { return session_token; }

    const Aws::Utils::DateTime & getExpiration() const { return expiration; }

private:
    std::string access_key_id;
    std::string secret_access_key;
    std::string session_token;
    Aws::Utils::DateTime expiration;

    LoggerPtr log{getLogger("AssumeRoleResult")};
};

using AssumeRoleOutcome = Aws::Utils::Outcome<AssumeRoleResult, Aws::S3::S3Error>;

class AWSAssumeRoleClient : public Aws::Client::AWSXMLClient
{
public:
    AWSAssumeRoleClient(
        const std::shared_ptr<Aws::Auth::AWSCredentialsProvider> & credentials_provider,
        const Aws::Client::ClientConfiguration & client_configuration,
        const std::string & sts_endpoint_override = "");

    AssumeRoleOutcome assumeRole(const AssumeRoleRequest & request) const;

    const auto & getEndpoint() const { return endpoint; }

private:
    Aws::Endpoint::AWSEndpoint endpoint;
};

class AwsAuthSTSAssumeRoleCredentialsProvider : public Aws::Auth::AWSCredentialsProvider
{
public:
    static std::shared_ptr<Aws::Auth::AWSCredentialsProvider> create(
            std::string role_arn_,
            std::string session_name_,
            uint64_t expiration_window_seconds_,
            std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider,
            const DB::S3::PocoHTTPClientConfiguration & client_configuration,
            const std::string & sts_endpoint_override = "");

    AwsAuthSTSAssumeRoleCredentialsProvider(
            std::string role_arn_,
            std::string session_name_,
            uint64_t expiration_window_seconds_,
            std::shared_ptr<AWSAssumeRoleClient> client_);

    Aws::Auth::AWSCredentials GetAWSCredentials() override;

    struct CacheKey
    {
        std::string role_arn;
        std::string session_name;
        std::string endpoint;
        Aws::Auth::AWSCredentials credentials;

        bool operator==(const CacheKey & rhs) const = default;

        void updateHash(SipHash & hash) const;
    };

protected:
    void Reload() override;
private:
    std::string role_arn;
    std::string session_name;
    uint64_t expiration_window_seconds;
    std::shared_ptr<AWSAssumeRoleClient> client;
    Aws::Auth::AWSCredentials credentials;
    LoggerPtr logger;
};

std::shared_ptr<Aws::Auth::AWSCredentialsProvider> getCredentialsProvider(
    const DB::S3::PocoHTTPClientConfiguration & configuration,
    const Aws::Auth::AWSCredentials & credentials,
    const CredentialsConfiguration & credentials_configuration);
}

#else

#    include <string>

namespace DB
{

namespace S3
{
std::string getRunningAvailabilityZone();
std::string tryGetRunningAvailabilityZone();
}

}
#endif
