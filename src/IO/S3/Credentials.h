#pragma once

#include "config.h"

#if USE_AWS_S3
#    include <aws/core/client/ClientConfiguration.h>
#    include <aws/core/internal/AWSHttpResourceClient.h>
#    include <aws/core/config/AWSProfileConfigLoader.h>
#    include <aws/core/auth/AWSCredentialsProvider.h>
#    include <aws/core/auth/AWSCredentialsProviderChain.h>

#    include <Common/logger_useful.h>

#    include <IO/S3/PocoHTTPClient.h>


namespace DB::S3
{

class AWSEC2MetadataClient : public Aws::Internal::AWSHttpResourceClient
{
    static constexpr char EC2_SECURITY_CREDENTIALS_RESOURCE[] = "/latest/meta-data/iam/security-credentials";
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

private:
    const Aws::String endpoint;
    mutable std::recursive_mutex token_mutex;
    mutable Aws::String token;
    Poco::Logger * logger;
};

std::shared_ptr<AWSEC2MetadataClient> InitEC2MetadataClient(const Aws::Client::ClientConfiguration & client_configuration);

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
    Poco::Logger * logger;
};

class AWSInstanceProfileCredentialsProvider : public Aws::Auth::AWSCredentialsProvider
{
public:
    /// See InstanceProfileCredentialsProvider.

    explicit AWSInstanceProfileCredentialsProvider(const std::shared_ptr<AWSEC2InstanceProfileConfigLoader> & config_loader);

    Aws::Auth::AWSCredentials GetAWSCredentials() override;
protected:
    void Reload() override;

private:
    void refreshIfExpired();

    std::shared_ptr<AWSEC2InstanceProfileConfigLoader> ec2_metadata_config_loader;
    Int64 load_frequency_ms;
    Poco::Logger * logger;
};

class AwsAuthSTSAssumeRoleWebIdentityCredentialsProvider : public Aws::Auth::AWSCredentialsProvider
{
    /// See STSAssumeRoleWebIdentityCredentialsProvider.

public:
    explicit AwsAuthSTSAssumeRoleWebIdentityCredentialsProvider(DB::S3::PocoHTTPClientConfiguration & aws_client_configuration);

    Aws::Auth::AWSCredentials GetAWSCredentials() override;
protected:
    void Reload() override;

private:
    void refreshIfExpired();

    std::unique_ptr<Aws::Internal::STSCredentialsClient> client;
    Aws::Auth::AWSCredentials credentials;
    Aws::String role_arn;
    Aws::String token_file;
    Aws::String session_name;
    Aws::String token;
    bool initialized = false;
    Poco::Logger * logger;
};

class S3CredentialsProviderChain : public Aws::Auth::AWSCredentialsProviderChain
{
public:
    S3CredentialsProviderChain(const DB::S3::PocoHTTPClientConfiguration & configuration, const Aws::Auth::AWSCredentials & credentials, bool use_environment_credentials, bool use_insecure_imds_request);
};

}

#endif
