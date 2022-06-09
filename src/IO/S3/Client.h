#pragma once

#include <Common/config.h>


#if USE_AWS_S3

#include <memory>
#include <vector>

#include <base/types.h>
#include <Common/RemoteHostFilter.h>

#include <aws/core/Aws.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/UploadPartCopyRequest.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/S3Client.h>

#include <Poco/URI.h>


namespace Aws::Client
{
class RetryStrategy;
}

namespace Aws::Auth
{
class Credentials;
}

namespace DB
{
class RemoteHostFilter;
struct HttpHeader;
using HeaderCollection = std::vector<HttpHeader>;
}

namespace DB::S3
{
class ClientFactory;
class PocoHTTPClientFactory;

/**
 * Represents S3 URI.
 *
 * The following patterns are allowed:
 * s3://bucket/key
 * http(s)://endpoint/bucket/key
 */
struct URI
{
    Poco::URI uri;
    // Custom endpoint if URI scheme is not S3.
    String endpoint;
    String bucket;
    String key;
    String version_id;
    String storage_name;
    String region;

    bool is_virtual_hosted_style;

    explicit URI(const String & uri_);
    explicit URI(const Poco::URI & uri_);

    static void validateBucket(const String & bucket, const Poco::URI & uri);
};

struct ClientConfigurationPerRequest
{
    String proxy_scheme = "https";
    String proxy_host;
    unsigned proxy_port = 0;
};

class ClientConfiguration : private Aws::Client::ClientConfiguration
{
    ClientConfiguration(const RemoteHostFilter & remote_host_filter);

    /// Constructor of Aws::Client::ClientConfiguration must be called after AWS SDK initialization.
    friend ClientFactory;

    /// HTTP client downcasts AWS ClientConfiguration to this one.
    friend PocoHTTPClientFactory;

    using PerRequestConfigurationFetcher = std::function<ClientConfigurationPerRequest(const Aws::Http::HttpRequest &)>;
    using ReportProvider = std::function<void(const ClientConfigurationPerRequest &)>;

    PerRequestConfigurationFetcher per_request_configuration = [] (const Aws::Http::HttpRequest &) { return ClientConfigurationPerRequest(); };
    ReportProvider error_report = [] (const ClientConfigurationPerRequest &) {};

    const RemoteHostFilter & remote_host_filter;

    bool use_insecure_imds_request = false;
    bool use_environment_credentials = false;
    bool enable_requests_logging = false;

    String access_key_id;
    String secret_access_key;
    String region_override; /// If set, Client ignores region determined from URL and doesn't try to guess it automatically.
    String server_side_encryption_customer_key_base64;

    HeaderCollection extra_headers;

    size_t max_redirects = 10;

public:
    const Aws::Client::ClientConfiguration & getLegacyConfiguration() const;

    void setUseInsecureImdsRequest(bool value);
    void setUseEnvironmentCredentials(bool value);
    void setEnableRequestsLogging(bool value);
    void setExtraHeaders(HeaderCollection extra_headers);
    void setCredentials(String access_key_id, String secret_access_key);
    void setMaxConnections(size_t max_connections);
    void setMaxRedirects(size_t max_redirects);
    void setRegion(String region);
    void setRegionOverride(String region_override);
    void setEndpointOverride(String endpoint_override);
    void setConnectTimeoutMs(size_t value);
    void setRequestTimeoutMs(size_t value);
    void setServerSideEncryptionCustomerKeyBase64(String server_side_encryption_customer_key_base64);
    void setErrorReport(ReportProvider error_report);
    void setRetryStrategy(std::shared_ptr<Aws::Client::RetryStrategy> retry_strategy);
    void setPerRequestConfiguration(PerRequestConfigurationFetcher per_request_configuration);
    void setScheme(Aws::Http::Scheme scheme);
    void setProxyHost(String proxy_host);
    void setProxyUserName(String proxy_user_name);
    void setProxyPassword(String proxy_password);
    void setProxyPort(unsigned short port);

    bool getUseInsecureImdsRequest() const;
    bool getUseEnvironmentCredentials() const;
    ReportProvider getErrorReport() const;
    PerRequestConfigurationFetcher getPerRequestConfiguration() const;
    String getRegion() const;
    String getRegionOverride() const;
    bool getEnableRequestsLogging() const;
    size_t getMaxRedirects() const;
    size_t getMaxConnections() const;
    size_t getConnectTimeoutMs() const;
    size_t getRequestTimeoutMs() const;
    const RemoteHostFilter & getRemoteHostFilter() const;
    HeaderCollection getExtraHeaders() const;
    String getServerSideEncryptionCustomerKeyBase64() const;
    Aws::Auth::AWSCredentials getCredentials() const;
    Aws::Http::Scheme getScheme() const;
};

class Client
{
public:
    Client(const ClientConfiguration & client_configuration, const URI & uri);

    size_t getObjectSize(const String & bucket, const String & key, const String & version_id = {}, bool throw_on_error = true) const;

    Aws::S3::Model::CopyObjectOutcome copyObject(Aws::S3::Model::CopyObjectRequest & request, std::shared_ptr<const Client> src_client = {}) const;
    Aws::S3::Model::DeleteObjectOutcome deleteObject(Aws::S3::Model::DeleteObjectRequest & request) const;
    Aws::S3::Model::DeleteObjectsOutcome deleteObjects(Aws::S3::Model::DeleteObjectsRequest & request) const;
    Aws::S3::Model::GetObjectOutcome getObject(Aws::S3::Model::GetObjectRequest & request) const;
    Aws::S3::Model::HeadObjectOutcome headObject(Aws::S3::Model::HeadObjectRequest & request) const;
    Aws::S3::Model::ListObjectsV2Outcome listObjectsV2(Aws::S3::Model::ListObjectsV2Request & request) const;
    Aws::S3::Model::PutObjectOutcome putObject(Aws::S3::Model::PutObjectRequest & request) const;

    Aws::S3::Model::CreateMultipartUploadOutcome createMultipartUpload(Aws::S3::Model::CreateMultipartUploadRequest & request) const;
    Aws::S3::Model::UploadPartOutcome uploadPart(Aws::S3::Model::UploadPartRequest & request) const;
    Aws::S3::Model::UploadPartCopyOutcome uploadPartCopy(Aws::S3::Model::UploadPartCopyRequest & request, std::shared_ptr<const Client> src_client = {}) const;
    Aws::S3::Model::CompleteMultipartUploadOutcome completeMultipartUpload(const Aws::S3::Model::CompleteMultipartUploadRequest & request) const;
    Aws::S3::Model::AbortMultipartUploadOutcome abortMultipartUpload(const Aws::S3::Model::AbortMultipartUploadRequest & request) const;

    void disableRequestProcessing();
    void enableRequestProcessing();

private:
    template<typename Request>
    void setServerSideEncryptionIfNeeded(Request & request) const;

    template<typename Request>
    void setServerSideEncryptionIfNeeded(Request & request, std::shared_ptr<const Client> src_client) const;

    String server_side_encryption_customer_key_base64;
    String server_side_encryption_customer_key_md5_base64;
    String server_side_encryption_customer_algorithm;

    ClientConfiguration client_configuration;
    std::unique_ptr<Aws::S3::S3Client> s3;
};

}

#endif
