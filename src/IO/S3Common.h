#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <base/types.h>
#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <IO/S3/PocoHTTPClient.h>
#include <Poco/URI.h>

namespace Aws::S3
{
    class S3Client;
}

namespace DB
{
    class RemoteHostFilter;
    struct HttpHeader;
    using HeaderCollection = std::vector<HttpHeader>;
}

namespace DB::S3
{
class ClientFactory
{
public:
    ~ClientFactory();

    static ClientFactory & instance();

    std::shared_ptr<Aws::S3::S3Client> create(
        const PocoHTTPClientConfiguration & cfg,
        bool is_virtual_hosted_style,
        const String & access_key_id,
        const String & secret_access_key,
        const String & server_side_encryption_customer_key_base64,
        HeaderCollection headers,
        bool use_environment_credentials,
        bool use_insecure_imds_request);

    PocoHTTPClientConfiguration createClientConfiguration(
        const String & force_region,
        const RemoteHostFilter & remote_host_filter,
        unsigned int s3_max_redirects);

private:
    ClientFactory();

    Aws::SDKOptions aws_options;
};

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

    bool is_virtual_hosted_style;

    explicit URI(const Poco::URI & uri_);

    static void validateBucket(const String & bucket, const Poco::URI & uri);
};

size_t getObjectSize(std::shared_ptr<Aws::S3::S3Client> client_ptr, const String & bucket, const String & key, const String & version_id = {}, bool throw_on_error = true);

}

#endif
