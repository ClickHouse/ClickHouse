#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <Common/Exception.h>
#include <common/types.h>

#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/HeadObjectRequest.h>

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

    namespace ErrorCodes
    {
        extern const int S3_ERROR;
    }

}

namespace DB::S3
{

class ClientFactory
{
public:
    ~ClientFactory();

    static ClientFactory & instance();

    std::shared_ptr<Aws::S3::S3Client> create(
        const String & endpoint,
        bool is_virtual_hosted_style,
        const String & access_key_id,
        const String & secret_access_key,
        bool use_environment_credentials,
        const RemoteHostFilter & remote_host_filter,
        unsigned int s3_max_redirects);

    std::shared_ptr<Aws::S3::S3Client> create(
        const Aws::Client::ClientConfiguration & cfg,
        bool is_virtual_hosted_style,
        const String & access_key_id,
        const String & secret_access_key,
        bool use_environment_credentials,
        const RemoteHostFilter & remote_host_filter,
        unsigned int s3_max_redirects);

    std::shared_ptr<Aws::S3::S3Client> create(
        const Aws::Client::ClientConfiguration & cfg,
        bool is_virtual_hosted_style,
        const String & access_key_id,
        const String & secret_access_key,
        HeaderCollection headers,
        bool use_environment_credentials,
        const RemoteHostFilter & remote_host_filter,
        unsigned int s3_max_redirects);

private:
    ClientFactory();

private:
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
    String storage_name;

    bool is_virtual_hosted_style;

    explicit URI(const Poco::URI & uri_);
};

size_t getObjectSize(std::shared_ptr<Aws::S3::S3Client> client_ptr, const String & bucket, const String & key);

}

#endif
