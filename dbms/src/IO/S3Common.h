#pragma once

#include <Common/config.h>

#if USE_AWS_S3

#include <Core/Types.h>
#include <Poco/URI.h>
#include <aws/core/Aws.h>

namespace Aws::S3
{
    class S3Client;
}

namespace DB::S3
{

class ClientFactory
{
public:
    ~ClientFactory();

    static ClientFactory & instance();

    std::shared_ptr<Aws::S3::S3Client> create(const String & endpoint,
                                              const String & access_key_id,
                                              const String & secret_access_key);

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

    explicit URI (Poco::URI & uri_);
};

}

#endif
