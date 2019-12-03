#pragma once

#include <regex>
#include <Core/Types.h>

#include <Poco/Net/HTTPRequest.h>
#include <aws/s3/S3Client.h>


namespace DB
{

struct S3Endpoint {
    String endpoint_url;
    String bucket;
    String key;
};


namespace S3Helper
{
    S3Endpoint parseS3EndpointFromUrl(const String & url);

    std::shared_ptr<Aws::S3::S3Client> createS3Client(const String & endpoint_url,
                        const String & access_key_id,
                        const String & secret_access_key);
}

}
