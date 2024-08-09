#pragma once

#include <string>

#include "config.h"

#if USE_AWS_S3

#include <Poco/URI.h>

namespace DB::S3
{

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
    std::string endpoint;
    std::string bucket;
    std::string key;
    std::string version_id;
    std::string storage_name;

    bool is_virtual_hosted_style;

    URI() = default;
    explicit URI(const std::string & uri_);

    static void validateBucket(const std::string & bucket, const Poco::URI & uri);
};

}

#endif
