#pragma once

#include "config.h"

#if USE_AWS_S3

#include <optional>
#include <string>
#include <Poco/URI.h>


namespace DB::S3
{

/**
 * Represents S3 URI.
 *
 * The following patterns are allowed:
 * s3://bucket/key
 * http(s)://endpoint/bucket/key
 * http(s)://bucket.<vpce_endpoint_id>.s3.<region>.vpce.amazonaws.com<:port_number>/bucket_name/key
 */
struct URI
{
    Poco::URI uri;
    // Custom endpoint if URI scheme, if not S3.
    std::string endpoint;
    std::string bucket;
    std::string key;
    std::string version_id;
    std::string storage_name;
    /// Path (or path pattern) in archive if uri is an archive.
    std::optional<std::string> archive_pattern;
    std::string uri_str;

    bool is_virtual_hosted_style;

    URI() = default;
    explicit URI(const std::string & uri_, bool allow_archive_path_syntax = false);
    void addRegionToURI(const std::string & region);

    static void validateBucket(const std::string & bucket, const Poco::URI & uri);

private:
    std::pair<std::string, std::optional<std::string>> getURIAndArchivePattern(const std::string & source);
};

}

#endif
