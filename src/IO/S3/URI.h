#pragma once

#include "config.h"

#if USE_AWS_S3

#include <optional>
#include <string>
#include <Poco/URI.h>

#include <Core/SettingsEnums.h>

namespace DB
{
class RemoteHostFilter;
}

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

    bool is_virtual_hosted_style = false;

    URI() = default;
    explicit URI(
        const std::string & uri_,
        bool allow_archive_path_syntax = false,
        bool keep_presigned_query_parameters = true,
        S3UriStyle uri_style = S3UriStyle::AUTO,
        bool validate_bucket_name = true);
    void addRegionToURI(const std::string & region);

    /// Allowed by an <s3_bucket> entry for the endpoint and bucket, or by a <host>/<host_regexp> entry for the host.
    /// With throw_on_error, throws UNACCEPTABLE_URL instead of returning false.
    bool checkRemoteHostFilter(const RemoteHostFilter & filter, bool throw_on_error = true) const;

    /// Checks the S3 rules for a bucket name. A caller passing `validate_bucket_name = false` to the constructor
    /// checks the rules of its own service instead (e.g. native GCS allows dotted bucket names of up to 222 characters).
    static void validateBucket(const std::string & bucket, const Poco::URI & uri);
    /// Checks the Google Cloud Storage rules for the length of a bucket name: 3 to 63 characters, or up to 222 for a
    /// name with dots, each dot-separated component of which has at most 63 characters.
    static void validateGCSBucket(const std::string & bucket, const Poco::URI & uri);
    static void validateKey(const std::string & key, const Poco::URI & uri);

private:
    bool tryInitPathStyle();
    bool tryInitVirtualHostedStyle(bool is_using_aws_private_link_interface, bool use_strict_pattern);
};

std::string expandRegionToAmazonPath(const std::string & region);

}

#endif
