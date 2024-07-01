#include <IO/S3/URI.h>

#if USE_AWS_S3
#include <Common/Exception.h>
#include <Common/quoteString.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <re2/re2.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace S3
{

URI::URI(const std::string & uri_)
{
    /// Case when bucket name represented in domain name of S3 URL.
    /// E.g. (https://bucket-name.s3.Region.amazonaws.com/key)
    /// https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html#virtual-hosted-style-access
    static const RE2 virtual_hosted_style_pattern(R"((.+)\.(s3|cos|obs|oss)([.\-][a-z0-9\-.:]+))");

    /// Case when bucket name and key represented in path of S3 URL.
    /// E.g. (https://s3.Region.amazonaws.com/bucket-name/key)
    /// https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html#path-style-access
    static const RE2 path_style_pattern("^/([^/]*)/(.*)");

    static constexpr auto S3 = "S3";
    static constexpr auto COSN = "COSN";
    static constexpr auto COS = "COS";
    static constexpr auto OBS = "OBS";
    static constexpr auto OSS = "OSS";

    uri = Poco::URI(uri_);

    storage_name = S3;

    if (uri.getHost().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Host is empty in S3 URI.");

    /// Extract object version ID from query string.
    bool has_version_id = false;
    for (const auto & [query_key, query_value] : uri.getQueryParameters())
        if (query_key == "versionId")
        {
            version_id = query_value;
            has_version_id = true;
        }

    /// Poco::URI will ignore '?' when parsing the path, but if there is a versionId in the http parameter,
    /// '?' can not be used as a wildcard, otherwise it will be ambiguous.
    /// If no "versionId" in the http parameter, '?' can be used as a wildcard.
    /// It is necessary to encode '?' to avoid deletion during parsing path.
    if (!has_version_id && uri_.find('?') != String::npos)
    {
        String uri_with_question_mark_encode;
        Poco::URI::encode(uri_, "?", uri_with_question_mark_encode);
        uri = Poco::URI(uri_with_question_mark_encode);
    }

    String name;
    String endpoint_authority_from_uri;

    if (re2::RE2::FullMatch(uri.getAuthority(), virtual_hosted_style_pattern, &bucket, &name, &endpoint_authority_from_uri))
    {
        is_virtual_hosted_style = true;
        endpoint = uri.getScheme() + "://" + name + endpoint_authority_from_uri;
        validateBucket(bucket, uri);

        if (!uri.getPath().empty())
        {
            /// Remove leading '/' from path to extract key.
            key = uri.getPath().substr(1);
        }

        boost::to_upper(name);
        if (name != S3 && name != COS && name != OBS && name != OSS)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "Object storage system name is unrecognized in virtual hosted style S3 URI: {}",
                            quoteString(name));

        if (name == S3)
            storage_name = name;
        else if (name == OBS)
            storage_name = OBS;
        else if (name == OSS)
            storage_name = OSS;
        else
            storage_name = COSN;
    }
    else if (re2::RE2::PartialMatch(uri.getPath(), path_style_pattern, &bucket, &key))
    {
        is_virtual_hosted_style = false;
        endpoint = uri.getScheme() + "://" + uri.getAuthority();
        validateBucket(bucket, uri);
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bucket or key name are invalid in S3 URI.");
}

void URI::validateBucket(const String & bucket, const Poco::URI & uri)
{
    /// S3 specification requires at least 3 and at most 63 characters in bucket name.
    /// https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-s3-bucket-naming-requirements.html
    if (bucket.length() < 3 || bucket.length() > 63)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bucket name length is out of bounds in virtual hosted style S3 URI: {}{}",
                        quoteString(bucket), !uri.empty() ? " (" + uri.toString() + ")" : "");
}

}

}

#endif
