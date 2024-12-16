#include <IO/S3/URI.h>

#if USE_AWS_S3
#include <Interpreters/Context.h>
#include <Common/Macros.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Common/re2.h>
#include <IO/Archives/ArchiveUtils.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

struct URIConverter
{
    static void modifyURI(Poco::URI & uri, std::unordered_map<std::string, std::string> mapper)
    {
        Macros macros({{"bucket", uri.getHost()}});
        uri = macros.expand(mapper[uri.getScheme()]).empty() ? uri : Poco::URI(macros.expand(mapper[uri.getScheme()]) + uri.getPathAndQuery());
    }
};

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace S3
{

URI::URI(const std::string & uri_, bool allow_archive_path_syntax)
{
    /// Case when bucket name represented in domain name of S3 URL.
    /// E.g. (https://bucket-name.s3.region.amazonaws.com/key)
    /// https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html#virtual-hosted-style-access
    static const RE2 virtual_hosted_style_pattern(R"((.+)\.(s3express[\-a-z0-9]+|s3|cos|obs|oss|eos)([.\-][a-z0-9\-.:]+))");

    /// Case when AWS Private Link Interface is being used
    /// E.g. (bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w.s3.us-east-1.vpce.amazonaws.com/bucket-name/key)
    /// https://docs.aws.amazon.com/AmazonS3/latest/userguide/privatelink-interface-endpoints.html
    static const RE2 aws_private_link_style_pattern(R"(bucket\.vpce\-([a-z0-9\-.]+)\.vpce\.amazonaws\.com(:\d{1,5})?)");

    /// Case when bucket name and key represented in the path of S3 URL.
    /// E.g. (https://s3.region.amazonaws.com/bucket-name/key)
    /// https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html#path-style-access
    static const RE2 path_style_pattern("^/([^/]*)/(.*)");

    if (allow_archive_path_syntax)
        std::tie(uri_str, archive_pattern) = getURIAndArchivePattern(uri_);
    else
        uri_str = uri_;

    uri = Poco::URI(uri_str);

    std::unordered_map<std::string, std::string> mapper;
    auto context = Context::getGlobalContextInstance();
    if (context)
    {
        const auto *config = &context->getConfigRef();
        if (config->has("url_scheme_mappers"))
        {
            std::vector<String> config_keys;
            config->keys("url_scheme_mappers", config_keys);
            for (const std::string & config_key : config_keys)
                mapper[config_key] = config->getString("url_scheme_mappers." + config_key + ".to");
        }
        else
        {
            mapper["s3"] = "https://{bucket}.s3.amazonaws.com";
            mapper["gs"] = "https://storage.googleapis.com/{bucket}";
            mapper["oss"] = "https://{bucket}.oss.aliyuncs.com";
        }

        if (!mapper.empty())
            URIConverter::modifyURI(uri, mapper);
    }

    storage_name = "S3";

    if (uri.getHost().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Host is empty in S3 URI.");

    /// Extract object version ID from query string.
    bool has_version_id = false;
    for (const auto & [query_key, query_value] : uri.getQueryParameters())
    {
        if (query_key == "versionId")
        {
            version_id = query_value;
            has_version_id = true;
        }
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

    bool is_using_aws_private_link_interface = re2::RE2::FullMatch(uri.getAuthority(), aws_private_link_style_pattern);

    if (!is_using_aws_private_link_interface
        && re2::RE2::FullMatch(uri.getAuthority(), virtual_hosted_style_pattern, &bucket, &name, &endpoint_authority_from_uri))
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
        if (name == "COS")
            storage_name = "COSN";
        else
            storage_name = name;
    }
    else if (re2::RE2::PartialMatch(uri.getPath(), path_style_pattern, &bucket, &key))
    {
        is_virtual_hosted_style = false;
        endpoint = uri.getScheme() + "://" + uri.getAuthority();
        validateBucket(bucket, uri);
    }
    else
    {
        /// Custom endpoint, e.g. a public domain of Cloudflare R2,
        /// which could be served by a custom server-side code.
        storage_name = "S3";
        bucket = "default";
        is_virtual_hosted_style = false;
        endpoint = uri.getScheme() + "://" + uri.getAuthority();
        if (!uri.getPath().empty())
            key = uri.getPath().substr(1);
    }
}

void URI::addRegionToURI(const std::string &region)
{
    if (auto pos = endpoint.find(".amazonaws.com"); pos != std::string::npos)
        endpoint = endpoint.substr(0, pos) + "." + region + endpoint.substr(pos);
}

void URI::validateBucket(const String & bucket, const Poco::URI & uri)
{
    /// S3 specification requires at least 3 and at most 63 characters in bucket name.
    /// https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-s3-bucket-naming-requirements.html
    if (bucket.length() < 3 || bucket.length() > 63)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Bucket name length is out of bounds in virtual hosted style S3 URI: {}{}",
            quoteString(bucket),
            !uri.empty() ? " (" + uri.toString() + ")" : "");
}

std::pair<std::string, std::optional<std::string>> URI::getURIAndArchivePattern(const std::string & source)
{
    size_t pos = source.find("::");
    if (pos == String::npos)
        return {source, std::nullopt};

    std::string_view path_to_archive_view = std::string_view{source}.substr(0, pos);
    bool contains_spaces_around_operator = false;
    while (path_to_archive_view.ends_with(' '))
    {
        contains_spaces_around_operator = true;
        path_to_archive_view.remove_suffix(1);
    }

    std::string_view archive_pattern_view = std::string_view{source}.substr(pos + 2);
    while (archive_pattern_view.starts_with(' '))
    {
        contains_spaces_around_operator = true;
        archive_pattern_view.remove_prefix(1);
    }

    /// possible situations when the first part can be archive is only if one of the following is true:
    /// - it contains supported extension
    /// - it contains spaces after or before :: (URI cannot contain spaces)
    /// - it contains characters that could mean glob expression
    if (archive_pattern_view.empty() || path_to_archive_view.empty()
        || (!contains_spaces_around_operator && !hasSupportedArchiveExtension(path_to_archive_view)
            && path_to_archive_view.find_first_of("*?{") == std::string_view::npos))
        return {source, std::nullopt};

    return std::pair{std::string{path_to_archive_view}, std::string{archive_pattern_view}};
}
}

}

#endif
