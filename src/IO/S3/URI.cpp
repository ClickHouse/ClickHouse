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
#include <iostream>

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
    extern const int UNKNOWN_ELEMENT_OF_ENUM;
    extern const int NOT_IMPLEMENTED;
    
}

namespace S3
{

URI::URI(const std::string & uri_, bool allow_archive_path_syntax, S3UriStyleIdentifierMode uri_style)
{
    /// Case when AWS Private Link Interface is being used
    /// E.g. (bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w.s3.us-east-1.vpce.amazonaws.com/bucket-name/key)
    /// https://docs.aws.amazon.com/AmazonS3/latest/userguide/privatelink-interface-endpoints.html
    static const RE2 aws_private_link_style_pattern(R"(bucket\.vpce\-([a-z0-9\-.]+)\.vpce\.amazonaws\.com(:\d{1,5})?)");

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
    if (!has_version_id && uri_.contains('?'))
    {
        String uri_with_question_mark_encode;
        Poco::URI::encode(uri_, "?", uri_with_question_mark_encode);
        uri = Poco::URI(uri_with_question_mark_encode);
    }

    bool is_using_aws_private_link_interface = re2::RE2::FullMatch(uri.getAuthority(), aws_private_link_style_pattern);

    if (uri_style == S3UriStyleIdentifierMode::AUTO)
    {
        if (!tryInitVirtualHostedStyle(is_using_aws_private_link_interface, true))
            if (!tryInitPathStyle())
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
    else if (uri_style == S3UriStyleIdentifierMode::VIRTUAL_HOSTED)
    {
        if (!tryInitVirtualHostedStyle(is_using_aws_private_link_interface, false))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid S3 virtual-hosted-style uri: {}", !uri.empty() ? uri.toString() : "");

    }
    else if (uri_style == S3UriStyleIdentifierMode::PATH)
    {
        if (!tryInitPathStyle())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid S3 path-style uri: {}", !uri.empty() ? uri.toString() : "");
    }
    else
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "URI Style Indetifier unknown type.");
    }

    validateBucket(bucket, uri);
    validateKey(key, uri);
}


bool URI::tryInitPathStyle()
{
    /// Case when bucket name and key represented in the path of S3 URL.
    /// E.g. (https://s3.region.amazonaws.com/bucket-name/key)
    /// https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html#path-style-access
    static const RE2 path_style_pattern("^/([^/]*)(?:/?(.*))");

    if (!re2::RE2::PartialMatch(uri.getPath(), path_style_pattern, &bucket, &key))
        return false;

    is_virtual_hosted_style = false;
    endpoint = uri.getScheme() + "://" + uri.getAuthority();
    return true;
}

bool URI::tryInitVirtualHostedStyle(bool is_using_aws_private_link_interface, bool use_strict_pattern)
{
    /// Case when bucket name represented in domain name of S3 URL.
    /// E.g. (https://bucket-name.s3.region.amazonaws.com/key)
    /// https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html#virtual-hosted-style-access
    static const RE2 virtual_hosted_style_pattern_strict(R"((.+)\.(s3express[\-a-z0-9]+|s3|cos|obs|oss-data-acc|oss|eos)([.\-][a-z0-9\-.:]+))");
    static const RE2 virtual_hosted_style_pattern_light(R"(([\-a-z0-9]+)\.([\-a-z0-9]+)([.\-][a-z0-9\-.:]+))");

    if (is_using_aws_private_link_interface)
        return false;
    
    String name;
    String endpoint_authority_from_uri;
    std::cerr << uri.getAuthority() << "\n";
    
    if (!re2::RE2::FullMatch(uri.getAuthority(), (use_strict_pattern) ? virtual_hosted_style_pattern_strict : virtual_hosted_style_pattern_light, &bucket, &name, &endpoint_authority_from_uri))
        return false;

    is_virtual_hosted_style = true;
    if (name == "oss-data-acc")
    {
        bucket = bucket.substr(0, bucket.find('.'));
        endpoint = uri.getScheme() + "://" + uri.getHost().substr(bucket.length() + 1);
    }
    else
    {
        endpoint = uri.getScheme() + "://" + name + endpoint_authority_from_uri;
    }

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
    return true;
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

void URI::validateKey(const String & key, const Poco::URI & uri)
{
    auto onError = [&]()
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid S3 key: {}{}",
            quoteString(key),
            !uri.empty() ? " (" + uri.toString() + ")" : "");
    };


    // this shouldn't happen ever because the regex should not catch this
    if (key.size() == 1 && key[0] == '/')
    {
       onError();
    }

    // the current regex impl allows something like "bucket-name/////".
    // bucket: bucket-name
    // key: ////
    // throw exception in case such thing is found
    for (size_t i = 1; i < key.size(); i++)
    {
        if (key[i - 1] == '/' && key[i] == '/')
        {
            onError();
        }
    }
}

}

}

#endif
