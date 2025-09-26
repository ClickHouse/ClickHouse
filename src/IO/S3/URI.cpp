#include <IO/S3/URI.h>
#include <Poco/String.h>

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
    static const RE2 virtual_hosted_style_pattern(R"((.+)\.(s3express[\-a-z0-9]+|s3|cos|obs|oss-data-acc|oss|eos)([.\-][a-z0-9\-.:]+))");

    /// Case when AWS Private Link Interface is being used
    /// E.g. (bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w.s3.us-east-1.vpce.amazonaws.com/bucket-name/key)
    /// https://docs.aws.amazon.com/AmazonS3/latest/userguide/privatelink-interface-endpoints.html
    static const RE2 aws_private_link_style_pattern(R"(bucket\.vpce\-([a-z0-9\-.]+)\.vpce\.amazonaws\.com(:\d{1,5})?)");

    /// Case when bucket name and key represented in the path of S3 URL.
    /// E.g. (https://s3.region.amazonaws.com/bucket-name/key)
    /// https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html#path-style-access
    static const RE2 path_style_pattern("^/([^/]*)(?:/?(.*))");

    if (allow_archive_path_syntax)
        std::tie(uri_str, archive_pattern) = getURIAndArchivePattern(uri_);
    else
        uri_str = uri_;

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
    }

    storage_name = "S3";

    /// Keep the original string/scheme for decisions that must happen before Poco parses the URI
    const String & original = uri_str; // after getURIAndArchivePattern, but before Poco::URI
    String original_scheme;
    if (auto p = original.find("://"); p != String::npos)
        original_scheme = Poco::toLower(original.substr(0, p));

    const bool is_s3_style =
        original_scheme == "s3" ||
        original_scheme == "minio"; // probably we can expand this list

    const bool contains_qmark = original.find('?') != String::npos;
    /// detect presence of 'versionId' key in the query, with or without '='
    auto has_version_id_key = [](const String &url) -> bool
    {
        auto qpos = url.find('?');
        if (qpos == String::npos)
            return false;
        String q = url.substr(qpos + 1);
        size_t start = 0;
        while (start <= q.size())
        {
            size_t amp = q.find('&', start);
            String token = q.substr(start, amp == String::npos ? q.size() - start : amp - start);
            if (!token.empty())
            {
                size_t eq = token.find('=');
                String name = token.substr(0, eq);
                if (name == "versionId")
                    return true;
            }
            if (amp == String::npos) break;
            start = amp + 1;
        }
        return false;
    };
    const bool has_version_id_hint = has_version_id_key(original);

    /// only for non-S3 schemes: if there is a '?' but no versionId, treat '?' as a path wildcard and encode it
    Poco::URI working_uri;
    if (!is_s3_style && contains_qmark && !has_version_id_hint)
    {
        String encoded;
        Poco::URI::encode(original, "?", encoded); // encode only '?' characters
        working_uri = Poco::URI(encoded);
    }
    else
    {
        working_uri = Poco::URI(original);
    }

    // then apply mapping if present
    if (!mapper.empty())
        URIConverter::modifyURI(working_uri, mapper);

    uri = working_uri;

    if (uri.getHost().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Host is empty in S3 URI.");

    /// Extract object version ID from query string.
    bool has_version_id = false;
    for (const auto & [query_key, query_value] : uri.getQueryParameters())
    {
        if (query_key == "versionId")
        {
            version_id = query_value; // may be empty
            has_version_id = true;
            break;
        }
    }
    if (!has_version_id)
    {
        /// fallback for bare 'versionId' token (no '=')
        String q = uri.getQuery();
        if (!q.empty())
        {
            size_t start = 0;
            while (start <= q.size())
            {
                size_t amp = q.find('&', start);
                String token = q.substr(start, amp == String::npos ? q.size() - start : amp - start);
                if (!token.empty())
                {
                    size_t eq = token.find('=');
                    String name = token.substr(0, eq);
                    if (name == "versionId")
                    {
                        version_id.clear(); // treat as present with empty value
                        break;
                    }
                }
                if (amp == String::npos) break;
                start = amp + 1;
            }
        }
    }

    String name;
    String endpoint_authority_from_uri;

    bool is_using_aws_private_link_interface = re2::RE2::FullMatch(uri.getAuthority(), aws_private_link_style_pattern);

    if (!is_using_aws_private_link_interface
        && re2::RE2::FullMatch(uri.getAuthority(), virtual_hosted_style_pattern, &bucket, &name, &endpoint_authority_from_uri))
    {
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
    }
    else if (re2::RE2::PartialMatch(uri.getPath(), path_style_pattern, &bucket, &key))
    {
        is_virtual_hosted_style = false;
        endpoint = uri.getScheme() + "://" + uri.getAuthority();
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

    validateBucket(bucket, uri);
    validateKey(key, uri);
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
