#include <Common/config.h>

#if USE_AWS_S3

#include <IO/COSCommon.h>
#include <Poco/URI.h>
#include <re2/re2.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace COS 
{
    URI::URI(const Poco::URI & uri_)
    {
        uri = uri_;
        key = uri_.getPath();
        /// Case when bucket name represented in domain name of COS URL.
        /// E.g. (https://bucketname.cos.ap-hongkong.myqcloud.com/test.dat)
        if (uri.getScheme() == "https")
            is_https_scheme = true;

        // Parse bucket, region and endpoint from path.
        static const RE2 BUCKET_REGION_PATTERN(R"((^.*-[0-9]*)\.cos\.(ap-[a-z]*))");
        static const RE2 ENDPOINT_PATTERN(R"(^.*-[0-9]*\.(cos.*))");
        const std::string bucket_region_endpoint = uri.getAuthority();
        if (!re2::RE2::PartialMatch(bucket_region_endpoint, BUCKET_REGION_PATTERN, &bucket, &region))
        {
            throw Exception ("Invalid COS URI: no bucket or region: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);
        }
        if (!re2::RE2::PartialMatch(bucket_region_endpoint, ENDPOINT_PATTERN, &endpoint)) {
            throw Exception ("Invalid COS URI: no endpoint: " + uri.toString(), ErrorCodes::BAD_ARGUMENTS);
        }
    }
}

}

#endif
