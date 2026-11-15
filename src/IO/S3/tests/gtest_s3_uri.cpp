#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/RemoteHostFilter.h>
#include <IO/S3/URI.h>
#include "config.h"

#include <Poco/AutoPtr.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/Util/XMLConfiguration.h>


#if USE_AWS_S3

TEST(IOTestS3URI, PathStyleNoKey)
{
    using namespace DB;

    auto uri_with_no_key_and_no_slash = S3::URI("https://s3.region.amazonaws.com/bucket-name");

    ASSERT_EQ(uri_with_no_key_and_no_slash.bucket, "bucket-name");
    ASSERT_EQ(uri_with_no_key_and_no_slash.key, "");

    auto uri_with_no_key_and_with_slash = S3::URI("https://s3.region.amazonaws.com/bucket-name/");

    ASSERT_EQ(uri_with_no_key_and_with_slash.bucket, "bucket-name");
    ASSERT_EQ(uri_with_no_key_and_with_slash.key, "");

    ASSERT_ANY_THROW(S3::URI("https://s3.region.amazonaws.com/bucket-name//"));
}

TEST(IOTestS3URI, PathStyleWithKey)
{
    using namespace DB;

    auto uri_with_no_key_and_no_slash = S3::URI("https://s3.region.amazonaws.com/bucket-name/key");

    ASSERT_EQ(uri_with_no_key_and_no_slash.bucket, "bucket-name");
    ASSERT_EQ(uri_with_no_key_and_no_slash.key, "key");

    auto uri_with_no_key_and_with_slash = S3::URI("https://s3.region.amazonaws.com/bucket-name/key/key/key/key");

    ASSERT_EQ(uri_with_no_key_and_with_slash.bucket, "bucket-name");
    ASSERT_EQ(uri_with_no_key_and_with_slash.key, "key/key/key/key");
}

namespace DB::ErrorCodes
{
extern const int UNACCEPTABLE_URL;
}

/// `S3::URI::checkRemoteHostFilter` accepts a URL when its endpoint and bucket match an <s3_bucket>
/// entry, and otherwise falls back to the host check against <host> and <host_regexp>.
TEST(IOTestS3URI, CheckRemoteHostFilter)
{
    using namespace DB;

    Poco::XML::DOMParser dom_parser;
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(R"CONFIG(<clickhouse>
    <remote_url_allow_hosts>
        <host>allowed.example.com</host>
        <s3_bucket>s3.example.com/my-bucket</s3_bucket>
        <s3_bucket>minio1:9001/root</s3_bucket>
    </remote_url_allow_hosts>
</clickhouse>)CONFIG");
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);

    RemoteHostFilter filter;
    filter.setValuesFromConfig(*config);

    const auto allowed = [&](const std::string & url)
    {
        EXPECT_NO_THROW(S3::URI(url).checkRemoteHostFilter(filter)) << url;
    };
    const auto rejected = [&](const std::string & url)
    {
        try
        {
            S3::URI(url).checkRemoteHostFilter(filter);
            FAIL() << "allowed " << url;
        }
        catch (const Exception & e)
        {
            EXPECT_EQ(e.code(), ErrorCodes::UNACCEPTABLE_URL) << url;
        }
    };

    /// One bucket entry covers the path-style and the virtual-hosted form of the same bucket,
    /// with or without the default port written out.
    allowed("https://s3.example.com/my-bucket/key");
    allowed("https://s3.example.com:443/my-bucket/key");
    allowed("https://my-bucket.s3.example.com/key");

    /// Another bucket on the same endpoint, a bucket whose name starts with the allowed name,
    /// and the allowed bucket name on another endpoint are rejected.
    rejected("https://s3.example.com/other-bucket/key");
    rejected("https://s3.example.com/my-bucket-2/key");
    rejected("https://other-bucket.s3.example.com/key");
    rejected("https://s3.example.org/my-bucket/key");

    /// An entry with a port matches that port only.
    allowed("http://minio1:9001/root/key");
    rejected("http://minio1:9002/root/key");
    rejected("http://minio1/root/key");

    /// A <host> entry keeps working for S3 URLs: every bucket on that host is allowed.
    allowed("https://allowed.example.com/any-bucket/key");
    allowed("http://allowed.example.com:8080/any-bucket/key");
}

#endif
