#include <gtest/gtest.h>
#include <Common/config.h>

#if USE_AWS_S3

#    include <IO/S3Common.h>

namespace
{
using namespace DB;

class S3UriTest : public testing::TestWithParam<std::string>
{
};

TEST(S3UriTest, validPatterns)
{
    {
        S3::URI uri(Poco::URI("https://jokserfn.s3.yandexcloud.net/"));
        ASSERT_EQ("https://s3.yandexcloud.net", uri.endpoint);
        ASSERT_EQ("jokserfn", uri.bucket);
        ASSERT_EQ("", uri.key);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri(Poco::URI("https://s3.yandexcloud.net/jokserfn/"));
        ASSERT_EQ("https://s3.yandexcloud.net", uri.endpoint);
        ASSERT_EQ("jokserfn", uri.bucket);
        ASSERT_EQ("", uri.key);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri(Poco::URI("https://yandexcloud.net/bucket/"));
        ASSERT_EQ("https://yandexcloud.net", uri.endpoint);
        ASSERT_EQ("bucket", uri.bucket);
        ASSERT_EQ("", uri.key);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri(Poco::URI("https://jokserfn.s3.yandexcloud.net/data"));
        ASSERT_EQ("https://s3.yandexcloud.net", uri.endpoint);
        ASSERT_EQ("jokserfn", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri(Poco::URI("https://storage.yandexcloud.net/jokserfn/data"));
        ASSERT_EQ("https://storage.yandexcloud.net", uri.endpoint);
        ASSERT_EQ("jokserfn", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri(Poco::URI("https://bucketname.cos.ap-beijing.myqcloud.com/data"));
        ASSERT_EQ("https://cos.ap-beijing.myqcloud.com", uri.endpoint);
        ASSERT_EQ("bucketname", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri(Poco::URI("https://bucketname.s3.us-east-2.amazonaws.com/data"));
        ASSERT_EQ("https://s3.us-east-2.amazonaws.com", uri.endpoint);
        ASSERT_EQ("bucketname", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri(Poco::URI("https://s3.us-east-2.amazonaws.com/bucketname/data"));
        ASSERT_EQ("https://s3.us-east-2.amazonaws.com", uri.endpoint);
        ASSERT_EQ("bucketname", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri(Poco::URI("https://bucketname.s3-us-east-2.amazonaws.com/data"));
        ASSERT_EQ("https://s3-us-east-2.amazonaws.com", uri.endpoint);
        ASSERT_EQ("bucketname", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri(Poco::URI("https://s3-us-east-2.amazonaws.com/bucketname/data"));
        ASSERT_EQ("https://s3-us-east-2.amazonaws.com", uri.endpoint);
        ASSERT_EQ("bucketname", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
}

TEST_P(S3UriTest, invalidPatterns)
{
    ASSERT_ANY_THROW(S3::URI(Poco::URI(GetParam())));
}

INSTANTIATE_TEST_SUITE_P(
    S3,
    S3UriTest,
    testing::Values(
        "https:///",
        "https://.s3.yandexcloud.net/key",
        "https://s3.yandexcloud.net/key",
        "https://jokserfn.s3yandexcloud.net/key",
        "https://s3.yandexcloud.net//",
        "https://yandexcloud.net/",
        "https://yandexcloud.net//",
        "https://yandexcloud.net//key"));

}

#endif
