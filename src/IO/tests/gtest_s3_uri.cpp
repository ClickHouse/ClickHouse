#include <gtest/gtest.h>
#include <Common/config.h>

#if USE_AWS_S3

#include <IO/S3Common.h>

namespace
{
using namespace DB;

struct TestCase
{
    S3::URI uri;
    String endpoint;
    String bucket;
    String key;
    String version_id;
    bool is_virtual_hosted_style;
};

const TestCase TestCases[] = {
    {S3::URI(Poco::URI("https://bucketname.s3.us-east-2.amazonaws.com/data")),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "",
     true},
    {S3::URI(Poco::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?firstKey=someKey&secondKey=anotherKey")),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "",
     true},
    {S3::URI(Poco::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?versionId=testVersionId&anotherKey=someOtherKey")),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "testVersionId",
     true},
    {S3::URI(Poco::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?firstKey=someKey&versionId=testVersionId&anotherKey=someOtherKey")),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "testVersionId",
     true},
    {S3::URI(Poco::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?anotherKey=someOtherKey&versionId=testVersionId")),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "testVersionId",
     true},
    {S3::URI(Poco::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?versionId=testVersionId")),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "testVersionId",
     true},
    {S3::URI(Poco::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?versionId=")),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "",
     true},
    {S3::URI(Poco::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?versionId&")),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "",
     true},
    {S3::URI(Poco::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?versionId")),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "",
     true},
};

class S3UriTest : public testing::TestWithParam<std::string>
{
};

TEST(S3UriTest, validPatterns)
{
    {
        S3::URI uri(Poco::URI("https://jokserfn.s3.amazonaws.com/"));
        ASSERT_EQ("https://s3.amazonaws.com", uri.endpoint);
        ASSERT_EQ("jokserfn", uri.bucket);
        ASSERT_EQ("", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri(Poco::URI("https://s3.amazonaws.com/jokserfn/"));
        ASSERT_EQ("https://s3.amazonaws.com", uri.endpoint);
        ASSERT_EQ("jokserfn", uri.bucket);
        ASSERT_EQ("", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri(Poco::URI("https://amazonaws.com/bucket/"));
        ASSERT_EQ("https://amazonaws.com", uri.endpoint);
        ASSERT_EQ("bucket", uri.bucket);
        ASSERT_EQ("", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri(Poco::URI("https://jokserfn.s3.amazonaws.com/data"));
        ASSERT_EQ("https://s3.amazonaws.com", uri.endpoint);
        ASSERT_EQ("jokserfn", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri(Poco::URI("https://storage.amazonaws.com/jokserfn/data"));
        ASSERT_EQ("https://storage.amazonaws.com", uri.endpoint);
        ASSERT_EQ("jokserfn", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri(Poco::URI("https://bucketname.cos.ap-beijing.myqcloud.com/data"));
        ASSERT_EQ("https://cos.ap-beijing.myqcloud.com", uri.endpoint);
        ASSERT_EQ("bucketname", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri(Poco::URI("https://bucketname.s3.us-east-2.amazonaws.com/data"));
        ASSERT_EQ("https://s3.us-east-2.amazonaws.com", uri.endpoint);
        ASSERT_EQ("bucketname", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri(Poco::URI("https://s3.us-east-2.amazonaws.com/bucketname/data"));
        ASSERT_EQ("https://s3.us-east-2.amazonaws.com", uri.endpoint);
        ASSERT_EQ("bucketname", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri(Poco::URI("https://bucketname.s3-us-east-2.amazonaws.com/data"));
        ASSERT_EQ("https://s3-us-east-2.amazonaws.com", uri.endpoint);
        ASSERT_EQ("bucketname", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri(Poco::URI("https://s3-us-east-2.amazonaws.com/bucketname/data"));
        ASSERT_EQ("https://s3-us-east-2.amazonaws.com", uri.endpoint);
        ASSERT_EQ("bucketname", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
}

TEST_P(S3UriTest, invalidPatterns)
{
    ASSERT_ANY_THROW(S3::URI(Poco::URI(GetParam())));
}

TEST(S3UriTest, versionIdChecks)
{
    for (const auto& test_case : TestCases)
    {
        ASSERT_EQ(test_case.endpoint, test_case.uri.endpoint);
        ASSERT_EQ(test_case.bucket, test_case.uri.bucket);
        ASSERT_EQ(test_case.key, test_case.uri.key);
        ASSERT_EQ(test_case.version_id, test_case.uri.version_id);
        ASSERT_EQ(test_case.is_virtual_hosted_style, test_case.uri.is_virtual_hosted_style);
    }
}

INSTANTIATE_TEST_SUITE_P(
    S3,
    S3UriTest,
    testing::Values(
        "https:///",
        "https://.s3.amazonaws.com/key",
        "https://s3.amazonaws.com/key",
        "https://jokserfn.s3amazonaws.com/key",
        "https://s3.amazonaws.com//",
        "https://amazonaws.com/",
        "https://amazonaws.com//",
        "https://amazonaws.com//key"));

}

#endif
