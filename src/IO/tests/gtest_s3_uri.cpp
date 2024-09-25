#include <gtest/gtest.h>
#include "config.h"

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
    {S3::URI("https://bucketname.s3.us-east-2.amazonaws.com/data"),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "",
     true},
    {S3::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?firstKey=someKey&secondKey=anotherKey"),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data?firstKey=someKey&secondKey=anotherKey",
     "",
     true},
    {S3::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?versionId=testVersionId&anotherKey=someOtherKey"),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "testVersionId",
     true},
    {S3::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?firstKey=someKey&versionId=testVersionId&anotherKey=someOtherKey"),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "testVersionId",
     true},
    {S3::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?anotherKey=someOtherKey&versionId=testVersionId"),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "testVersionId",
     true},
    {S3::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?versionId=testVersionId"),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "testVersionId",
     true},
    {S3::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?versionId="),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "",
     true},
    {S3::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?versionId&"),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "",
     true},
    {S3::URI("https://bucketname.s3.us-east-2.amazonaws.com/data?versionId"),
     "https://s3.us-east-2.amazonaws.com",
     "bucketname",
     "data",
     "",
     true},
    {S3::URI("https://bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w.s3.us-east-1.vpce.amazonaws.com/root/nested/file.txt"),
     "https://bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w.s3.us-east-1.vpce.amazonaws.com",
     "root",
     "nested/file.txt",
     "",
     false},
    // Test with a file with no extension
    {S3::URI("https://bucket.vpce-03b2c987f1bd55c5f-j3b4vg7w.s3.ap-southeast-2.vpce.amazonaws.com/some_bucket/document"),
     "https://bucket.vpce-03b2c987f1bd55c5f-j3b4vg7w.s3.ap-southeast-2.vpce.amazonaws.com",
     "some_bucket",
     "document",
     "",
     false},
    // Test with a deeply nested file path
    {S3::URI("https://bucket.vpce-0242cd56f1bd55c5f-l5b7vg8x.s3.sa-east-1.vpce.amazonaws.com/some_bucket/b/c/d/e/f/g/h/i/j/data.json"),
     "https://bucket.vpce-0242cd56f1bd55c5f-l5b7vg8x.s3.sa-east-1.vpce.amazonaws.com",
     "some_bucket",
     "b/c/d/e/f/g/h/i/j/data.json",
     "",
     false},
    // Zonal
    {S3::URI("https://bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w-us-east-1a.s3.us-east-1.vpce.amazonaws.com/root/nested/file.txt"),
     "https://bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w-us-east-1a.s3.us-east-1.vpce.amazonaws.com",
     "root",
     "nested/file.txt",
     "",
     false},
    // Non standard port
    {S3::URI("https://bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w-us-east-1a.s3.us-east-1.vpce.amazonaws.com:65535/root/nested/file.txt"),
     "https://bucket.vpce-07a1cd78f1bd55c5f-j3a3vg6w-us-east-1a.s3.us-east-1.vpce.amazonaws.com:65535",
     "root",
     "nested/file.txt",
     "",
     false},
};

class S3UriTest : public testing::TestWithParam<std::string>
{
};

TEST(S3UriTest, validPatterns)
{
    {
        S3::URI uri("https://jokserfn.s3.amazonaws.com/");
        ASSERT_EQ("https://s3.amazonaws.com", uri.endpoint);
        ASSERT_EQ("jokserfn", uri.bucket);
        ASSERT_EQ("", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://s3.amazonaws.com/jokserfn/");
        ASSERT_EQ("https://s3.amazonaws.com", uri.endpoint);
        ASSERT_EQ("jokserfn", uri.bucket);
        ASSERT_EQ("", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://amazonaws.com/bucket/");
        ASSERT_EQ("https://amazonaws.com", uri.endpoint);
        ASSERT_EQ("bucket", uri.bucket);
        ASSERT_EQ("", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://jokserfn.s3.amazonaws.com/data");
        ASSERT_EQ("https://s3.amazonaws.com", uri.endpoint);
        ASSERT_EQ("jokserfn", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://storage.amazonaws.com/jokserfn/data");
        ASSERT_EQ("https://storage.amazonaws.com", uri.endpoint);
        ASSERT_EQ("jokserfn", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://bucketname.cos.ap-beijing.myqcloud.com/data");
        ASSERT_EQ("https://cos.ap-beijing.myqcloud.com", uri.endpoint);
        ASSERT_EQ("bucketname", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://bucketname.s3.us-east-2.amazonaws.com/data");
        ASSERT_EQ("https://s3.us-east-2.amazonaws.com", uri.endpoint);
        ASSERT_EQ("bucketname", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://s3.us-east-2.amazonaws.com/bucketname/data");
        ASSERT_EQ("https://s3.us-east-2.amazonaws.com", uri.endpoint);
        ASSERT_EQ("bucketname", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://bucketname.s3-us-east-2.amazonaws.com/data");
        ASSERT_EQ("https://s3-us-east-2.amazonaws.com", uri.endpoint);
        ASSERT_EQ("bucketname", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://s3-us-east-2.amazonaws.com/bucketname/data");
        ASSERT_EQ("https://s3-us-east-2.amazonaws.com", uri.endpoint);
        ASSERT_EQ("bucketname", uri.bucket);
        ASSERT_EQ("data", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(false, uri.is_virtual_hosted_style);
    }
    {
        S3::URI uri("https://test-perf-bucket--eun1-az1--x-s3.s3express-eun1-az1.eu-north-1.amazonaws.com/test.csv");
        ASSERT_EQ("https://s3express-eun1-az1.eu-north-1.amazonaws.com", uri.endpoint);
        ASSERT_EQ("test-perf-bucket--eun1-az1--x-s3", uri.bucket);
        ASSERT_EQ("test.csv", uri.key);
        ASSERT_EQ("", uri.version_id);
        ASSERT_EQ(true, uri.is_virtual_hosted_style);
    }
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

}
#endif
