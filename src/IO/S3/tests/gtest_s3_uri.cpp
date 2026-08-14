#include <gtest/gtest.h>

#include <IO/S3/URI.h>
#include "config.h"


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

TEST(IOTestS3URI, ResolveS3Endpoint)
{
    using namespace DB;

    ASSERT_EQ(S3::resolveS3Endpoint("us-east-1"),
              "https://s3.us-east-1.amazonaws.com");
    ASSERT_EQ(S3::resolveS3Endpoint("eu-west-1"),
              "https://s3.eu-west-1.amazonaws.com");

    auto cn_north = S3::resolveS3Endpoint("cn-north-1");
    ASSERT_TRUE(cn_north.ends_with(".amazonaws.com.cn"))
        << "China region should resolve to .amazonaws.com.cn suffix, got: " << cn_north;
    ASSERT_TRUE(cn_north.find("cn-north-1") != std::string::npos)
        << "Got: " << cn_north;

    ASSERT_EQ(S3::resolveS3Endpoint("us-gov-west-1"),
              "https://s3.us-gov-west-1.amazonaws.com");
}

#endif
