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

TEST(IOTestS3URI, AddRegionUpdatesAuthority)
{
    using namespace DB;

    auto uri = S3::URI("https://bucket.s3.amazonaws.com/key");
    uri.addRegionToURI("eu-west-1");

    ASSERT_EQ(uri.endpoint, "https://s3.eu-west-1.amazonaws.com");
    ASSERT_EQ(uri.uri.getAuthority(), "bucket.s3.eu-west-1.amazonaws.com");

    auto http_uri = S3::URI("http://bucket.s3.amazonaws.com/key");
    http_uri.addRegionToURI("eu-west-1");

    ASSERT_EQ(http_uri.endpoint, "http://s3.eu-west-1.amazonaws.com");
    ASSERT_EQ(http_uri.uri.getAuthority(), "bucket.s3.eu-west-1.amazonaws.com");
}

#endif
