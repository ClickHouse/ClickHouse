#include <gtest/gtest.h>

#include <IO/S3/URI.h>
#include "config.h"


#if USE_AWS_S3
#include <IO/S3/Client.h>
#include <aws/s3/S3EndpointProvider.h>

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

TEST(IOTestS3URI, MRAPArn)
{
    const auto uri = DB::S3::URI::fromMRAPArn(
        "arn:aws:s3::123456789012:accesspoint/mfzwi23gnjvgw.mrap", "data/smoke.csv");
    EXPECT_TRUE(uri.is_mrap);
    EXPECT_TRUE(uri.is_virtual_hosted_style);
    EXPECT_TRUE(uri.endpoint.empty());
    EXPECT_EQ(uri.bucket, "arn:aws:s3::123456789012:accesspoint/mfzwi23gnjvgw.mrap");
    EXPECT_EQ(uri.key, "data/smoke.csv");
    EXPECT_EQ(uri.uri.toString(), "https://mfzwi23gnjvgw.mrap.accesspoint.s3-global.amazonaws.com/data/smoke.csv");
}

TEST(IOTestS3URI, MRAPKeyIsNotAURL)
{
    const std::string key = "/a//b%2Fc?#.csv";
    const auto uri = DB::S3::URI::fromMRAPArn(
        "arn:aws:s3::123456789012:accesspoint:mfzwi23gnjvgw.mrap", key);
    EXPECT_EQ(uri.bucket, "arn:aws:s3::123456789012:accesspoint/mfzwi23gnjvgw.mrap");
    EXPECT_EQ(uri.key, key);
    EXPECT_EQ(uri.uri.getPath(), "/" + key);
    EXPECT_TRUE(uri.uri.getQuery().empty());
    EXPECT_TRUE(uri.uri.getFragment().empty());
}

TEST(IOTestS3URI, InvalidMRAPTarget)
{
    for (const auto * arn : {
        "mfzwi23gnjvgw.mrap",
        "arn:aws:s3:us-east-1:123456789012:accesspoint/mfzwi23gnjvgw.mrap",
        "arn:aws:s3::1234:accesspoint/mfzwi23gnjvgw.mrap",
        "arn:aws:s3::123456789012:accesspoint/regional-ap",
        "arn:aws:s3::123456789012:accesspoint/mfzwi23gnjvgw.mrap/object",
        "arn:aws-cn:s3::123456789012:accesspoint/mfzwi23gnjvgw.mrap"})
    {
        EXPECT_ANY_THROW(DB::S3::URI::fromMRAPArn(arn, "key"));
    }
    EXPECT_ANY_THROW(DB::S3::URI::fromMRAPArn("arn:aws:s3::123456789012:accesspoint/mfzwi23gnjvgw.mrap", ""));
}

TEST(IOTestS3URI, MRAPEndpointSigning)
{
    DB::S3::ClientFactory::instance();
    Aws::S3::Endpoint::S3EndpointProvider provider;
    Aws::Endpoint::EndpointParameters parameters{
        {"Bucket", "arn:aws:s3::123456789012:accesspoint/mfzwi23gnjvgw.mrap"},
        {"Region", "us-east-1"},
        {"ForcePathStyle", false}};
    const auto outcome = provider.ResolveEndpoint(parameters);
    ASSERT_TRUE(outcome.IsSuccess());
    EXPECT_EQ(outcome.GetResult().GetURI().GetURIString(),
        "https://mfzwi23gnjvgw.mrap.accesspoint.s3-global.amazonaws.com");
    const auto & attributes = outcome.GetResult().GetAttributes();
    ASSERT_TRUE(attributes);
    EXPECT_EQ(attributes->authScheme.GetName(), "AsymmetricSignatureV4");
    ASSERT_TRUE(attributes->authScheme.GetSigningName());
    EXPECT_EQ(*attributes->authScheme.GetSigningName(), "s3");
    ASSERT_TRUE(attributes->authScheme.GetSigningRegionSet());
    EXPECT_EQ(*attributes->authScheme.GetSigningRegionSet(), "*");

    parameters.emplace_back("DisableMultiRegionAccessPoints", true);
    EXPECT_FALSE(provider.ResolveEndpoint(parameters).IsSuccess());
}

TEST(IOTestS3URI, MRAPCacheIdentityIncludesAccount)
{
    auto & registry = DB::S3::ClientCacheRegistry::instance();
    auto first = registry.getOrCreateCacheForKey("", "arn:aws:s3::123456789012:accesspoint/mfzwi23gnjvgw.mrap");
    auto same = registry.getOrCreateCacheForKey("", "arn:aws:s3::123456789012:accesspoint/mfzwi23gnjvgw.mrap");
    auto other = registry.getOrCreateCacheForKey("", "arn:aws:s3::123456789013:accesspoint/mfzwi23gnjvgw.mrap");
    EXPECT_EQ(first, same);
    EXPECT_NE(first, other);
}

#endif
