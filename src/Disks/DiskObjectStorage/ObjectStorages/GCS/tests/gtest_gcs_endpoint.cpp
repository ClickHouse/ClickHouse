#include "config.h"

#if USE_GOOGLE_CLOUD

#include <Disks/DiskObjectStorage/ObjectStorages/GCS/gcsSettings.h>
#include <Common/Exception.h>
#include <gtest/gtest.h>

using namespace DB;

namespace
{
struct Parsed
{
    String bucket;
    String key_prefix;
    String endpoint_override;
};

Parsed parse(const String & endpoint)
{
    Parsed p;
    parseGCSEndpoint(endpoint, p.bucket, p.key_prefix, p.endpoint_override);
    return p;
}
}

TEST(GCSEndpoint, GsScheme)
{
    auto p = parse("gs://my-bucket/data/dir/");
    EXPECT_EQ(p.bucket, "my-bucket");
    EXPECT_EQ(p.key_prefix, "data/dir/");
    EXPECT_EQ(p.endpoint_override, "");
}

TEST(GCSEndpoint, PathStyleDefaultHost)
{
    auto p = parse("https://storage.googleapis.com/my-bucket/a/b");
    EXPECT_EQ(p.bucket, "my-bucket");
    EXPECT_EQ(p.key_prefix, "a/b/");
    EXPECT_EQ(p.endpoint_override, "");
}

TEST(GCSEndpoint, BucketOnlyGetsEmptyPrefix)
{
    auto p = parse("https://storage.googleapis.com/my-bucket");
    EXPECT_EQ(p.bucket, "my-bucket");
    EXPECT_EQ(p.key_prefix, "");
    EXPECT_EQ(p.endpoint_override, "");
}

TEST(GCSEndpoint, VirtualHostedStyle)
{
    auto p = parse("https://my-bucket.storage.googleapis.com/a/b/");
    EXPECT_EQ(p.bucket, "my-bucket");
    EXPECT_EQ(p.key_prefix, "a/b/");
    EXPECT_EQ(p.endpoint_override, "");
}

TEST(GCSEndpoint, EmulatorEndpointKeptAsOverride)
{
    auto p = parse("http://localhost:4443/my-bucket/prefix");
    EXPECT_EQ(p.bucket, "my-bucket");
    EXPECT_EQ(p.key_prefix, "prefix/");
    EXPECT_EQ(p.endpoint_override, "http://localhost:4443");
}

TEST(GCSEndpoint, RejectsUnknownScheme)
{
    String b, k, e;
    EXPECT_THROW(parseGCSEndpoint("ftp://my-bucket/x", b, k, e), Exception);
}

#endif
