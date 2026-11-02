#include "config.h"

#if USE_GOOGLE_CLOUD

#include <Disks/DiskObjectStorage/ObjectStorages/GCS/gcsSettings.h>
#include <Common/RemoteHostFilter.h>

#include <Poco/AutoPtr.h>
#include <Poco/DOM/DOMParser.h>
#include <Poco/URI.h>
#include <Poco/Util/XMLConfiguration.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

void configureFilter(RemoteHostFilter & filter, const std::string & xml)
{
    Poco::XML::DOMParser dom_parser;
    Poco::AutoPtr<Poco::XML::Document> document = dom_parser.parseString(xml);
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(document);
    filter.setValuesFromConfig(*config);
}

bool allowed(const RemoteHostFilter & filter, const std::string & url, std::string_view endpoint_path = "")
{
    return isGCSRequestURLAllowed(filter, Poco::URI(url, /* enable_url_encoding */ false), endpoint_path);
}

}

/// A `<s3_bucket>` entry grants the requests to that bucket of the native client, as it does for the
/// S3-compatibility path, without granting the other buckets of the host or any other host.
TEST(GCSRemoteHostFilter, BucketScopedPermission)
{
    RemoteHostFilter filter;
    configureFilter(filter, R"CONFIG(<clickhouse>
    <remote_url_allow_hosts>
        <s3_bucket>storage.googleapis.com/my-bucket</s3_bucket>
    </remote_url_allow_hosts>
</clickhouse>)CONFIG");

    EXPECT_TRUE(allowed(filter, "https://storage.googleapis.com/storage/v1/b/my-bucket/o/data.tsv?alt=media"));
    EXPECT_TRUE(allowed(filter, "https://storage.googleapis.com/storage/v1/b/my-bucket/o?prefix=data"));
    EXPECT_TRUE(allowed(filter, "https://storage.googleapis.com/upload/storage/v1/b/my-bucket/o?uploadType=resumable&upload_id=1"));

    /// Another bucket of the same host.
    EXPECT_FALSE(allowed(filter, "https://storage.googleapis.com/storage/v1/b/other-bucket/o/data.tsv?alt=media"));
    EXPECT_FALSE(allowed(filter, "https://storage.googleapis.com/upload/storage/v1/b/other-bucket/o?uploadType=resumable"));
    /// A request of the host that does not address the bucket.
    EXPECT_FALSE(allowed(filter, "https://storage.googleapis.com/my-bucket/data.tsv"));
    EXPECT_FALSE(allowed(filter, "https://storage.googleapis.com/batch/storage/v1"));
    /// A foreign destination, such as the session URL of a resumable upload handed out by the server.
    EXPECT_FALSE(allowed(filter, "https://evil.example/upload/storage/v1/b/my-bucket/o?uploadType=resumable&upload_id=1"));
}

TEST(GCSRemoteHostFilter, BucketScopedPermissionBelowEndpointPath)
{
    RemoteHostFilter filter;
    configureFilter(filter, R"CONFIG(<clickhouse>
    <remote_url_allow_hosts>
        <s3_bucket>gcs.example:4443/my-bucket</s3_bucket>
    </remote_url_allow_hosts>
</clickhouse>)CONFIG");

    EXPECT_TRUE(allowed(filter, "http://gcs.example:4443/prefix/storage/v1/b/my-bucket/o/x", "/prefix/"));
    EXPECT_FALSE(allowed(filter, "http://gcs.example:4443/storage/v1/b/my-bucket/o/x", "/prefix/"));
    EXPECT_FALSE(allowed(filter, "http://gcs.example:4443/prefix/storage/v1/b/other-bucket/o/x", "/prefix/"));
}

TEST(GCSRemoteHostFilter, HostPermission)
{
    RemoteHostFilter filter;
    configureFilter(filter, R"CONFIG(<clickhouse>
    <remote_url_allow_hosts>
        <host>storage.googleapis.com</host>
    </remote_url_allow_hosts>
</clickhouse>)CONFIG");

    EXPECT_TRUE(allowed(filter, "https://storage.googleapis.com/storage/v1/b/any-bucket/o/data.tsv"));
    EXPECT_FALSE(allowed(filter, "https://evil.example/upload/storage/v1/b/any-bucket/o"));
}

#endif
