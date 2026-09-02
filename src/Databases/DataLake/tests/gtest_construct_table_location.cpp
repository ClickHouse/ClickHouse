#include <Databases/DataLake/Common.h>
#include <Databases/DataLake/ICatalog.h>

#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <base/types.h>

namespace DataLake::Test
{

class ConstructTableLocationTest : public ::testing::Test
{
};

TEST_F(ConstructTableLocationTest, S3HttpsEndpoint)
{
    EXPECT_EQ(
        constructTableLocation("s3", "http://minio:9000/warehouse-rest", "ns", "tbl"),
        "s3://warehouse-rest/ns/tbl");
    EXPECT_EQ(
        constructTableLocation("s3", "http://minio:9000/warehouse/data", "ns", "tbl"),
        "s3://warehouse/data/ns/tbl");
}

TEST_F(ConstructTableLocationTest, S3RejectsEndpointWithoutBucket)
{
    EXPECT_THROW(
        constructTableLocation("s3", "http://minio:9000/", "ns", "tbl"),
        DB::Exception);
}

/// A virtual-hosted host cannot be split into bucket and service unambiguously, so `CREATE TABLE` rejects it
/// whatever the endpoint looks like; `default_base_location` has to be set instead.
TEST_F(ConstructTableLocationTest, S3VirtualHostedIsRejected)
{
    for (const auto * endpoint : {
             "https://warehouse-rest.minio.example.com",
             "https://warehouse-rest.minio.example.com/prefix",
             "https://s3.us-east-1.amazonaws.com",
             "https://10.0.0.5:9000",
         })
        EXPECT_THROW(
            constructTableLocation("s3", endpoint, "ns", "tbl", DB::S3UriStyle::VIRTUAL_HOSTED),
            DB::Exception);
}

/// The constructed Azure URI must round-trip through `setLocation`, which means it has to carry the
/// `<container>@<host>` authority.
TEST_F(ConstructTableLocationTest, AzureHttpsEndpoint)
{
    const String location = constructTableLocation(
        "abfss",
        "https://account.dfs.core.windows.net/mycontainer",
        "ns",
        "tbl");
    EXPECT_EQ(location, "abfss://mycontainer@account.dfs.core.windows.net/ns/tbl");

    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation(location);
    EXPECT_EQ(metadata.getLocation(), location);
    EXPECT_EQ(metadata.getStorageType(), StorageType::Azure);

    EXPECT_EQ(
        constructTableLocation(
            "abfss",
            "https://account.dfs.core.windows.net/mycontainer/warehouse/data",
            "ns",
            "tbl"),
        "abfss://mycontainer@account.dfs.core.windows.net/warehouse/data/ns/tbl");
    EXPECT_EQ(
        constructTableLocation(
            "abfss",
            "https://account.dfs.core.windows.net/mycontainer/",
            "ns",
            "tbl"),
        "abfss://mycontainer@account.dfs.core.windows.net/ns/tbl");
}

TEST_F(ConstructTableLocationTest, AzureAbfssEndpoint)
{
    EXPECT_EQ(
        constructTableLocation(
            "abfss",
            "abfss://mycontainer@account.dfs.core.windows.net/",
            "ns",
            "tbl"),
        "abfss://mycontainer@account.dfs.core.windows.net/ns/tbl");
    EXPECT_EQ(
        constructTableLocation(
            "abfss",
            "abfss://mycontainer@account.dfs.core.windows.net/warehouse/data",
            "ns",
            "tbl"),
        "abfss://mycontainer@account.dfs.core.windows.net/warehouse/data/ns/tbl");
}

TEST_F(ConstructTableLocationTest, AzureRejectsEndpointWithoutContainer)
{
    EXPECT_THROW(
        constructTableLocation("abfss", "https://account.dfs.core.windows.net/", "ns", "tbl"),
        DB::Exception);
    EXPECT_THROW(
        constructTableLocation("abfss", "abfss://account.dfs.core.windows.net/", "ns", "tbl"),
        DB::Exception);
}

TEST_F(ConstructTableLocationTest, HdfsPreservesAuthority)
{
    EXPECT_EQ(
        constructTableLocation("hdfs", "hdfs://namenode:9000/warehouse", "ns", "tbl"),
        "hdfs://namenode:9000/warehouse/ns/tbl");
    EXPECT_EQ(
        constructTableLocation("hdfs", "hdfs://namenode:9000", "ns", "tbl"),
        "hdfs://namenode:9000/ns/tbl");
}

TEST_F(ConstructTableLocationTest, FileWithoutAuthority)
{
    EXPECT_EQ(
        constructTableLocation("file", "file:///var/iceberg/warehouse", "ns", "tbl"),
        "file:///var/iceberg/warehouse/ns/tbl");
}

}
