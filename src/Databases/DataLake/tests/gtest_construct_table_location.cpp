#include <Databases/DataLake/Common.h>
#include <Databases/DataLake/ICatalog.h>

#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <base/types.h>

namespace DataLake::Test
{

TEST(ConstructTableLocation, S3HttpsEndpoint)
{
    EXPECT_EQ(
        constructTableLocation("s3", "http://minio:9000/warehouse-rest", "ns", "tbl"),
        "s3://warehouse-rest/ns/tbl");
    EXPECT_EQ(
        constructTableLocation("s3", "http://minio:9000/warehouse/data", "ns", "tbl"),
        "s3://warehouse/data/ns/tbl");
}

TEST(ConstructTableLocation, S3RejectsEndpointWithoutBucket)
{
    EXPECT_THROW(
        constructTableLocation("s3", "http://minio:9000/", "ns", "tbl"),
        DB::Exception);
}

TEST(ConstructTableLocation, S3VirtualHostedIsRejected)
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

TEST(ConstructTableLocation, AzureHttpsEndpoint)
{
    const String location = constructTableLocation(
        "abfss",
        "https://account.dfs.core.windows.net/mycontainer",
        "ns",
        "tbl");
    EXPECT_EQ(location, "abfss://mycontainer@account.dfs.core.windows.net/ns/tbl");

    /// Verify the generated URI round-trips through `TableMetadata::setLocation`.
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

TEST(ConstructTableLocation, AzureAbfssEndpoint)
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

TEST(ConstructTableLocation, AzureRejectsEndpointWithoutContainer)
{
    EXPECT_THROW(
        constructTableLocation("abfss", "https://account.dfs.core.windows.net/", "ns", "tbl"),
        DB::Exception);
    EXPECT_THROW(
        constructTableLocation("abfss", "abfss://account.dfs.core.windows.net/", "ns", "tbl"),
        DB::Exception);
}

TEST(ConstructTableLocation, HdfsPreservesAuthority)
{
    EXPECT_EQ(
        constructTableLocation("hdfs", "hdfs://namenode:9000/warehouse", "ns", "tbl"),
        "hdfs://namenode:9000/warehouse/ns/tbl");
    EXPECT_EQ(
        constructTableLocation("hdfs", "hdfs://namenode:9000", "ns", "tbl"),
        "hdfs://namenode:9000/ns/tbl");
}

TEST(ConstructTableLocation, FileWithoutAuthority)
{
    EXPECT_EQ(
        constructTableLocation("file", "file:///var/iceberg/warehouse", "ns", "tbl"),
        "file:///var/iceberg/warehouse/ns/tbl");
}

}
