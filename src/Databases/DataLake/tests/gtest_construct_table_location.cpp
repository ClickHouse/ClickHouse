#include <Databases/DataLake/Common.h>
#include <Common/Exception.h>

#include <gtest/gtest.h>

namespace DataLake::Test
{

TEST(ConstructTableLocation, StorageSchemes)
{
    EXPECT_EQ(
        constructTableLocation("s3", "http://minio:9000/warehouse/data/", "ns", "tbl"),
        "s3://warehouse/data/ns/tbl");
    EXPECT_EQ(
        constructTableLocation("abfss", "https://account.dfs.core.windows.net/container/", "ns", "tbl"),
        "abfss://container@account.dfs.core.windows.net/ns/tbl");
    EXPECT_EQ(
        constructTableLocation("abfss", "https://account.dfs.core.windows.net/container/data", "ns", "tbl"),
        "abfss://container@account.dfs.core.windows.net/data/ns/tbl");
    EXPECT_EQ(
        constructTableLocation("abfss", "abfss://container@account.dfs.core.windows.net/data", "ns", "tbl"),
        "abfss://container@account.dfs.core.windows.net/data/ns/tbl");
    EXPECT_EQ(
        constructTableLocation("hdfs", "hdfs://namenode:9000/warehouse", "ns", "tbl"),
        "hdfs://namenode:9000/warehouse/ns/tbl");
    EXPECT_EQ(
        constructTableLocation("hdfs", "hdfs://namenode:9000", "ns", "tbl"),
        "hdfs://namenode:9000/ns/tbl");
    EXPECT_EQ(
        constructTableLocation("file", "file:///var/iceberg/warehouse", "ns", "tbl"),
        "file:///var/iceberg/warehouse/ns/tbl");
}

TEST(ConstructTableLocation, InvalidEndpoints)
{
    EXPECT_THROW(constructTableLocation("s3", "http://minio:9000/", "ns", "tbl"), DB::Exception);
    EXPECT_THROW(
        constructTableLocation("s3", "https://bucket.s3.amazonaws.com", "ns", "tbl", DB::S3UriStyle::VIRTUAL_HOSTED),
        DB::Exception);
    EXPECT_THROW(constructTableLocation("abfss", "https://account.dfs.core.windows.net/", "ns", "tbl"), DB::Exception);
}

}
