#include <Databases/DataLake/ICatalog.h>
#include <gtest/gtest.h>
#include <Common/Exception.h>

namespace DataLake::Test
{

TEST(StorageTypeParsingTest, ParsesHDFS)
{
    EXPECT_EQ(parseStorageTypeFromString("hdfs"), StorageType::HDFS);
    EXPECT_EQ(parseStorageTypeFromString("hdfs://"), StorageType::HDFS);
}

TEST(StorageTypeParsingTest, RejectsUppercaseHDFS)
{
    EXPECT_THROW(parseStorageTypeFromString("HDFS"), DB::Exception);
    EXPECT_THROW(parseStorageTypeFromString("HDFS://"), DB::Exception);
    EXPECT_THROW(parseStorageTypeFromLocation("HDFS://namenode/warehouse/table"), DB::Exception);
}

TEST(StorageTypeParsingTest, ParsesHDFSLocation)
{
    EXPECT_EQ(parseStorageTypeFromLocation("hdfs://namenode/warehouse/table"), StorageType::HDFS);
}

TEST(StorageTypeParsingTest, PreservesExistingStorageSchemes)
{
    EXPECT_EQ(parseStorageTypeFromString("s3"), StorageType::S3);
    EXPECT_EQ(parseStorageTypeFromString("s3a"), StorageType::S3);
    EXPECT_EQ(parseStorageTypeFromString("azure"), StorageType::Azure);
    EXPECT_EQ(parseStorageTypeFromString("abfss"), StorageType::Azure);
    EXPECT_EQ(parseStorageTypeFromString("file"), StorageType::Local);
    EXPECT_EQ(parseStorageTypeFromString("local"), StorageType::Local);
}

TEST(StorageTypeParsingTest, TableMetadataAcceptsHDFSLocation)
{
    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation("hdfs://namenode/warehouse/table");

    EXPECT_EQ(metadata.getStorageType(), StorageType::HDFS);
    EXPECT_EQ(metadata.getLocation(), "hdfs://namenode/warehouse/table");
}

}
