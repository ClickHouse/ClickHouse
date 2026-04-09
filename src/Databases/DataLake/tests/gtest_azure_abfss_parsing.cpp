#include <Databases/DataLake/ICatalog.h>
#include <gtest/gtest.h>
#include <Common/Exception.h>
#include <base/types.h>

namespace DB::ErrorCodes
{
extern const int NOT_IMPLEMENTED;
}

namespace DataLake::Test
{

class AzureAbfssParsingTest : public ::testing::Test
{
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(AzureAbfssParsingTest, ParseStorageTypeFromStringAbfss)
{
    auto storage_type = parseStorageTypeFromString("abfss");
    EXPECT_EQ(storage_type, StorageType::Azure);
}

TEST_F(AzureAbfssParsingTest, ParseStorageTypeFromStringAbfssWithProtocol)
{
    auto storage_type = parseStorageTypeFromString("abfss://");
    EXPECT_EQ(storage_type, StorageType::Azure);
}

TEST_F(AzureAbfssParsingTest, ParseStorageTypeFromLocationAzureAbfss)
{
    auto storage_type = parseStorageTypeFromLocation("abfss://container@account.dfs.core.windows.net/path/to/data");
    EXPECT_EQ(storage_type, StorageType::Azure);
}

TEST_F(AzureAbfssParsingTest, ParseStorageTypeFromStringS3)
{
    auto storage_type = parseStorageTypeFromString("s3");
    EXPECT_EQ(storage_type, StorageType::S3);
}

TEST_F(AzureAbfssParsingTest, ParseStorageTypeFromStringS3a)
{
    auto storage_type = parseStorageTypeFromString("s3a");
    EXPECT_EQ(storage_type, StorageType::S3);
}

TEST_F(AzureAbfssParsingTest, TableMetadataSetLocationAzureAbfss)
{
    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/path/to/table");

    EXPECT_EQ(metadata.getStorageType(), StorageType::Azure);
    EXPECT_TRUE(metadata.hasLocation());
}

TEST_F(AzureAbfssParsingTest, TableMetadataSetLocationAzureAbfssGetLocation)
{
    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/path/to/table");

    std::string location = metadata.getLocation();
    EXPECT_EQ(location, "abfss://mycontainer@mystorageaccount.dfs.core.windows.net/path/to/table");
}

TEST_F(AzureAbfssParsingTest, TableMetadataSetLocationAzureAbfssWithEndpoint)
{
    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/path/to/table");
    metadata.setEndpoint("https://mystorageaccount.dfs.core.windows.net");

    std::string location = metadata.getLocation();
    EXPECT_EQ(location, "https://mystorageaccount.dfs.core.windows.net/mycontainer/path/to/table/");
}

TEST_F(AzureAbfssParsingTest, TableMetadataSetLocationS3)
{
    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation("s3://mybucket/path/to/table");

    EXPECT_EQ(metadata.getStorageType(), StorageType::S3);
    EXPECT_TRUE(metadata.hasLocation());

    std::string location = metadata.getLocation();
    EXPECT_EQ(location, "s3://mybucket/path/to/table");
}

TEST_F(AzureAbfssParsingTest, TableMetadataSetLocationInvalidFormat)
{
    TableMetadata metadata;
    metadata.withLocation();

    EXPECT_THROW({
        metadata.setLocation("invalid-location-without-protocol");
    }, DB::Exception);
}

TEST_F(AzureAbfssParsingTest, TableMetadataSetLocationMissingPath)
{
    TableMetadata metadata;
    metadata.withLocation();

    EXPECT_THROW({
        metadata.setLocation("abfss://container@account.dfs.core.windows.net");
    }, DB::Exception);
}

}
