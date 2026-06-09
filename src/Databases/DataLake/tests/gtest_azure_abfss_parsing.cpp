#include <Databases/DataLake/ICatalog.h>
#include <gtest/gtest.h>
#include <Common/Exception.h>
#include <Core/SettingsEnums.h>
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

    std::string location = metadata.getLocationWithEndpoint("https://mystorageaccount.dfs.core.windows.net");
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

TEST_F(AzureAbfssParsingTest, TableMetadataGetLocationWithEndpointPathStyle)
{
    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation("s3://mybucket/path/to/table");

    std::string location = metadata.getLocationWithEndpoint("https://s3.mycompany.com", DB::S3UriStyle::PATH);
    EXPECT_EQ(location, "https://s3.mycompany.com/mybucket/path/to/table/");
}

TEST_F(AzureAbfssParsingTest, TableMetadataGetLocationWithEndpointVirtualHosted)
{
    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation("s3://mybucket/path/to/table");

    std::string location = metadata.getLocationWithEndpoint("https://s3.mycompany.com", DB::S3UriStyle::VIRTUAL_HOSTED);
    EXPECT_EQ(location, "https://mybucket.s3.mycompany.com/path/to/table/");
}

TEST_F(AzureAbfssParsingTest, TableMetadataGetLocationWithEndpointVirtualHostedWithPort)
{
    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation("s3://mybucket/path/to/table");

    std::string location = metadata.getLocationWithEndpoint("https://s3.mycompany.com:9000", DB::S3UriStyle::VIRTUAL_HOSTED);
    EXPECT_EQ(location, "https://mybucket.s3.mycompany.com:9000/path/to/table/");
}

TEST_F(AzureAbfssParsingTest, TableMetadataGetLocationWithEndpointVirtualHostedAlreadyEmbedded)
{
    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation("s3://mybucket/path/to/table");

    std::string location = metadata.getLocationWithEndpoint("https://mybucket.s3.mycompany.com", DB::S3UriStyle::VIRTUAL_HOSTED);
    EXPECT_EQ(location, "https://mybucket.s3.mycompany.com/path/to/table/");
}

TEST_F(AzureAbfssParsingTest, TableMetadataGetLocationWithEndpointAutoDefaultsToPathStyle)
{
    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation("s3://mybucket/path/to/table");

    std::string auto_location = metadata.getLocationWithEndpoint("https://s3.mycompany.com", DB::S3UriStyle::AUTO);
    std::string path_location = metadata.getLocationWithEndpoint("https://s3.mycompany.com", DB::S3UriStyle::PATH);
    EXPECT_EQ(auto_location, path_location);
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

TEST_F(AzureAbfssParsingTest, TableMetadataSetLocationNonPolarisContainerInPath)
{
    const std::string location = "abfss://c@account.dfs.core.windows.net/c/table";

    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation(location);

    EXPECT_EQ(metadata.getLocation(), location);
}

TEST_F(AzureAbfssParsingTest, TableMetadataGetMetadataLocationNonPolarisContainerInPath)
{
    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation("abfss://c@account.dfs.core.windows.net/c/table");

    const std::string metadata_file =
        "abfss://c@account.dfs.core.windows.net/c/table/metadata/v1.metadata.json";
    EXPECT_EQ(metadata.getMetadataLocation(metadata_file), "metadata/v1.metadata.json");
}

TEST_F(AzureAbfssParsingTest, TableMetadataGetMetadataLocationPolarisStyle)
{
    TableMetadata metadata;
    metadata.withLocation().withForceAddBucket();
    metadata.setLocation("abfss://mycontainer@mystorageaccount.dfs.core.windows.net/mycontainer/actual/path");

    const std::string metadata_file =
        "abfss://mycontainer@mystorageaccount.dfs.core.windows.net/mycontainer/actual/path/metadata/v1.metadata.json";
    EXPECT_EQ(metadata.getMetadataLocation(metadata_file), "metadata/v1.metadata.json");
}

TEST_F(AzureAbfssParsingTest, TableMetadataSetLocationPolarisStyle)
{
    const std::string location = "abfss://mycontainer@mystorageaccount.dfs.core.windows.net/mycontainer/actual/path";

    TableMetadata metadata;
    metadata.withLocation().withForceAddBucket();
    metadata.setLocation(location);

    /// `getLocation` without endpoint is always a round-trip regardless of the Polaris flag.
    EXPECT_EQ(metadata.getLocation(), location);
}

TEST_F(AzureAbfssParsingTest, TableMetadataGetMetadataLocationEqualStrings)
{
    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation("abfss://c@account.dfs.core.windows.net/c/table");

    const std::string metadata_file = "abfss://c@account.dfs.core.windows.net/c/table";
    EXPECT_EQ(metadata.getMetadataLocation(metadata_file), "");
}

TEST_F(AzureAbfssParsingTest, TableMetadataGetLocationWithEndpointPathStyleRejectedForVirtualHostedEndpoint)
{
    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation("s3://mybucket/path/to/table");

    EXPECT_THROW(
        metadata.getLocationWithEndpoint("https://mybucket.s3.mycompany.com", DB::S3UriStyle::PATH),
        DB::Exception);
}

TEST_F(AzureAbfssParsingTest, TableMetadataGetLocationWithEndpointVirtualHostedDottedBucketName)
{
    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation("s3://my.dotted.bucket/path/to/table");

    std::string location = metadata.getLocationWithEndpoint("https://s3.mycompany.com", DB::S3UriStyle::VIRTUAL_HOSTED);
    EXPECT_EQ(location, "https://my.dotted.bucket.s3.mycompany.com/path/to/table/");
}

TEST_F(AzureAbfssParsingTest, TableMetadataGetMetadataLocationS3NoEndpoint)
{
    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation("s3://bucket123/music/albums");

    const std::string metadata_file =
        "s3://bucket123/music/albums/metadata/00005-a4c1e01b-61ce-4131-9ea8-62591e3a9907.metadata.json";
    EXPECT_EQ(
        metadata.getMetadataLocation(metadata_file),
        "metadata/00005-a4c1e01b-61ce-4131-9ea8-62591e3a9907.metadata.json");
}

/// A REST catalog (e.g. Apache Polaris) can vend an `s3.endpoint` that is a bare host such as
/// `http://minio:9000`. The metadata file path must still be made relative to the table
/// location and must not be polluted by the endpoint. Before the fix this returned the full
/// `s3://bucket123/...` URI because the endpoint-rebuilt location no longer shared its prefix
/// with the metadata file URI.
TEST_F(AzureAbfssParsingTest, TableMetadataGetMetadataLocationS3VendedEndpointBareHost)
{
    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation("s3://bucket123/music/albums");
    metadata.setEndpoint("http://minio:9000");

    const std::string metadata_file =
        "s3://bucket123/music/albums/metadata/00005-a4c1e01b-61ce-4131-9ea8-62591e3a9907.metadata.json";
    EXPECT_EQ(
        metadata.getMetadataLocation(metadata_file),
        "metadata/00005-a4c1e01b-61ce-4131-9ea8-62591e3a9907.metadata.json");
}

/// The vended `s3.endpoint` may also already include the bucket, e.g.
/// `http://minio:9000/bucket123`. This is the shape the previous fix attempt could not handle:
/// stripping the endpoint left `music/albums` while the metadata URI still carried the leading
/// `bucket123/`, so the prefix comparison failed.
TEST_F(AzureAbfssParsingTest, TableMetadataGetMetadataLocationS3VendedEndpointIncludesBucket)
{
    TableMetadata metadata;
    metadata.withLocation();
    metadata.setLocation("s3://bucket123/music/albums");
    metadata.setEndpoint("http://minio:9000/bucket123");

    const std::string metadata_file =
        "s3://bucket123/music/albums/metadata/00005-a4c1e01b-61ce-4131-9ea8-62591e3a9907.metadata.json";
    EXPECT_EQ(
        metadata.getMetadataLocation(metadata_file),
        "metadata/00005-a4c1e01b-61ce-4131-9ea8-62591e3a9907.metadata.json");
}

}
