#include <gtest/gtest.h>
#include <config.h>

#if USE_AWS_S3
#include <Storages/ObjectStorage/S3/Configuration.h>
#endif

#if USE_AZURE_BLOB_STORAGE
#include <Storages/ObjectStorage/Azure/Configuration.h>
#endif

/// The tests check that `getURIForAccessCheck` puts the key that is written into the URI the user typed,
/// so that filtered grants, which match URIs, are checked against the destination that is actually written.

#if USE_AWS_S3

namespace
{

String uriForAccessCheck(const String & raw_uri, const String & path)
{
    DB::StorageS3Configuration configuration;
    configuration.url = DB::S3::URI(raw_uri);
    return configuration.getURIForAccessCheck(path);
}

}

TEST(URIForAccessCheck, S3PathStyle)
{
    EXPECT_EQ(
        uriForAccessCheck("http://minio1:9001/root/data/{_partition_id}.tsv", "data/a.tsv"),
        "http://minio1:9001/root/data/a.tsv");
}

TEST(URIForAccessCheck, S3VirtualHostedStyle)
{
    EXPECT_EQ(
        uriForAccessCheck("https://bucket.s3.amazonaws.com/data/{_partition_id}.tsv", "data/a.tsv"),
        "https://bucket.s3.amazonaws.com/data/a.tsv");
}

/// The raw URI spells `{_partition_id}` percent-encoded, but the key that is written is decoded: the result is built
/// from the key, so a partition id with `..` cannot hide behind the encoded placeholder.
TEST(URIForAccessCheck, S3EncodedPlaceholder)
{
    EXPECT_EQ(
        uriForAccessCheck("http://minio1:9001/root/allowed/%7B_partition_id%7D.tsv", "allowed/../protected/x.tsv"),
        "http://minio1:9001/root/allowed/../protected/x.tsv");
}

/// A key renamed by `s3_create_new_file_on_insert` is checked as it is written.
TEST(URIForAccessCheck, S3RenamedKey)
{
    EXPECT_EQ(
        uriForAccessCheck("http://minio1:9001/root/team.v1/{_partition_id}.tsv", "team.1.v1/a.tsv"),
        "http://minio1:9001/root/team.1.v1/a.tsv");
}

/// Characters that are not allowed in a URI are encoded the same way as in the URI that the source access check uses.
TEST(URIForAccessCheck, S3KeyIsEncoded)
{
    EXPECT_EQ(
        uriForAccessCheck("http://minio1:9001/root/data/{_partition_id}.tsv", "data/a b?.tsv"),
        "http://minio1:9001/root/data/a%20b%3F.tsv");
}

#endif

#if USE_AZURE_BLOB_STORAGE

namespace
{

String azureURIForAccessCheck(const String & raw_path, const String & path)
{
    DB::StorageAzureConfiguration configuration;
    configuration.setRawPath(DB::StorageObjectStorageConfiguration::Path{raw_path});
    return configuration.getURIForAccessCheck(path);
}

}

/// For `azureBlobStorage` the raw URI is the blob path itself, so the result is the key.
TEST(URIForAccessCheck, AzureBlobPath)
{
    EXPECT_EQ(azureURIForAccessCheck("data/{_partition_id}.tsv", "data/a.tsv"), "data/a.tsv");
}

/// When the raw URI does not end with the raw path, the key cannot be placed in it: the result is empty,
/// and then only an unfiltered grant allows the write.
TEST(URIForAccessCheck, AzureMismatchIsEmpty)
{
    EXPECT_EQ(azureURIForAccessCheck("data/%41/{_partition_id}.tsv", "data/%41/a.tsv"), "");
}

#endif
