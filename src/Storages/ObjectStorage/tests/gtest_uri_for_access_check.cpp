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

String uriForAccessCheck(const String & granted_uri, const String & write_path, const String & schema_hash = "")
{
    DB::StorageS3Configuration configuration;
    configuration.url = DB::S3::URI(granted_uri);
    if (!schema_hash.empty())
    {
        configuration.setPaths({configuration.getRawPath()});
        configuration.setSchemaHash(schema_hash);
    }
    return configuration.getURIForAccessCheck(write_path);
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

/// `url_scheme_mappers` can map `s3://root/data/...` to `http://minio1:9001/root/prefix/data/...`: the key then has a prefix
/// that the URI as the user typed it does not have, and the check uses the typed form.
namespace
{

String mappedURIForAccessCheck(const String & mapped_uri, const String & granted_uri, const String & write_path)
{
    DB::StorageS3Configuration configuration;
    configuration.url = DB::S3::URI(mapped_uri);
    configuration.url.uri_str = granted_uri;
    return configuration.getURIForAccessCheck(write_path);
}

}

TEST(URIForAccessCheck, S3SchemeMapperWithKeyPrefix)
{
    EXPECT_EQ(
        mappedURIForAccessCheck(
            "http://minio1:9001/root/prefix/data/{_partition_id}.tsv", "s3://root/data/{_partition_id}.tsv", "prefix/data/a.tsv"),
        "s3://root/data/a.tsv");
}

/// A write path outside the prefix of the key cannot be placed in the typed URI, so only an unfiltered grant allows it.
TEST(URIForAccessCheck, S3SchemeMapperOutsideKeyPrefixIsEmpty)
{
    EXPECT_EQ(
        mappedURIForAccessCheck(
            "http://minio1:9001/root/prefix/data/{_partition_id}.tsv", "s3://root/data/{_partition_id}.tsv", "other/data/a.tsv"),
        "");
}

/// A `%` in the key is a literal character: `%61` must not be checked as `a`, which is a different object.
TEST(URIForAccessCheck, S3LiteralPercentEscape)
{
    EXPECT_EQ(
        uriForAccessCheck("http://minio1:9001/root/allowed/{_partition_id}.tsv", "allowed/%61.tsv"),
        "http://minio1:9001/root/allowed/%2561.tsv");
}

/// A `%` that does not start an escape is a valid key too.
TEST(URIForAccessCheck, S3LonePercent)
{
    EXPECT_EQ(
        uriForAccessCheck("http://minio1:9001/root/allowed/{_partition_id}.tsv", "allowed/100%.tsv"),
        "http://minio1:9001/root/allowed/100%25.tsv");
}

/// Encoded dot segments in the key are literal characters, so they stay encoded and are not resolved as `..`.
TEST(URIForAccessCheck, S3EncodedDotSegments)
{
    EXPECT_EQ(
        uriForAccessCheck("http://minio1:9001/root/allowed/{_partition_id}.tsv", "allowed/%2E%2E/x.tsv"),
        "http://minio1:9001/root/allowed/%252E%252E/x.tsv");
}

/// `{_schema_hash}` is expanded in the key but not in the raw URI, which must not prevent building the URI.
TEST(URIForAccessCheck, S3SchemaHash)
{
    const String schema_hash = "0123456789abcdef0123456789abcdef";
    EXPECT_EQ(
        uriForAccessCheck(
            "http://minio1:9001/root/allowed/{_schema_hash}/{_partition_id}.tsv",
            "allowed/" + schema_hash + "/a.tsv",
            schema_hash),
        "http://minio1:9001/root/allowed/" + schema_hash + "/a.tsv");
}

#endif

#if USE_AZURE_BLOB_STORAGE

namespace
{

String azureURIForAccessCheck(const String & granted_path, const String & write_path)
{
    DB::StorageAzureConfiguration configuration;
    configuration.setRawPath(DB::StorageObjectStorageConfiguration::Path{granted_path});
    return configuration.getURIForAccessCheck(write_path);
}

}

/// For `azureBlobStorage` the raw URI is the blob path itself, so the result is the key.
TEST(URIForAccessCheck, AzureBlobPath)
{
    EXPECT_EQ(azureURIForAccessCheck("data/{_partition_id}.tsv", "data/a.tsv"), "data/a.tsv");
}

/// The blob path is not parsed as a URI, so escapes in it are kept as literal characters.
TEST(URIForAccessCheck, AzurePercentEscapes)
{
    EXPECT_EQ(azureURIForAccessCheck("data/%41/{_partition_id}.tsv", "data/%41/a.tsv"), "data/%41/a.tsv");
}

#endif
