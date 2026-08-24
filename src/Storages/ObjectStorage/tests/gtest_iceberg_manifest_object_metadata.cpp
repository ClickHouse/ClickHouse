#include "config.h"

#if USE_AVRO

#include <gtest/gtest.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergDataObjectInfo.h>

using namespace DB;

namespace
{

/// Mirrors how a cluster function worker builds the object: the path-only constructor, with `info`
/// assigned afterwards. Deriving the metadata lazily is what makes it work on that path too.
IcebergDataObjectInfo makeObject(const String & path, std::optional<Int64> file_size_in_bytes)
{
    IcebergDataObjectInfo object(RelativePathWithMetadata{path});
    object.info.file_size_in_bytes = file_size_in_bytes;
    return object;
}

}

TEST(IcebergManifestObjectMetadata, UsesManifestRecordedSize)
{
    const auto object = makeObject("db/table/data/file.parquet", 4096);

    const auto metadata = object.tryGetObjectMetadataWithoutRequest();
    ASSERT_TRUE(metadata.has_value());
    EXPECT_EQ(metadata->size_bytes, 4096u);
    EXPECT_TRUE(metadata->is_size_known);
}

TEST(IcebergManifestObjectMetadata, ContentIsIdentifiedSoContentCachesStayEnabled)
{
    /// The Parquet metadata cache, the filesystem cache and the page cache all disable themselves
    /// for an object whose contents cannot be identified.
    const auto object = makeObject("db/table/data/file.parquet", 4096);

    const auto metadata = object.tryGetObjectMetadataWithoutRequest();
    ASSERT_TRUE(metadata.has_value());
    EXPECT_TRUE(metadata->getContentCacheToken().has_value());
}

TEST(IcebergManifestObjectMetadata, ThePathAloneKeysTheContentCaches)
{
    /// An empty token means the caches key on the path alone, which the spec allows because a data
    /// file is immutable.
    const auto object = makeObject("db/table/data/file.parquet", 4096);

    const auto metadata = object.tryGetObjectMetadataWithoutRequest();
    ASSERT_TRUE(metadata.has_value());
    EXPECT_TRUE(metadata->contents_identified_by_path);
    EXPECT_EQ(metadata->getContentCacheToken(), "");
}

TEST(IcebergManifestObjectMetadata, CarriesNoEtagAndIsNotThePlaceholder)
{
    /// No ETag, because the store returned nothing: that keeps this out of the S3 read-time
    /// validation, where it would be sent as `If-Match` and match nothing. `is_fetched` stays true
    /// because this metadata is complete, unlike the placeholder the read path fills in.
    const auto object = makeObject("db/table/data/file.parquet", 4096);

    const auto metadata = object.tryGetObjectMetadataWithoutRequest();
    ASSERT_TRUE(metadata.has_value());
    EXPECT_TRUE(metadata->etag.empty());
    EXPECT_FALSE(metadata->isEtagUsableAsCacheKey());
    EXPECT_TRUE(metadata->is_fetched);
}

TEST(IcebergManifestObjectMetadata, ModificationTimeIsReportedAsUnknown)
{
    /// The manifest records none, and a default `last_modified` presented as known would look older
    /// than any cached entry to the schema and count caches, which would then reuse a stale value.
    const auto object = makeObject("db/table/data/file.parquet", 4096);

    const auto metadata = object.tryGetObjectMetadataWithoutRequest();
    ASSERT_TRUE(metadata.has_value());
    EXPECT_FALSE(metadata->is_last_modified_known);
}

TEST(IcebergManifestObjectMetadata, FallsBackWhenTheManifestRecordedNoSize)
{
    const auto object = makeObject("db/table/data/file.parquet", std::nullopt);

    /// No value means the caller asks the object store, exactly as before this change.
    EXPECT_FALSE(object.tryGetObjectMetadataWithoutRequest().has_value());
}

TEST(IcebergManifestObjectMetadata, FallsBackOnANegativeSize)
{
    /// `file_size_in_bytes` is signed in the Avro schema, so a malformed manifest can carry a
    /// negative value. It must not be cast into a huge unsigned size.
    const auto object = makeObject("db/table/data/file.parquet", -1);

    EXPECT_FALSE(object.tryGetObjectMetadataWithoutRequest().has_value());
}

TEST(IcebergManifestObjectMetadata, AZeroByteFileIsStillAnswered)
{
    /// Zero is a legitimate size, distinct from "not recorded": answering it keeps
    /// `skip_empty_files` working without a request.
    const auto object = makeObject("db/table/data/empty.parquet", 0);

    const auto metadata = object.tryGetObjectMetadataWithoutRequest();
    ASSERT_TRUE(metadata.has_value());
    EXPECT_EQ(metadata->size_bytes, 0u);
    EXPECT_TRUE(metadata->is_size_known);
}

TEST(IcebergManifestObjectMetadata, PlainObjectInfoKnowsNothingWithoutARequest)
{
    /// The default stays empty so every storage other than Iceberg keeps asking the object store.
    const ObjectInfo object(RelativePathWithMetadata{"bucket/key.parquet"});

    EXPECT_FALSE(object.tryGetObjectMetadataWithoutRequest().has_value());
}

#endif
