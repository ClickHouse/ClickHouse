#include "config.h"

#if USE_HDFS

#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/ObjectStorages/HDFS/HDFSObjectStorage.h>

using DB::HDFSObjectStorage;

/// `HDFSObjectStorage::makeObjectMetadata` builds the `ObjectMetadata` that the HDFS
/// `tryGetObjectMetadata` / `listObjects` paths return. A live NameNode cannot run in a
/// unit test, so this exercises the pure construction logic instead. The safety-critical
/// contract is that the synthesised etag, while populated (for the `_etag` virtual
/// column), is weak and therefore must never be usable as a content-cache key.

TEST(HDFSObjectStorageMetadata, EtagIsNeverUsableAsCacheKey)
{
    auto metadata = HDFSObjectStorage::makeObjectMetadata(/*last_modified=*/ 1700000000, /*size=*/ 42);
    EXPECT_FALSE(metadata.etag_is_strong);
    EXPECT_FALSE(metadata.isEtagUsableAsCacheKey());
}

TEST(HDFSObjectStorageMetadata, EtagIsPopulatedForTheVirtualColumn)
{
    /// The token must be non-empty so the `_etag` virtual column is populated on HDFS,
    /// even though it cannot key a cache.
    auto metadata = HDFSObjectStorage::makeObjectMetadata(/*last_modified=*/ 1700000000, /*size=*/ 42);
    EXPECT_FALSE(metadata.etag.empty());
}

TEST(HDFSObjectStorageMetadata, SizeAndModificationTimeArePreserved)
{
    auto metadata = HDFSObjectStorage::makeObjectMetadata(/*last_modified=*/ 1700000000, /*size=*/ 42);
    EXPECT_EQ(metadata.size_bytes, 42u);
    EXPECT_EQ(metadata.last_modified.epochTime(), 1700000000);
}

#endif
