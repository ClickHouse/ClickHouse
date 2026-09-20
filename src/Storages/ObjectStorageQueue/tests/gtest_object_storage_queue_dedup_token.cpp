#include <gtest/gtest.h>

#include <Storages/ObjectStorageQueue/ObjectStorageQueueSource.h>

using namespace DB;

/// The usual case: the endpoint reports a (quoted) `ETag`, so the token identifies the chunk by the
/// file it came from and its offset in it.
TEST(ObjectStorageQueueDeduplicationToken, ETagAndOffsetIdentifyTheChunk)
{
    EXPECT_EQ(ObjectStorageQueueSource::makeDeduplicationToken("\"abc\"", "data/one.csv", 0), "abc:0");
    EXPECT_EQ(ObjectStorageQueueSource::makeDeduplicationToken("\"abc\"", "data/one.csv", 100), "abc:100");

    /// An unquoted tag is taken as it is.
    EXPECT_EQ(ObjectStorageQueueSource::makeDeduplicationToken("abc", "data/one.csv", 0), "abc:0");

    /// Two different files never collide, at any offset.
    EXPECT_NE(
        ObjectStorageQueueSource::makeDeduplicationToken("\"abc\"", "data/one.csv", 100),
        ObjectStorageQueueSource::makeDeduplicationToken("\"def\"", "data/two.csv", 100));
}

/// `ETag` is an optional response header. A token built without it would be `:<row offset>` for
/// every file, and an empty token would send the chunk down the data-hash path of
/// `DeduplicationInfo` - either way two distinct files would deduplicate against each other and one
/// file's rows would disappear from the dependent materialized views. The path of the object takes
/// the place of the tag instead.
TEST(ObjectStorageQueueDeduplicationToken, NoETagFallsBackToThePath)
{
    EXPECT_EQ(ObjectStorageQueueSource::makeDeduplicationToken("", "data/one.csv", 0), "data/one.csv:0");
    EXPECT_EQ(ObjectStorageQueueSource::makeDeduplicationToken("", "data/one.csv", 100), "data/one.csv:100");

    /// A tag that is nothing but the quotes is just as absent.
    EXPECT_EQ(ObjectStorageQueueSource::makeDeduplicationToken("\"\"", "data/one.csv", 0), "data/one.csv:0");

    /// The token is never empty, because an empty user token means "deduplicate by the data".
    EXPECT_FALSE(ObjectStorageQueueSource::makeDeduplicationToken("", "data/one.csv", 0).empty());
}

/// Two different files whose chunks hold exactly the same data. Without a per-file identifier they
/// would produce the same block hash and one of them would be dropped.
TEST(ObjectStorageQueueDeduplicationToken, EqualChunksOfDifferentFilesDoNotCollideWithoutETag)
{
    EXPECT_NE(
        ObjectStorageQueueSource::makeDeduplicationToken("", "data/one.csv", 0),
        ObjectStorageQueueSource::makeDeduplicationToken("", "data/two.csv", 0));
    EXPECT_NE(
        ObjectStorageQueueSource::makeDeduplicationToken("", "data/one.csv", 100),
        ObjectStorageQueueSource::makeDeduplicationToken("", "data/two.csv", 100));

    /// The same file at two offsets stays distinct as well.
    EXPECT_NE(
        ObjectStorageQueueSource::makeDeduplicationToken("", "data/one.csv", 0),
        ObjectStorageQueueSource::makeDeduplicationToken("", "data/one.csv", 100));
}
