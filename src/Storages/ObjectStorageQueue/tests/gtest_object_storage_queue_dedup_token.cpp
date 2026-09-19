#include <gtest/gtest.h>

#include <Storages/ObjectStorageQueue/ObjectStorageQueueSource.h>

using namespace DB;

/// The usual case: the endpoint reports a (quoted) `ETag`, so the token identifies the chunk by the
/// file it came from and its offset in it.
TEST(ObjectStorageQueueDeduplicationToken, ETagAndOffsetIdentifyTheChunk)
{
    EXPECT_EQ(ObjectStorageQueueSource::makeDeduplicationToken("\"abc\"", 0), "abc:0");
    EXPECT_EQ(ObjectStorageQueueSource::makeDeduplicationToken("\"abc\"", 100), "abc:100");

    /// An unquoted tag is taken as it is.
    EXPECT_EQ(ObjectStorageQueueSource::makeDeduplicationToken("abc", 0), "abc:0");

    /// Two different files never collide, at any offset.
    EXPECT_NE(
        ObjectStorageQueueSource::makeDeduplicationToken("\"abc\"", 100),
        ObjectStorageQueueSource::makeDeduplicationToken("\"def\"", 100));
}

/// `ETag` is an optional response header. A token built without it would be `:<row offset>` for
/// every file, so distinct files would deduplicate against each other and their rows would disappear
/// from the dependent materialized views. The token must be empty instead, which is the value that
/// makes `DeduplicationInfo` deduplicate the chunk by the hash of its data.
TEST(ObjectStorageQueueDeduplicationToken, NoETagYieldsNoToken)
{
    EXPECT_EQ(ObjectStorageQueueSource::makeDeduplicationToken("", 0), "");
    EXPECT_EQ(ObjectStorageQueueSource::makeDeduplicationToken("", 100), "");

    /// A tag that is nothing but the quotes is just as absent.
    EXPECT_EQ(ObjectStorageQueueSource::makeDeduplicationToken("\"\"", 0), "");
}
