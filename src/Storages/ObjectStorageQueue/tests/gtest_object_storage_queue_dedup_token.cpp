#include <gtest/gtest.h>

#include <Storages/ObjectStorageQueue/ObjectStorageQueueSource.h>

using namespace DB;

namespace
{

/// The metadata an object storage reports for a file: its tag, its size and when it was last
/// written. A test that leaves the tag out still gets the other two, exactly as a real endpoint
/// that only omits the optional `ETag` response header does.
ObjectMetadata makeMetadata(const std::string & etag, uint64_t size_bytes, Poco::Timestamp::TimeVal last_modified)
{
    ObjectMetadata metadata;
    metadata.etag = etag;
    metadata.size_bytes = size_bytes;
    metadata.last_modified = Poco::Timestamp(last_modified);
    return metadata;
}

std::string token(const std::string & etag, const std::string & path, size_t row_offset)
{
    return ObjectStorageQueueSource::makeDeduplicationToken(makeMetadata(etag, 1024, 1700000000000000), path, row_offset);
}

}

/// The usual case: the endpoint reports a (quoted) `ETag`, so the token identifies the chunk by the
/// file it came from and its offset in it.
TEST(ObjectStorageQueueDeduplicationToken, ETagAndOffsetIdentifyTheChunk)
{
    EXPECT_EQ(token("\"abc\"", "data/one.csv", 0), "abc:0");
    EXPECT_EQ(token("\"abc\"", "data/one.csv", 100), "abc:100");

    /// An unquoted tag is taken as it is.
    EXPECT_EQ(token("abc", "data/one.csv", 0), "abc:0");

    /// Two different files never collide, at any offset.
    EXPECT_NE(token("\"abc\"", "data/one.csv", 100), token("\"def\"", "data/two.csv", 100));
}

/// `ETag` is an optional response header. A token built without it would be `:<row offset>` for
/// every file, and an empty token would send the chunk down the data-hash path of
/// `DeduplicationInfo` - either way two distinct files would deduplicate against each other and one
/// file's rows would disappear from the dependent materialized views. The path of the object and
/// the generation of the blob behind it take the place of the tag instead.
TEST(ObjectStorageQueueDeduplicationToken, NoETagFallsBackToThePathAndTheGeneration)
{
    EXPECT_EQ(token("", "data/one.csv", 0), "data/one.csv:1024:1700000000000000:0");
    EXPECT_EQ(token("", "data/one.csv", 100), "data/one.csv:1024:1700000000000000:100");

    /// A tag that is nothing but the quotes is just as absent.
    EXPECT_EQ(token("\"\"", "data/one.csv", 0), token("", "data/one.csv", 0));

    /// The token is never empty, because an empty user token means "deduplicate by the data".
    EXPECT_FALSE(token("", "data/one.csv", 0).empty());
}

/// Two different files whose chunks hold exactly the same data. Without a per-file identifier they
/// would produce the same block hash and one of them would be dropped.
TEST(ObjectStorageQueueDeduplicationToken, EqualChunksOfDifferentFilesDoNotCollideWithoutETag)
{
    EXPECT_NE(token("", "data/one.csv", 0), token("", "data/two.csv", 0));
    EXPECT_NE(token("", "data/one.csv", 100), token("", "data/two.csv", 100));

    /// The same file at two offsets stays distinct as well.
    EXPECT_NE(token("", "data/one.csv", 0), token("", "data/one.csv", 100));
}

/// `tracked_file_ttl_sec` and `tracked_files_limit` let the queue re-import a path it has already
/// processed, and by then the path can hold a different blob. Keyed on the path alone the rows of
/// the second generation would be deduplicated away against the first.
TEST(ObjectStorageQueueDeduplicationToken, GenerationsOfTheSamePathDoNotCollideWithoutETag)
{
    const std::string path = "data/one.csv";

    const auto first = ObjectStorageQueueSource::makeDeduplicationToken(makeMetadata("", 1024, 1700000000000000), path, 0);

    /// A rewrite with different contents of the same length is told apart by the modification time.
    const auto rewritten = ObjectStorageQueueSource::makeDeduplicationToken(makeMetadata("", 1024, 1700000001000000), path, 0);
    EXPECT_NE(first, rewritten);

    /// A rewrite within the same timestamp is told apart by the size.
    const auto grown = ObjectStorageQueueSource::makeDeduplicationToken(makeMetadata("", 2048, 1700000000000000), path, 0);
    EXPECT_NE(first, grown);

    /// The very same generation read twice keeps the very same token, which is what makes a
    /// retried chunk deduplicate.
    const auto again = ObjectStorageQueueSource::makeDeduplicationToken(makeMetadata("", 1024, 1700000000000000), path, 0);
    EXPECT_EQ(first, again);
}

/// When the storage reports neither a tag nor a generation there is no way to build a token that
/// tells two files apart, so the read fails instead of silently dropping rows.
TEST(ObjectStorageQueueDeduplicationToken, NoETagAndNoGenerationFailsClosed)
{
    auto no_size = makeMetadata("", 1024, 1700000000000000);
    no_size.is_size_known = false;
    EXPECT_ANY_THROW(ObjectStorageQueueSource::makeDeduplicationToken(no_size, "data/one.csv", 0));

    auto no_time = makeMetadata("", 1024, 1700000000000000);
    no_time.is_last_modified_known = false;
    EXPECT_ANY_THROW(ObjectStorageQueueSource::makeDeduplicationToken(no_time, "data/one.csv", 0));

    /// The placeholder that `skip_object_metadata` leaves behind carries no real values at all.
    auto not_fetched = makeMetadata("", 0, 0);
    not_fetched.is_fetched = false;
    EXPECT_ANY_THROW(ObjectStorageQueueSource::makeDeduplicationToken(not_fetched, "data/one.csv", 0));

    EXPECT_ANY_THROW(ObjectStorageQueueSource::makeDeduplicationToken(std::nullopt, "data/one.csv", 0));

    /// A tag makes all of that irrelevant.
    auto tagged = makeMetadata("\"abc\"", 0, 0);
    tagged.is_fetched = false;
    EXPECT_EQ(ObjectStorageQueueSource::makeDeduplicationToken(tagged, "data/one.csv", 0), "abc:0");
}
