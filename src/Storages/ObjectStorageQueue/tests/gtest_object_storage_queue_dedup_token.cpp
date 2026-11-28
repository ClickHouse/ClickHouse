#include <gtest/gtest.h>

#include <Storages/ObjectStorageQueue/ObjectStorageQueueSource.h>

namespace DB::ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
}

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

constexpr UInt64 LEGACY_VERSION = ObjectStorageQueueTableMetadata::DEDUPLICATION_TOKEN_VERSION_ETAG;
constexpr UInt64 CURRENT_VERSION = ObjectStorageQueueTableMetadata::DEDUPLICATION_TOKEN_VERSION_PATH_AND_ETAG;

std::string legacyToken(const std::string & etag, const std::string & path, size_t row_offset)
{
    return ObjectStorageQueueSource::makeDeduplicationToken(makeMetadata(etag, 1024, 1700000000000000), path, row_offset, LEGACY_VERSION);
}

/// The metadata of a queue as a server that did not record the scheme of the token wrote it to Keeper.
std::string metadataWrittenBeforeTheTokenWasVersioned()
{
    return R"({"after_processing":"keep","mode":"unordered","tracked_files_limit":0,"tracked_files_ttl_sec":0,)"
        R"("processing_threads_num":1,"buckets":0,"format_name":"CSV","columns":"columns format version: 1\n0 columns:\n",)"
        R"("last_processed_file":"","loading_retries":10})";
}

std::string token(const std::string & etag, const std::string & path, size_t row_offset)
{
    return ObjectStorageQueueSource::makeDeduplicationToken(makeMetadata(etag, 1024, 1700000000000000), path, row_offset, CURRENT_VERSION);
}

}

/// The usual case: the endpoint reports a (quoted) `ETag`, so the token identifies the chunk by the
/// file it came from and its offset in it.
TEST(ObjectStorageQueueDeduplicationToken, PathETagAndOffsetIdentifyTheChunk)
{
    EXPECT_EQ(token("\"abc\"", "data/one.csv", 0), "12:data/one.csv:abc:0");
    EXPECT_EQ(token("\"abc\"", "data/one.csv", 100), "12:data/one.csv:abc:100");

    /// An unquoted tag is taken as it is.
    EXPECT_EQ(token("abc", "data/one.csv", 0), "12:data/one.csv:abc:0");

    /// Two different files never collide, at any offset.
    EXPECT_NE(token("\"abc\"", "data/one.csv", 100), token("\"def\"", "data/two.csv", 100));

    /// An `ETag` identifies content, not an object: two files with identical bytes (for a single-part
    /// S3 upload the tag is the MD5 of the bytes) have the same tag. They are still different files.
    EXPECT_NE(token("\"abc\"", "data/one.csv", 0), token("\"abc\"", "data/two.csv", 0));
    EXPECT_NE(token("\"abc\"", "data/one.csv", 100), token("\"abc\"", "data/two.csv", 100));

    /// The parts cannot be confused with each other, whatever characters they hold.
    EXPECT_NE(token("b:c", "a", 0), token("c", "a:b", 0));
    EXPECT_NE(token("b:1", "a", 2), token("b", "a", 12));
    EXPECT_NE(token("x", "1:a", 0), token("a:x", "1", 0));

    /// The very same file read twice keeps the very same token, which is what makes a retried
    /// chunk deduplicate. The size and the modification time play no part in it.
    EXPECT_EQ(
        ObjectStorageQueueSource::makeDeduplicationToken(makeMetadata("\"abc\"", 1024, 1700000000000000), "data/one.csv", 0, CURRENT_VERSION),
        ObjectStorageQueueSource::makeDeduplicationToken(makeMetadata("\"abc\"", 2048, 1700000009000000), "data/one.csv", 0, CURRENT_VERSION));
    EXPECT_EQ(token("\"abc\"", "data/one.csv", 100), token("\"abc\"", "data/one.csv", 100));
}

/// The token is the identity of a chunk in the deduplication logs of the dependent targets. A server
/// before the path was added built it as `<etag>:<offset>`. Such a server may have inserted a part of
/// a file and aborted the batch on shutdown, which resets the file to be replayed from offset `0` - by
/// the upgraded server, or by another replica in the middle of a rolling upgrade. The replay has to
/// produce the very tokens of the first pass, or the rows already inserted are inserted again.
TEST(ObjectStorageQueueDeduplicationToken, QueueCreatedBeforeThePathWasAddedKeepsItsTokens)
{
    /// Exactly what the previous servers produced, quotes removed.
    EXPECT_EQ(legacyToken("\"abc\"", "data/one.csv", 0), "abc:0");
    EXPECT_EQ(legacyToken("\"abc\"", "data/one.csv", 100), "abc:100");
    EXPECT_EQ(legacyToken("abc", "data/one.csv", 100), "abc:100");

    /// The metadata such a queue has in Keeper does not name a scheme, and that means the first one:
    /// the file replayed after the upgrade gets the tokens the old server gave its chunks.
    const auto old_queue = ObjectStorageQueueTableMetadata::parse(metadataWrittenBeforeTheTokenWasVersioned());
    EXPECT_EQ(old_queue.deduplication_token_version, LEGACY_VERSION);
    EXPECT_EQ(
        ObjectStorageQueueSource::makeDeduplicationToken(
            makeMetadata("\"abc\"", 1024, 1700000000000000), "data/one.csv", 100, old_queue.deduplication_token_version),
        "abc:100");

    /// The scheme survives every rewrite of the metadata (`ALTER TABLE ... MODIFY SETTING`), and a table
    /// that attaches to the queue takes it over instead of the one it would use for a new queue.
    const auto rewritten = ObjectStorageQueueTableMetadata::parse(old_queue.toString());
    EXPECT_EQ(rewritten.deduplication_token_version, LEGACY_VERSION);

    auto attached = ObjectStorageQueueTableMetadata::parse(old_queue.toString());
    attached.deduplication_token_version = CURRENT_VERSION;
    attached.adjustFromKeeper(old_queue);
    EXPECT_EQ(attached.deduplication_token_version, LEGACY_VERSION);

    /// A queue that records the scheme with the path keeps it the same way.
    attached.deduplication_token_version = CURRENT_VERSION;
    const auto new_queue = ObjectStorageQueueTableMetadata::parse(attached.toString());
    EXPECT_EQ(new_queue.deduplication_token_version, CURRENT_VERSION);

    /// The old scheme requires a strong `ETag` just as well.
    EXPECT_ANY_THROW(legacyToken("", "data/one.csv", 0));

    /// A scheme this server does not know is refused instead of being replaced with another one.
    EXPECT_ANY_THROW(ObjectStorageQueueSource::makeDeduplicationToken(
        makeMetadata("\"abc\"", 1024, 1700000000000000), "data/one.csv", 0, CURRENT_VERSION + 1));
}

/// `ETag` is an optional response header, and without it nothing identifies a chunk exactly. Every
/// surrogate is a wrong-results path: a token of the row offset alone is `:0` for the first chunk
/// of every file, an empty token sends the chunk down the data-hash path of `DeduplicationInfo` so
/// two files holding an identical chunk collapse into one, and the path alone does not survive the
/// re-import that `tracked_file_ttl_sec` and `tracked_files_limit` allow. The read fails instead.
TEST(ObjectStorageQueueDeduplicationToken, NoETagFailsClosed)
{
    /// The size and the modification time are known here - they are still not a token.
    EXPECT_ANY_THROW(token("", "data/one.csv", 0));
    EXPECT_ANY_THROW(token("", "data/one.csv", 100));

    /// A tag that is nothing but the quotes is just as absent.
    EXPECT_ANY_THROW(token("\"\"", "data/one.csv", 0));

    /// The placeholder that `skip_object_metadata` leaves behind carries no real values at all.
    auto not_fetched = makeMetadata("", 0, 0);
    not_fetched.is_fetched = false;
    EXPECT_ANY_THROW(ObjectStorageQueueSource::makeDeduplicationToken(not_fetched, "data/one.csv", 0, CURRENT_VERSION));

    EXPECT_ANY_THROW(ObjectStorageQueueSource::makeDeduplicationToken(std::nullopt, "data/one.csv", 0, CURRENT_VERSION));
}

/// A `(size, modification time)` pair is not a substitute for the tag either. Listings report the
/// modification time with a one-second resolution (`AzureObjectStorage::iterate` truncates it), so
/// two generations of one path that have the same length and are written within one second look
/// identical, and the rows of the newer one would be deduplicated away against the older one.
/// A storage that only derives such a weak tag itself (HDFS) is refused for the same reason.
TEST(ObjectStorageQueueDeduplicationToken, WeakGenerationSurrogateFailsClosed)
{
    /// Two generations of the same path, same size, same second. Neither may yield a token.
    EXPECT_ANY_THROW(ObjectStorageQueueSource::makeDeduplicationToken(makeMetadata("", 1024, 1700000000000000), "data/one.csv", 0, CURRENT_VERSION));
    EXPECT_ANY_THROW(ObjectStorageQueueSource::makeDeduplicationToken(makeMetadata("", 1024, 1700000000000000), "data/one.csv", 0, CURRENT_VERSION));

    /// The weak `(mtime, size)` token that HDFS reports as an `ETag` is refused as well.
    auto weak = makeMetadata("1700000000_1024", 1024, 1700000000000000);
    weak.etag_is_strong = false;
    EXPECT_ANY_THROW(ObjectStorageQueueSource::makeDeduplicationToken(weak, "data/one.csv", 0, CURRENT_VERSION));

    /// The same tag, reported as strong, is accepted - it is the strength that decides.
    auto strong = makeMetadata("1700000000_1024", 1024, 1700000000000000);
    EXPECT_EQ(ObjectStorageQueueSource::makeDeduplicationToken(strong, "data/one.csv", 0, CURRENT_VERSION), "12:data/one.csv:1700000000_1024:0");
}

/// `hasStrongETag` is the predicate the source asks before the first pull of a file: a file whose
/// metadata has no usable tag fails as a read error instead of being read without a token.
/// It has to agree with `makeDeduplicationToken` on what "usable" means, tag by tag.
TEST(ObjectStorageQueueDeduplicationToken, StrongETagIsWhatDecidesTheReadCheck)
{
    EXPECT_TRUE(ObjectStorageQueueSource::hasStrongETag(makeMetadata("\"abc\"", 1024, 1700000000000000)));
    EXPECT_TRUE(ObjectStorageQueueSource::hasStrongETag(makeMetadata("abc", 1024, 1700000000000000)));

    /// No tag at all, and a tag of nothing but the quotes, both fail the check.
    EXPECT_FALSE(ObjectStorageQueueSource::hasStrongETag(makeMetadata("", 1024, 1700000000000000)));
    EXPECT_FALSE(ObjectStorageQueueSource::hasStrongETag(makeMetadata("\"\"", 1024, 1700000000000000)));

    /// So does a present but weak one: it would be refused by the token as well.
    auto weak = makeMetadata("1700000000_1024", 1024, 1700000000000000);
    weak.etag_is_strong = false;
    EXPECT_FALSE(ObjectStorageQueueSource::hasStrongETag(weak));

    /// No metadata at all - the iterator left it out - fails the check too.
    EXPECT_FALSE(ObjectStorageQueueSource::hasStrongETag(std::nullopt));

    /// Exactly the tags a token can be built from, and no others.
    for (const auto & etag : {std::string{"\"abc\""}, std::string{"abc"}, std::string{""}, std::string{"\"\""}})
    {
        const auto metadata = makeMetadata(etag, 1024, 1700000000000000);
        bool token_built = true;
        try
        {
            ObjectStorageQueueSource::makeDeduplicationToken(metadata, "data/one.csv", 0, CURRENT_VERSION);
        }
        catch (const Exception & e)
        {
            /// The refusal is the one `UNSUPPORTED_METHOD` the source reports for an unidentifiable file.
            EXPECT_EQ(e.code(), ErrorCodes::UNSUPPORTED_METHOD) << "etag: " << etag;
            token_built = false;
        }
        EXPECT_EQ(ObjectStorageQueueSource::hasStrongETag(metadata), token_built) << "etag: " << etag;
    }
}
