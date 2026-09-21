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

    /// The very same file read twice keeps the very same token, which is what makes a retried
    /// chunk deduplicate. The size and the modification time play no part in it.
    EXPECT_EQ(
        ObjectStorageQueueSource::makeDeduplicationToken(makeMetadata("\"abc\"", 1024, 1700000000000000), "data/one.csv", 0),
        ObjectStorageQueueSource::makeDeduplicationToken(makeMetadata("\"abc\"", 2048, 1700000009000000), "data/one.csv", 0));
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
    EXPECT_ANY_THROW(ObjectStorageQueueSource::makeDeduplicationToken(not_fetched, "data/one.csv", 0));

    EXPECT_ANY_THROW(ObjectStorageQueueSource::makeDeduplicationToken(std::nullopt, "data/one.csv", 0));
}

/// A `(size, modification time)` pair is not a substitute for the tag either. Listings report the
/// modification time with a one-second resolution (`AzureObjectStorage::iterate` truncates it), so
/// two generations of one path that have the same length and are written within one second look
/// identical, and the rows of the newer one would be deduplicated away against the older one.
/// A storage that only derives such a weak tag itself (HDFS) is refused for the same reason.
TEST(ObjectStorageQueueDeduplicationToken, WeakGenerationSurrogateFailsClosed)
{
    /// Two generations of the same path, same size, same second. Neither may yield a token.
    EXPECT_ANY_THROW(ObjectStorageQueueSource::makeDeduplicationToken(makeMetadata("", 1024, 1700000000000000), "data/one.csv", 0));
    EXPECT_ANY_THROW(ObjectStorageQueueSource::makeDeduplicationToken(makeMetadata("", 1024, 1700000000000000), "data/one.csv", 0));

    /// The weak `(mtime, size)` token that HDFS reports as an `ETag` is refused as well.
    auto weak = makeMetadata("1700000000_1024", 1024, 1700000000000000);
    weak.etag_is_strong = false;
    EXPECT_ANY_THROW(ObjectStorageQueueSource::makeDeduplicationToken(weak, "data/one.csv", 0));

    /// The same tag, reported as strong, is accepted - it is the strength that decides.
    auto strong = makeMetadata("1700000000_1024", 1024, 1700000000000000);
    EXPECT_EQ(ObjectStorageQueueSource::makeDeduplicationToken(strong, "data/one.csv", 0), "1700000000_1024:0");
}

/// `hasStrongETag` is the predicate the source asks before it reaches for a token: a listing that
/// came back without a usable tag makes it refresh the metadata with a per-object request, so an
/// endpoint that omits `ETag` in the listing but reports it on `GetProperties` is not refused.
/// It has to agree with `makeDeduplicationToken` on what "usable" means, tag by tag.
TEST(ObjectStorageQueueDeduplicationToken, StrongETagIsWhatDecidesTheRefresh)
{
    EXPECT_TRUE(ObjectStorageQueueSource::hasStrongETag(makeMetadata("\"abc\"", 1024, 1700000000000000)));
    EXPECT_TRUE(ObjectStorageQueueSource::hasStrongETag(makeMetadata("abc", 1024, 1700000000000000)));

    /// No tag at all, and a tag of nothing but the quotes, both need the refresh.
    EXPECT_FALSE(ObjectStorageQueueSource::hasStrongETag(makeMetadata("", 1024, 1700000000000000)));
    EXPECT_FALSE(ObjectStorageQueueSource::hasStrongETag(makeMetadata("\"\"", 1024, 1700000000000000)));

    /// So does a present but weak one: it would be refused by the token as well.
    auto weak = makeMetadata("1700000000_1024", 1024, 1700000000000000);
    weak.etag_is_strong = false;
    EXPECT_FALSE(ObjectStorageQueueSource::hasStrongETag(weak));

    /// No metadata at all - the iterator left it out - needs the refresh too.
    EXPECT_FALSE(ObjectStorageQueueSource::hasStrongETag(std::nullopt));

    /// Exactly the tags a token can be built from, and no others.
    for (const auto & etag : {std::string{"\"abc\""}, std::string{"abc"}, std::string{""}, std::string{"\"\""}})
    {
        const auto metadata = makeMetadata(etag, 1024, 1700000000000000);
        bool token_built = true;
        try
        {
            ObjectStorageQueueSource::makeDeduplicationToken(metadata, "data/one.csv", 0);
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
