#include <gtest/gtest.h>

#include <IO/ConcatReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>

using namespace DB;

namespace
{

/// Wraps `payload` the way `PostingsSerialization::serialize` writes an uncompressed posting list:
/// [VarUInt: number of bytes][portable serialization of a roaring bitmap].
String encodePostingsPayload(const String & payload)
{
    WriteBufferFromOwnString out;
    writeVarUInt(payload.size(), out);
    out.write(payload.data(), payload.size());
    return out.str();
}

String serializePostings(const PostingList & postings)
{
    String payload(postings.getSizeInBytes(), '\0');
    postings.write(payload.data());
    return payload;
}

PostingsSerialization makeSerialization()
{
    /// A posting list without the `IsCompressed` flag never consults the codec.
    return PostingsSerialization(std::make_unique<PostingListCodecNone>(), MergeTreeTextIndexSerializationVersion::V2_WithPositions);
}

/// The whole payload is in the buffer, so `deserialize` deserializes it in place.
PostingList decodePostingsInPlace(const String & encoded)
{
    auto serialization = makeSerialization();
    ReadBufferFromString in(encoded);
    return *serialization.deserialize(in, /*header=*/ 0, /*cardinality=*/ 0);
}

/// The payload spans two buffers, so `deserialize` copies it out before deserializing.
PostingList decodePostingsAcrossBuffers(const String & encoded)
{
    auto serialization = makeSerialization();
    size_t split_at = encoded.size() / 2;
    ReadBufferFromMemory head(encoded.data(), split_at);
    ReadBufferFromMemory tail(encoded.data() + split_at, encoded.size() - split_at);
    ConcatReadBuffer in(head, tail);
    return *serialization.deserialize(in, /*header=*/ 0, /*cardinality=*/ 0);
}

}

/// Bounding the deserialization must not reject posting lists that the index writer produces.
TEST(TextIndexPostingsDeserializationTest, RoundTrip)
{
    PostingList empty;

    PostingList array_container;
    for (uint32_t row_id = 0; row_id < 100; ++row_id)
        array_container.add(row_id * 7);

    /// More than 4096 row ids per 65536-row range are stored as a bitset container.
    PostingList bitset_container;
    for (uint32_t row_id = 0; row_id < 30000; ++row_id)
        bitset_container.add(row_id * 2);

    PostingList run_container;
    run_container.addRangeClosed(1000, 500000);
    run_container.runOptimize();

    for (const auto & postings : {empty, array_container, bitset_container, run_container})
    {
        const String encoded = encodePostingsPayload(serializePostings(postings));

        auto decoded_in_place = decodePostingsInPlace(encoded);
        EXPECT_EQ(decoded_in_place.cardinality(), postings.cardinality());
        EXPECT_TRUE(decoded_in_place == postings);

        auto decoded_across_buffers = decodePostingsAcrossBuffers(encoded);
        EXPECT_EQ(decoded_across_buffers.cardinality(), postings.cardinality());
        EXPECT_TRUE(decoded_across_buffers == postings);
    }
}

/// A container that claims more values than the declared payload holds must not be deserialized:
/// the deserializer would read past the end of the buffer and return leaked heap bytes as row ids.
TEST(TextIndexPostingsDeserializationTest, TruncatedPayloadRejected)
{
    PostingList postings;
    for (uint32_t row_id = 0; row_id < 3 * 4096; ++row_id)
        postings.add(row_id * 16);

    const String payload = serializePostings(postings);
    ASSERT_GT(payload.size(), 64u);

    /// Truncate the payload while the declared size stays consistent with what is actually passed in,
    /// so only the container headers claim data that is not there. `Roaring::readSafe` reports this as
    /// `std::runtime_error`, so assert on the common base rather than on the concrete exception type.
    for (size_t size : {payload.size() / 2, payload.size() - 1})
    {
        const String encoded = encodePostingsPayload(payload.substr(0, size));
        EXPECT_THROW(decodePostingsInPlace(encoded), std::exception) << "size = " << size;
        EXPECT_THROW(decodePostingsAcrossBuffers(encoded), std::exception) << "size = " << size;
    }
}
