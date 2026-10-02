#include <gtest/gtest.h>

#include <Common/PODArray.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>

#include <roaring/roaring.hh>

#include <limits>
#include <vector>

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

PostingList decodePostings(const String & encoded)
{
    PostingListCodecNone codec;
    ReadBufferFromString in(encoded);
    PostingList postings;
    PaddedPODArray<char> buffer;
    codec.decode(in, std::numeric_limits<UInt64>::max(), postings, buffer);
    return postings;
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
        auto decoded = decodePostings(encodePostingsPayload(serializePostings(postings)));
        EXPECT_EQ(decoded.cardinality(), postings.cardinality());
        EXPECT_TRUE(decoded == postings);
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
        const String truncated = payload.substr(0, size);
        EXPECT_THROW(decodePostings(encodePostingsPayload(truncated)), std::exception) << "size = " << size;
    }
}

/// A decoded block whose first and last row ids span as many values as it holds is a range only if
/// it is increasing: a corrupted block must be added as the row ids it holds.
TEST(TextIndexPostingsDeserializationTest, NonIncreasingBlockIsNotRange)
{
    struct Case
    {
        std::vector<uint32_t> deltas;
        PostingList expected;
    };

    /// Deltas from the first row id 5: row ids 5, 6, 6, 8 and 5, 9, 6, 8.
    const std::vector<Case> cases = {
        {{0, 1, 0, 2}, {5, 6, 8}},
        {{0, 4, std::numeric_limits<uint32_t>::max() - 2, 2}, {5, 6, 8, 9}},
    };

    for (auto type : {IPostingListCodec::Type::Bitpacking, IPostingListCodec::Type::PFor})
    {
        for (auto [deltas, expected] : cases)
        {
            PODArray<char> payload;
            createPostingListBlockCodec(type)->encodeBlock(deltas, payload);

            /// Segment header: codec type, payload size, cardinality, first row id.
            WriteBufferFromOwnString out;
            writeVarUInt(static_cast<UInt8>(type), out);
            writeVarUInt(payload.size(), out);
            writeVarUInt(deltas.size(), out);
            writeVarUInt(5, out);
            out.write(payload.data(), payload.size());

            ReadBufferFromString in(out.str());
            PostingList postings;
            PaddedPODArray<char> buffer;
            SegmentedPostingListCodec().decode(in, deltas.size(), postings, buffer);
            EXPECT_TRUE(postings == expected) << "codec " << static_cast<int>(type) << ": " << postings.toString();
        }
    }
}
