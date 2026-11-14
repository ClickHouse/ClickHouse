#include <gtest/gtest.h>

#include <Common/PODArray.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>

#include <fmt/ranges.h>
#include <roaring/roaring.hh>

#include <limits>
#include <vector>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

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
    codec.decode(in, std::numeric_limits<UInt64>::max(), postings, TextIndexScoringKind::None, buffer);
    return postings;
}

template <typename F>
void expectCorruptedData(F && f, const std::vector<UInt32> & row_ids)
{
    try
    {
        f();
        ADD_FAILURE() << "Expected CORRUPTED_DATA for row ids " << fmt::format("{}", fmt::join(row_ids, ", "));
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::CORRUPTED_DATA) << e.message();
    }
}

/// Raw postings with an interior row id out of the strictly increasing order, while the first and the last row ids
/// are equal to the row range of the token: the interior row id may lie far outside the range.
const std::vector<std::vector<UInt32>> malformed_raw_row_ids =
{
    {0, 1048576, 5},
    {0, 3, 3, 5},
    {0, 4, 2, 5},
    {0, 1, 2, 3, 4, 5, std::numeric_limits<UInt32>::max(), 7},
    {0, 1, 2, 3, 4, 5, 6, 8, 7, 9, 10, 11},
};

/// The token info and the raw postings in `.pst` written the way the index writer does for `row_ids`.
TokenPostingsInfo makeRawTokenInfo(const std::vector<UInt32> & row_ids, const std::vector<UInt32> & weights, String & postings)
{
    using enum PostingsSerialization::Flags;

    TokenPostingsInfo info;
    info.header = RawPostings | SingleBlock;
    if (!weights.empty())
        info.header |= HasWeights;
    info.cardinality = static_cast<UInt32>(row_ids.size());
    info.offsets.emplace_back(0);
    info.ranges.emplace_back(row_ids.front(), row_ids.back());

    WriteBufferFromOwnString out;
    TextIndexSerialization::serializeRawPostings(row_ids, weights, out);
    postings = out.str();
    return info;
}

/// The token info with embedded postings serialized the way the dictionary block stores it.
String serializeEmbeddedTokenInfo(const std::vector<UInt32> & row_ids)
{
    using enum PostingsSerialization::Flags;

    TokenPostingsInfo info;
    info.header = RawPostings | EmbeddedPostings;
    info.cardinality = static_cast<UInt32>(row_ids.size());

    WriteBufferFromOwnString out;
    TextIndexSerialization::serializeTokenInfo(out, info);
    TextIndexSerialization::serializeRawPostings(row_ids, {}, out);
    return out.str();
}

PostingsSerialization makeRawPostingsSerialization(TextIndexScoringKind scoring)
{
    return PostingsSerialization(std::make_unique<PostingListCodecNone>(), MergeTreeTextIndexSerializationVersion::V3_WithScoring, scoring);
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

/// The strict order check must not reject raw and embedded postings that the index writer produces.
TEST(TextIndexPostingsDeserializationTest, RawPostingsRoundTrip)
{
    for (const auto & row_ids : std::vector<std::vector<UInt32>>{{7}, {0, 1, 2}, {0, 1048576, std::numeric_limits<UInt32>::max()}})
    {
        ReadBufferFromString in(serializeEmbeddedTokenInfo(row_ids));
        auto info = TextIndexSerialization::deserializeTokenInfo(in, /*with_postings=*/ true);
        EXPECT_EQ(std::vector<UInt32>(info.embedded_postings.begin(), info.embedded_postings.end()), row_ids);
    }

    const std::vector<UInt32> row_ids = {3, 10, 11, 12, 100, 1000, 1048576};
    const std::vector<UInt32> weights = {1, 2, 3, 4, 5, 6, 7};

    String postings;
    auto info = makeRawTokenInfo(row_ids, weights, postings);
    auto serialization = makeRawPostingsSerialization(TextIndexScoringKind::BM25);

    ReadBufferFromString in(postings);
    PaddedPODArray<UInt32> decoded_row_ids;
    PaddedPODArray<UInt32> decoded_weights;
    serialization.deserializeToArray(in, info, 0, decoded_row_ids, &decoded_weights);
    EXPECT_EQ(std::vector<UInt32>(decoded_row_ids.begin(), decoded_row_ids.end()), row_ids);
    EXPECT_EQ(std::vector<UInt32>(decoded_weights.begin(), decoded_weights.end()), weights);

    ReadBufferFromString bitmap_in(postings);
    EXPECT_EQ(serialization.deserializeToBitmap(bitmap_in, info, 0)->cardinality(), row_ids.size());
}

/// The row range check of a segment verifies only the first and the last row ids,
/// so an out-of-order interior row id of raw postings must be rejected on read, before consumers index by it.
TEST(TextIndexPostingsDeserializationTest, RawPostingsOutOfOrderRejected)
{
    for (const auto & row_ids : malformed_raw_row_ids)
    {
        for (bool with_weights : {false, true})
        {
            const std::vector<UInt32> weights = with_weights ? std::vector<UInt32>(row_ids.size(), 2) : std::vector<UInt32>{};
            String postings;
            auto info = makeRawTokenInfo(row_ids, weights, postings);
            auto serialization = makeRawPostingsSerialization(TextIndexScoringKind::BM25);

            expectCorruptedData([&]
            {
                ReadBufferFromString in(postings);
                PaddedPODArray<UInt32> decoded_row_ids;
                PaddedPODArray<UInt32> decoded_weights;
                serialization.deserializeToArray(in, info, 0, decoded_row_ids, &decoded_weights);
            }, row_ids);

            expectCorruptedData([&]
            {
                ReadBufferFromString in(postings);
                PaddedPODArray<UInt32> decoded_row_ids;
                serialization.deserializeToArray(in, info, 0, decoded_row_ids, nullptr);
            }, row_ids);

            expectCorruptedData([&]
            {
                ReadBufferFromString in(postings);
                serialization.deserializeToBitmap(in, info, 0);
            }, row_ids);
        }
    }
}

TEST(TextIndexPostingsDeserializationTest, EmbeddedPostingsOutOfOrderRejected)
{
    for (const auto & row_ids : malformed_raw_row_ids)
    {
        if (row_ids.size() > MAX_CARDINALITY_FOR_EMBEDDED_POSTINGS)
            continue;

        const String serialized = serializeEmbeddedTokenInfo(row_ids);
        expectCorruptedData([&]
        {
            ReadBufferFromString in(serialized);
            TextIndexSerialization::deserializeTokenInfo(in, /*with_postings=*/ true);
        }, row_ids);
    }
}
