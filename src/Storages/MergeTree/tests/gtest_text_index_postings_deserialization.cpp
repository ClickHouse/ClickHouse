#include <gtest/gtest.h>

#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCursor.h>

#include <roaring/roaring.hh>

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
    codec.decode(in, postings, buffer);
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

/// The lazy cursor restores absolute row ids from per-block deltas (`inclusive_scan`) without knowing the
/// segment layout, so a crafted `.pst` payload can produce ids that are non-monotonic or outside the
/// segment's row range. The apply paths (`padColumn`, leapfrog) assume the ids are strictly increasing and
/// clip them with `std::lower_bound`, so such an array would be written out of bounds. `requireDecodedRowIdsValid`
/// rejects it first.
TEST(TextIndexPostingsDeserializationTest, DecodedRowIdsValidationAcceptsWellFormed)
{
    /// A legitimate block: strictly increasing ids inside [range_begin, range_end].
    const std::vector<uint32_t> values = {10, 11, 15, 16, 100, 4095};
    EXPECT_NO_THROW(requireDecodedRowIdsValid(values.data(), values.size(), 10, 4095));

    /// Single element at the range boundary is fine.
    const uint32_t single = 4095;
    EXPECT_NO_THROW(requireDecodedRowIdsValid(&single, 1, 0, 4095));
}

TEST(TextIndexPostingsDeserializationTest, DecodedRowIdsValidationRejectsWrapCliff)
{
    /// The exact vector from the finding: a large `first_row_id` (0xFFFFFFF0) makes the uint32
    /// `inclusive_scan` wrap, producing a non-monotonic "cliff" array. `values[0] - row_begin` in
    /// `padColumn` would then index far out of bounds. Both the out-of-range and the non-monotonic
    /// checks catch this.
    const std::vector<uint32_t> wrapped = {0xFFFFFFF0u, 0xFFFFFFF1u, 0x00000001u, 0x00000002u};
    EXPECT_THROW(requireDecodedRowIdsValid(wrapped.data(), wrapped.size(), 0, 1048575), DB::Exception);
}

TEST(TextIndexPostingsDeserializationTest, DecodedRowIdsValidationRejectsOutOfRange)
{
    /// A value above the segment's range end.
    const std::vector<uint32_t> above = {5, 6, 9999};
    EXPECT_THROW(requireDecodedRowIdsValid(above.data(), above.size(), 0, 100), DB::Exception);

    /// A value below the segment's range begin.
    const std::vector<uint32_t> below = {3, 50, 60};
    EXPECT_THROW(requireDecodedRowIdsValid(below.data(), below.size(), 10, 100), DB::Exception);
}

TEST(TextIndexPostingsDeserializationTest, DecodedRowIdsValidationRejectsNonMonotonic)
{
    /// Equal adjacent ids (delta 0) are not strictly increasing.
    const std::vector<uint32_t> equal = {5, 5, 6};
    EXPECT_THROW(requireDecodedRowIdsValid(equal.data(), equal.size(), 0, 100), DB::Exception);

    /// Decreasing ids in the middle of an otherwise in-range block.
    const std::vector<uint32_t> decreasing = {5, 20, 19, 30};
    EXPECT_THROW(requireDecodedRowIdsValid(decreasing.data(), decreasing.size(), 0, 100), DB::Exception);
}
