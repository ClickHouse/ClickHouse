#include <gtest/gtest.h>

#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <Storages/MergeTree/TextIndexCoarsePostings.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <IO/VarInt.h>

#include <absl/container/flat_hash_set.h>

#include <random>
#include <vector>

using namespace DB;

namespace
{

PostingList makePostingList(const std::vector<uint32_t> & values)
{
    PostingList list;
    for (auto value : values)
        list.add(value);
    return list;
}

std::vector<uint32_t> toVector(const PostingList & list)
{
    std::vector<uint32_t> result(list.cardinality());
    list.toUint32Array(result.data());
    return result;
}

/// Reference implementation of the level selection: the finest level in [1, max_level]
/// whose number of distinct buckets fits the budget, or max_level if none fits.
UInt32 bruteForceFinestLevel(const std::vector<uint32_t> & values, UInt64 budget, UInt32 max_level)
{
    for (UInt32 level = 1; level < max_level; ++level)
    {
        absl::flat_hash_set<uint32_t> buckets;
        for (auto value : values)
            buckets.insert(value >> level);

        if (buckets.size() <= budget)
            return level;
    }
    return max_level;
}

std::vector<uint32_t> bruteForceBuckets(const std::vector<uint32_t> & values, UInt32 level)
{
    absl::flat_hash_set<uint32_t> buckets;
    for (auto value : values)
        buckets.insert(value >> level);

    std::vector<uint32_t> result(buckets.begin(), buckets.end());
    std::sort(result.begin(), result.end());
    return result;
}

std::vector<uint32_t> bruteForceExpand(const std::vector<uint32_t> & buckets, UInt32 level)
{
    static constexpr UInt64 row_domain_end = static_cast<UInt64>(std::numeric_limits<UInt32>::max()) + 1;

    std::vector<uint32_t> rows;
    for (auto bucket : buckets)
    {
        UInt64 begin = static_cast<UInt64>(bucket) << level;
        UInt64 end = std::min(begin + (UInt64(1) << level), row_domain_end);
        for (UInt64 row = begin; row < end; ++row)
            rows.push_back(static_cast<uint32_t>(row));
    }
    return rows;
}

}

TEST(TextIndexCoarsePostings, CoarsenLevelSelection)
{
    std::mt19937 rng(42); /// NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed for reproducible test failures

    for (size_t iteration = 0; iteration < 50; ++iteration)
    {
        /// A mix of clustered runs and uniformly spread values.
        std::vector<uint32_t> values;
        size_t num_runs = 1 + rng() % 5;
        for (size_t run = 0; run < num_runs; ++run)
        {
            uint32_t start = rng() % 1000000;
            size_t length = 1 + rng() % 500;
            uint32_t step = 1 + rng() % 3;
            for (size_t i = 0; i < length; ++i)
                values.push_back(start + static_cast<uint32_t>(i) * step);
        }

        std::sort(values.begin(), values.end());
        values.erase(std::unique(values.begin(), values.end()), values.end());

        UInt64 budget = 1 + rng() % values.size();
        UInt32 max_level = 13;

        if (values.size() <= budget)
            continue;

        auto postings = makePostingList(values);
        auto [buckets, level] = coarsenPostings(postings, budget, max_level);

        EXPECT_EQ(level, bruteForceFinestLevel(values, budget, max_level));
        EXPECT_GE(level, 1u);
        EXPECT_LE(level, max_level);
        EXPECT_EQ(toVector(buckets), bruteForceBuckets(values, level));
    }
}

TEST(TextIndexCoarsePostings, CoarsenCapsAtMaxLevel)
{
    /// Uniformly spread values do not collapse below the granule level:
    /// the level selection must stop at max_level even if the budget is not met.
    std::vector<uint32_t> values;
    for (uint32_t i = 0; i < 1000; ++i)
        values.push_back(i * 100000);

    auto postings = makePostingList(values);
    auto [buckets, level] = coarsenPostings(postings, /*budget=*/ 1, /*max_level=*/ 13);

    EXPECT_EQ(level, 13u);
    EXPECT_EQ(toVector(buckets), bruteForceBuckets(values, 13));
}

TEST(TextIndexCoarsePostings, CoarsenBudgetEdges)
{
    /// Adjacent values collapse at level 1 if aligned: {2k, 2k+1} form one bucket.
    auto postings = makePostingList({10, 11, 12, 13});
    auto [buckets, level] = coarsenPostings(postings, /*budget=*/ 2, /*max_level=*/ 13);
    EXPECT_EQ(level, 1u);
    EXPECT_EQ(toVector(buckets), (std::vector<uint32_t>{5, 6}));

    /// A budget of 1 forces the coarsest level that covers the whole run.
    auto [single_bucket, single_level] = coarsenPostings(postings, /*budget=*/ 1, /*max_level=*/ 13);
    EXPECT_EQ(single_level, bruteForceFinestLevel({10, 11, 12, 13}, 1, 13));
    EXPECT_EQ(toVector(single_bucket).size(), 1u);
}

TEST(TextIndexCoarsePostings, ExpandCoarsenIsSuperset)
{
    std::mt19937 rng(7); /// NOLINT(cert-msc32-c,cert-msc51-cpp): deterministic seed for reproducible test failures

    for (size_t iteration = 0; iteration < 20; ++iteration)
    {
        std::vector<uint32_t> values;
        for (size_t i = 0; i < 300; ++i)
            values.push_back(rng() % 100000);

        std::sort(values.begin(), values.end());
        values.erase(std::unique(values.begin(), values.end()), values.end());

        auto postings = makePostingList(values);
        auto [buckets, level] = coarsenPostings(postings, /*budget=*/ 10, /*max_level=*/ 13);
        auto expanded = expandCoarsePostings(buckets, level);

        /// Every original row is covered by the expansion.
        for (auto value : values)
            EXPECT_TRUE(expanded.contains(value));

        EXPECT_EQ(toVector(expanded), bruteForceExpand(toVector(buckets), level));
    }
}

TEST(TextIndexCoarsePostings, ExpandCoalescesAdjacentBuckets)
{
    auto buckets = makePostingList({0, 2, 3, 10});
    auto expanded = expandCoarsePostings(buckets, /*level=*/ 3);

    EXPECT_EQ(toVector(expanded), bruteForceExpand({0, 2, 3, 10}, 3));

    /// Adjacent buckets 2 and 3 coalesce into one contiguous range [16, 32).
    EXPECT_TRUE(expanded.contains(16));
    EXPECT_TRUE(expanded.contains(31));
    EXPECT_FALSE(expanded.contains(32));

    /// Bucket 10 covers rows [80, 88).
    EXPECT_TRUE(expanded.contains(80));
    EXPECT_TRUE(expanded.contains(87));
    EXPECT_FALSE(expanded.contains(88));
}

TEST(TextIndexCoarsePostings, ExpandClampsToRowDomain)
{
    constexpr UInt32 level = 3;
    constexpr UInt32 top_bucket = std::numeric_limits<UInt32>::max() >> level;

    /// The top bucket of the domain expands to the last representable row id and no further.
    auto expanded = expandCoarsePostings(makePostingList({top_bucket}), level);
    EXPECT_EQ(expanded.maximum(), std::numeric_limits<UInt32>::max());
    EXPECT_EQ(expanded.cardinality(), UInt64(1) << level);

    /// A bucket id beyond the domain (only reachable with corrupted data) contributes nothing
    /// instead of wrapping around into low row ids.
    auto out_of_domain = expandCoarsePostings(makePostingList({top_bucket + 1}), level);
    EXPECT_EQ(out_of_domain.cardinality(), 0u);
}

TEST(TextIndexCoarsePostings, MakeCoarseSerializationParams)
{
    MergeTreeIndexTextParams params;
    params.coarse_granularity = 8192;

    /// Budget = ceil(100000 / 8192) = 13 buckets, max_level = log2(8192) = 13.
    auto coarse_params = makeCoarseSerializationParams(params, 100000);
    EXPECT_TRUE(coarse_params.enabled());
    EXPECT_EQ(coarse_params.budget, 13u);
    EXPECT_EQ(coarse_params.max_level, 13u);

    /// Smaller buckets mean a larger budget and a lower level cap.
    params.coarse_granularity = 2048;
    coarse_params = makeCoarseSerializationParams(params, 100000);
    EXPECT_EQ(coarse_params.budget, 49u);
    EXPECT_EQ(coarse_params.max_level, 11u);

    /// An everywhere-token always fits at the level cap: its bucket count
    /// at max_level does not exceed the budget by more than one.
    params.coarse_granularity = 1024;
    coarse_params = makeCoarseSerializationParams(params, 4096);
    EXPECT_EQ(coarse_params.budget, 4u);
    EXPECT_EQ(coarse_params.max_level, 10u);

    /// A bucket size that is not a power of two is rounded down to the level cap.
    params.coarse_granularity = 1000;
    coarse_params = makeCoarseSerializationParams(params, 100000);
    EXPECT_EQ(coarse_params.budget, 100u);
    EXPECT_EQ(coarse_params.max_level, 9u);

    /// Disabled cases: buckets of less than 2 rows are exact posting lists.
    params.coarse_granularity = 0;
    EXPECT_FALSE(makeCoarseSerializationParams(params, 100000).enabled());

    params.coarse_granularity = 1;
    EXPECT_FALSE(makeCoarseSerializationParams(params, 100000).enabled());

    params.coarse_granularity = 8192;
    EXPECT_FALSE(makeCoarseSerializationParams(params, 0).enabled());
}

TEST(TextIndexCoarsePostings, TokenInfoWireRoundTrip)
{
    using enum PostingsSerialization::Flags;

    /// A non-embedded coarse token: level and exact document frequency are stored,
    /// block ranges are written in the bucket domain and read back in the row domain.
    TokenPostingsInfo info;
    info.header = CoarsePostings | SingleBlock | IsCompressed | HasBlockIndex;
    info.postings_cardinality = 5;
    info.coarse_level = 13;
    info.rows_cardinality = 100000;
    info.offsets.emplace_back(777);
    info.ranges.emplace_back(10, 400);

    /// A trailing exact token to check that the coarse fields keep the stream in sync.
    TokenPostingsInfo exact_info;
    exact_info.header = SingleBlock;
    exact_info.postings_cardinality = 42;
    exact_info.offsets.emplace_back(999);
    exact_info.ranges.emplace_back(1, 2);

    WriteBufferFromOwnString out;
    TextIndexSerialization::serializeTokenInfo(out, info);
    TextIndexSerialization::serializeTokenInfo(out, exact_info);

    auto str = out.str();
    auto postings_serialization = PostingsSerialization(
        PostingListCodecFactory::createPostingListCodec(IPostingListCodec::Type::Bitpacking),
        static_cast<MergeTreeIndexVersion>(TextIndexHeader::Version::WithCoarsePostings));

    {
        ReadBufferFromMemory in(str.data(), str.size());
        auto read_info = TextIndexSerialization::deserializeTokenInfo(in, &postings_serialization);

        EXPECT_TRUE(read_info.isCoarse());
        EXPECT_EQ(read_info.coarse_level, 13u);
        EXPECT_EQ(read_info.postings_cardinality, 5u);
        EXPECT_EQ(read_info.rows_cardinality, 100000u);
        EXPECT_EQ(read_info.offsets.size(), 1u);
        EXPECT_EQ(read_info.offsets[0], 777u);

        /// Bucket range [10, 400] at level 13 becomes row range [10 * 8192, 401 * 8192 - 1].
        EXPECT_EQ(read_info.ranges[0].begin, 10u << 13);
        EXPECT_EQ(read_info.ranges[0].end, (401u << 13) - 1);

        auto read_exact = TextIndexSerialization::deserializeTokenInfo(in, &postings_serialization);
        EXPECT_FALSE(read_exact.isCoarse());
        EXPECT_EQ(read_exact.postings_cardinality, 42u);
        EXPECT_EQ(read_exact.rows_cardinality, 42u);
        EXPECT_EQ(read_exact.ranges[0].begin, 1u);
        EXPECT_EQ(read_exact.ranges[0].end, 2u);
    }

    /// skipTokenInfo must skip exactly the same bytes.
    {
        ReadBufferFromMemory in(str.data(), str.size());
        TextIndexSerialization::skipTokenInfo(in);
        auto read_exact = TextIndexSerialization::deserializeTokenInfo(in, &postings_serialization);
        EXPECT_EQ(read_exact.postings_cardinality, 42u);
        EXPECT_TRUE(in.eof());
    }
}

TEST(TextIndexCoarsePostings, EmbeddedCoarseTokenExpandsOnRead)
{
    using enum PostingsSerialization::Flags;

    TokenPostingsInfo info;
    info.header = CoarsePostings | RawPostings | EmbeddedPostings;
    info.postings_cardinality = 3;
    info.coarse_level = 2;
    info.rows_cardinality = 50;

    WriteBufferFromOwnString out;
    TextIndexSerialization::serializeTokenInfo(out, info);

    /// The caller serializes embedded postings (bucket ids) right after the token info.
    for (UInt32 bucket : {1, 5, 6})
        writeVarUInt(bucket, out);

    auto str = out.str();
    auto postings_serialization = PostingsSerialization(
        PostingListCodecFactory::createPostingListCodec(IPostingListCodec::Type::None),
        static_cast<MergeTreeIndexVersion>(TextIndexHeader::Version::WithCoarsePostings));

    ReadBufferFromMemory in(str.data(), str.size());
    auto read_info = TextIndexSerialization::deserializeTokenInfo(in, &postings_serialization);

    EXPECT_TRUE(read_info.isCoarse());
    ASSERT_NE(read_info.embedded_postings, nullptr);

    /// Buckets {1, 5, 6} at level 2 expand to rows {4..7, 20..27}.
    std::vector<uint32_t> expected;
    for (uint32_t row = 4; row < 8; ++row)
        expected.push_back(row);
    for (uint32_t row = 20; row < 28; ++row)
        expected.push_back(row);

    EXPECT_EQ(toVector(*read_info.embedded_postings), expected);
    EXPECT_EQ(read_info.ranges[0].begin, 4u);
    EXPECT_EQ(read_info.ranges[0].end, 27u);
}

TEST(TextIndexCoarsePostings, BitpackingCodecExpandsWhileDecoding)
{
    using enum PostingsSerialization::Flags;
    constexpr UInt32 level = 3;

    /// A long run of adjacent buckets spanning several packed blocks (128 values each),
    /// followed by isolated buckets in a partial block.
    std::vector<uint32_t> buckets;
    for (uint32_t bucket = 0; bucket < 300; ++bucket)
        buckets.push_back(bucket);
    for (uint32_t bucket = 1000; bucket < 1100; bucket += 2)
        buckets.push_back(bucket);

    PostingListCodecBitpacking codec;
    TokenPostingsInfo info;
    info.header = CoarsePostings | IsCompressed | HasBlockIndex;
    info.postings_cardinality = static_cast<UInt32>(buckets.size());
    info.coarse_level = level;

    WriteBufferFromOwnString out;
    codec.encode(makePostingList(buckets), /*max_rowids_in_segment=*/ 1024 * 1024, info, out);
    auto str = out.str();

    /// Without a coarse level the decoded values are the stored bucket ids.
    {
        ReadBufferFromMemory in(str.data(), str.size());
        PostingList decoded;
        codec.decode(in, decoded, /*coarse_level=*/ 0);
        EXPECT_EQ(toVector(decoded), buckets);
    }

    /// With the coarse level the buckets are expanded into rows while decoding, and a run of
    /// adjacent buckets is coalesced across the block boundaries it spans.
    {
        ReadBufferFromMemory in(str.data(), str.size());
        PostingList decoded;
        codec.decode(in, decoded, level);
        EXPECT_EQ(toVector(decoded), bruteForceExpand(buckets, level));
    }
}

TEST(TextIndexCoarsePostings, NoneCodecExpandsWhileDecoding)
{
    using enum PostingsSerialization::Flags;
    constexpr UInt32 level = 4;

    std::vector<uint32_t> buckets = {0, 1, 2, 7, 8, 100};

    auto postings_serialization = PostingsSerialization(
        PostingListCodecFactory::createPostingListCodec(IPostingListCodec::Type::None),
        static_cast<MergeTreeIndexVersion>(TextIndexHeader::Version::WithCoarsePostings));

    TokenPostingsInfo info;
    info.header = CoarsePostings | SingleBlock;
    info.postings_cardinality = static_cast<UInt32>(buckets.size());
    info.coarse_level = level;
    info.rows_cardinality = 1000;

    WriteBufferFromOwnString out;
    postings_serialization.serialize(makePostingList(buckets).roaring, info.header, out);
    auto str = out.str();

    ReadBufferFromMemory in(str.data(), str.size());
    auto decoded = postings_serialization.deserialize(in, info);

    ASSERT_NE(decoded, nullptr);
    EXPECT_EQ(toVector(*decoded), bruteForceExpand(buckets, level));
    EXPECT_TRUE(in.eof());
}

TEST(TextIndexCoarsePostings, NoneCodecExactRoundTrip)
{
    using enum PostingsSerialization::Flags;

    /// An exact posting list of an index with the `none` codec is deserialized by the codec as well.
    std::vector<uint32_t> rows = {0, 1, 2, 1000, 100000};

    auto postings_serialization = PostingsSerialization(
        PostingListCodecFactory::createPostingListCodec(IPostingListCodec::Type::None),
        static_cast<MergeTreeIndexVersion>(TextIndexHeader::Version::WithCoarsePostings));

    TokenPostingsInfo info;
    info.header = SingleBlock;
    info.postings_cardinality = static_cast<UInt32>(rows.size());
    info.rows_cardinality = info.postings_cardinality;

    WriteBufferFromOwnString out;
    postings_serialization.serialize(makePostingList(rows).roaring, info.header, out);
    auto str = out.str();

    ReadBufferFromMemory in(str.data(), str.size());
    auto decoded = postings_serialization.deserialize(in, info);

    ASSERT_NE(decoded, nullptr);
    EXPECT_EQ(toVector(*decoded), rows);
    EXPECT_TRUE(in.eof());
}
