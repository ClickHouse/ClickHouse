#include <gtest/gtest.h>

#include <Storages/MergeTree/TextIndexBlockedPositionsCodec.h>
#include <Compression/PFor.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <limits>
#include <map>
#include <random>
#include <vector>

using DB::TextIndexBlockedPositionsCodec;

namespace
{

using PerDocPositions = std::map<uint32_t, std::vector<uint32_t>>;

std::string encodeToString(const PerDocPositions & docs)
{
    DB::WriteBufferFromOwnString out;
    DB::PositionListBuilder builder;
    for (const auto & [doc, positions] : docs)
        for (uint32_t position : positions)
            builder.add(doc, position);
    builder.finalize(out);
    return out.str();
}

/// Full decode; asserts the stream round-trips to the reference model (per posting rank).
void expectRoundTrip(const PerDocPositions & docs)
{
    const std::string blob = encodeToString(docs);

    DB::ReadBufferFromMemory in(blob.data(), blob.size());
    DB::PaddedPODArray<UInt32> doc_offsets;
    DB::PaddedPODArray<UInt32> positions;
    TextIndexBlockedPositionsCodec::DecodeScratch scratch;
    TextIndexBlockedPositionsCodec::decodeAll(in, docs.size(), blob.size(), doc_offsets, positions, scratch);

    ASSERT_EQ(doc_offsets.size(), docs.size() + 1);
    size_t rank = 0;
    for (const auto & [doc, expected] : docs)
    {
        std::vector<uint32_t> got(positions.begin() + doc_offsets[rank], positions.begin() + doc_offsets[rank + 1]);
        EXPECT_EQ(got, expected) << "posting rank " << rank << " (doc " << doc << ")";
        ++rank;
    }
}

PerDocPositions randomDocs(size_t num_docs, uint32_t max_freq, size_t seed)
{
    std::mt19937 rng(static_cast<std::mt19937::result_type>(seed));
    std::uniform_int_distribution<uint32_t> freq_dist(1, max_freq);
    std::uniform_int_distribution<uint32_t> gap_dist(1, 300);

    PerDocPositions docs;
    uint32_t doc = 0;
    for (size_t i = 0; i < num_docs; ++i)
    {
        doc += 1 + rng() % 50;
        uint32_t position = rng() % 64;
        auto & positions = docs[doc];
        for (uint32_t k = freq_dist(rng); k > 0; --k)
        {
            positions.push_back(position);
            position += gap_dist(rng);
        }
    }
    return docs;
}

std::string singleBlockStream(UInt64 num_docs, const std::string & payload)
{
    DB::WriteBufferFromOwnString blob;
    DB::writeVarUInt(num_docs, blob);
    DB::writeVarUInt(1, blob); /// num_blocks
    DB::writeVarUInt(payload.size(), blob);
    DB::writeString(payload, blob);
    return blob.str();
}

std::string frequencyRun(const std::vector<UInt32> & frequencies_minus_one)
{
    std::string run(DB::PFor::maxCompressedBytes<UInt32>(frequencies_minus_one.size()), '\0');
    run.resize(DB::PFor::encodeBlocks<UInt32>(frequencies_minus_one, DB::PFor::Delta::none, reinterpret_cast<uint8_t *>(run.data())));
    return run;
}

void expectDecodeThrows(const std::string & blob, UInt64 num_docs)
{
    DB::ReadBufferFromMemory in(blob.data(), blob.size());
    DB::PaddedPODArray<UInt32> doc_offsets;
    DB::PaddedPODArray<UInt32> positions;
    TextIndexBlockedPositionsCodec::DecodeScratch scratch;
    EXPECT_THROW(TextIndexBlockedPositionsCodec::decodeAll(in, num_docs, blob.size(), doc_offsets, positions, scratch), DB::Exception);
}

}

TEST(BlockedPositionsCodec, Empty)
{
    expectRoundTrip({});
}

TEST(BlockedPositionsCodec, SingleDocSinglePosition)
{
    expectRoundTrip({{7, {42}}});
}

TEST(BlockedPositionsCodec, SingleDocManyPositions)
{
    /// A document with many positions.
    std::vector<uint32_t> positions;
    for (uint32_t k = 0; k < 300; ++k)
        positions.push_back(k * 37);
    expectRoundTrip({{123456, positions}});
}

TEST(BlockedPositionsCodec, ExactBlockBoundaries)
{
    for (size_t num_docs : {TextIndexBlockedPositionsCodec::BLOCK_DOCS - 1,
                            TextIndexBlockedPositionsCodec::BLOCK_DOCS,
                            TextIndexBlockedPositionsCodec::BLOCK_DOCS + 1,
                            3 * TextIndexBlockedPositionsCodec::BLOCK_DOCS})
        expectRoundTrip(randomDocs(num_docs, 3, num_docs));
}

TEST(BlockedPositionsCodec, RandomLarge)
{
    expectRoundTrip(randomDocs(10000, 6, 1));
    expectRoundTrip(randomDocs(2500, 1, 2)); /// all frequencies 1: no frequency run at all
}

TEST(BlockedPositionsCodec, CandidateBlockDecodeMatchesFullDecode)
{
    const auto docs = randomDocs(1000, 4, 3);
    const std::string prefix(17, 'x'); /// nonzero blob offset
    const std::string blob = encodeToString(docs);
    const std::string file = prefix + blob;

    /// Reference: full decode.
    DB::PaddedPODArray<UInt32> ref_offsets;
    DB::PaddedPODArray<UInt32> ref_positions;
    TextIndexBlockedPositionsCodec::DecodeScratch scratch;
    {
        DB::ReadBufferFromMemory in(blob.data(), blob.size());
        TextIndexBlockedPositionsCodec::decodeAll(in, docs.size(), blob.size(), ref_offsets, ref_positions, scratch);
    }

    DB::ReadBufferFromMemory dir_in(file.data() + prefix.size(), file.size() - prefix.size());
    const auto dir = TextIndexBlockedPositionsCodec::readDirectory(dir_in, prefix.size(), docs.size(), blob.size());
    ASSERT_EQ(dir.num_docs, docs.size());

    /// Every 3rd posting rank as a candidate.
    std::mt19937 rng(4); // NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp)
    std::vector<uint32_t> ranks;
    for (uint32_t r = 0; r < docs.size(); r += 1 + rng() % 5)
        ranks.push_back(r);

    DB::PaddedPODArray<UInt32> offsets;
    DB::PaddedPODArray<UInt32> positions;
    size_t emitted = 0;
    for (size_t b = 0; b < dir.numBlocks(); ++b)
    {
        std::vector<uint32_t> local_ranks;
        for (uint32_t r : ranks)
            if (r / TextIndexBlockedPositionsCodec::BLOCK_DOCS == b)
                local_ranks.push_back(r % TextIndexBlockedPositionsCodec::BLOCK_DOCS);
        if (local_ranks.empty())
            continue;

        /// Seek = a fresh buffer at the block's absolute offset (as the reader's stream seek would).
        DB::ReadBufferFromMemory block_in(file.data() + dir.block_offsets[b], file.size() - dir.block_offsets[b]);
        TextIndexBlockedPositionsCodec::decodeBlock(block_in, dir, b, local_ranks, offsets, positions, scratch);

        for (uint32_t r : ranks)
        {
            if (r / TextIndexBlockedPositionsCodec::BLOCK_DOCS != b)
                continue;
            const std::vector<uint32_t> expected(ref_positions.begin() + ref_offsets[r], ref_positions.begin() + ref_offsets[r + 1]);
            const uint32_t begin = emitted == 0 ? 0 : offsets[emitted - 1];
            const std::vector<uint32_t> got(positions.begin() + begin, positions.begin() + offsets[emitted]);
            EXPECT_EQ(got, expected) << "rank " << r;
            ++emitted;
        }
    }
    ASSERT_EQ(emitted, ranks.size());
}

TEST(BlockedPositionsCodec, RejectsHeaderMismatch)
{
    const std::string blob = encodeToString(randomDocs(200, 3, 5));
    DB::ReadBufferFromMemory in(blob.data(), blob.size());
    DB::PaddedPODArray<UInt32> doc_offsets;
    DB::PaddedPODArray<UInt32> positions;
    TextIndexBlockedPositionsCodec::DecodeScratch scratch;
    EXPECT_THROW(
        TextIndexBlockedPositionsCodec::decodeAll(in, /*expected_num_docs=*/ 201, blob.size(), doc_offsets, positions, scratch),
        DB::Exception);
}

TEST(BlockedPositionsCodec, RejectsTruncation)
{
    const std::string blob = encodeToString(randomDocs(500, 3, 6));
    for (size_t cut : {blob.size() / 2, blob.size() - 1, size_t(3)})
    {
        DB::ReadBufferFromMemory in(blob.data(), cut);
        DB::PaddedPODArray<UInt32> doc_offsets;
        DB::PaddedPODArray<UInt32> positions;
        TextIndexBlockedPositionsCodec::DecodeScratch scratch;
        EXPECT_ANY_THROW(
            TextIndexBlockedPositionsCodec::decodeAll(in, 500, cut, doc_offsets, positions, scratch))
            << "cut at " << cut;
    }
}

TEST(BlockedPositionsCodec, SurvivesCorruption)
{
    /// Flip bytes across the stream; decode must throw or produce a consistent result, never crash.
    const auto docs = randomDocs(300, 4, 7);
    const std::string blob = encodeToString(docs);
    std::mt19937 rng(8); // NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp)
    for (int i = 0; i < 200; ++i)
    {
        std::string corrupted = blob;
        corrupted[rng() % corrupted.size()] ^= static_cast<char>(1 + rng() % 255);
        DB::ReadBufferFromMemory in(corrupted.data(), corrupted.size());
        DB::PaddedPODArray<UInt32> doc_offsets;
        DB::PaddedPODArray<UInt32> positions;
        TextIndexBlockedPositionsCodec::DecodeScratch scratch;
        try
        {
            TextIndexBlockedPositionsCodec::decodeAll(in, docs.size(), corrupted.size(), doc_offsets, positions, scratch);
        }
        catch (const DB::Exception &) /// NOLINT(bugprone-empty-catch)
        {
            /// Expected for most flips.
        }
    }
}

#include <Storages/MergeTree/TextIndexPhraseSearch.h>

namespace
{

DB::PaddedPODArray<UInt32> toPod(const std::vector<uint32_t> & v)
{
    DB::PaddedPODArray<UInt32> out;
    out.insert(v.begin(), v.end());
    return out;
}

/// Test-side wrapper over the appending kernel.
DB::PaddedPODArray<UInt32> matchCandidates(
    const DB::PaddedPODArray<UInt32> & candidates,
    const std::vector<DB::PaddedPODArray<UInt32>> & offsets,
    const std::vector<DB::PaddedPODArray<UInt32>> & positions,
    const std::vector<size_t> & term_to_unique)
{
    DB::PaddedPODArray<UInt32> matching;
    DB::TextIndexPhraseSearch::matchCandidatePositions(
        std::span<const UInt32>(candidates.data(), candidates.size()), offsets, positions, term_to_unique, matching);
    return matching;
}

}

TEST(BlockedPositionsMatch, TwoTerms)
{
    /// candidate 5: term0 at {3, 7}, term1 at {8} -> 7+1 matches; candidate 9: {2} then {10} -> no.
    const auto candidates = toPod({5, 9});
    std::vector<DB::PaddedPODArray<UInt32>> offsets;
    offsets.push_back(toPod({0, 2, 3}));
    offsets.push_back(toPod({0, 1, 2}));
    std::vector<DB::PaddedPODArray<UInt32>> positions;
    positions.push_back(toPod({3, 7, 2}));
    positions.push_back(toPod({8, 10}));

    const auto matching = matchCandidates(candidates, offsets, positions, {0, 1});
    ASSERT_EQ(matching.size(), 1u);
    EXPECT_EQ(matching[0], 5u);
}

TEST(BlockedPositionsMatch, RepeatedTerm)
{
    /// "foo foo": candidate 1 has foo at {0,1,2} (consecutive), candidate 2 only at {4}.
    const auto candidates = toPod({1, 2});
    std::vector<DB::PaddedPODArray<UInt32>> offsets;
    offsets.push_back(toPod({0, 3, 4}));
    std::vector<DB::PaddedPODArray<UInt32>> positions;
    positions.push_back(toPod({0, 1, 2, 4}));

    const auto matching = matchCandidates(candidates, offsets, positions, {0, 0});
    ASSERT_EQ(matching.size(), 1u);
    EXPECT_EQ(matching[0], 1u);

    const auto triple = matchCandidates(candidates, offsets, positions, {0, 0, 0});
    ASSERT_EQ(triple.size(), 1u);
    EXPECT_EQ(triple[0], 1u);

    const auto quad = matchCandidates(candidates, offsets, positions, {0, 0, 0, 0});
    EXPECT_TRUE(quad.empty());
}

TEST(BlockedPositionsMatch, ThreeTermsChain)
{
    const auto candidates = toPod({7});
    std::vector<DB::PaddedPODArray<UInt32>> offsets;
    offsets.push_back(toPod({0, 1}));
    offsets.push_back(toPod({0, 1}));
    offsets.push_back(toPod({0, 1}));
    std::vector<DB::PaddedPODArray<UInt32>> positions;
    positions.push_back(toPod({0}));
    positions.push_back(toPod({1}));
    positions.push_back(toPod({2}));

    auto matching = matchCandidates(candidates, offsets, positions, {0, 1, 2});
    ASSERT_EQ(matching.size(), 1u);
    EXPECT_EQ(matching[0], 7u);

    /// Break the chain: the last term moves off position 2.
    positions[2] = toPod({3});
    matching = matchCandidates(candidates, offsets, positions, {0, 1, 2});
    EXPECT_TRUE(matching.empty());
}

TEST(BlockedPositionsMatch, Empty)
{
    EXPECT_TRUE(matchCandidates({}, {}, {}, {}).empty());
}

TEST(BlockedPositionsCodec, RejectsFrequencyAboveUInt32)
{
    /// The declared ~40 MB payload lifts the payload-derived bound above UInt32 max, so the
    /// frequency slips past it and must be rejected explicitly, not truncated.
    const std::string run = frequencyRun({std::numeric_limits<UInt32>::max()});
    DB::WriteBufferFromOwnString payload;
    DB::writeVarUInt(run.size(), payload);
    DB::writeString(run, payload);
    std::string blob = payload.str();
    blob.resize(40'000'000, '\0');
    expectDecodeThrows(singleBlockStream(1, blob), 1);
}

TEST(BlockedPositionsCodec, RejectsFrequencyRunBeyondBlock)
{
    DB::WriteBufferFromOwnString payload;
    DB::writeVarUInt(50, payload); /// freq_bytes, larger than the payload
    DB::writeString(std::string(3, '\0'), payload);
    expectDecodeThrows(singleBlockStream(1, payload.str()), 1);
}

TEST(BlockedPositionsCodec, RejectsPositionsBeyondBlock)
{
    /// 300 positions in a stream too short for them.
    const std::string run = frequencyRun({299});
    DB::WriteBufferFromOwnString payload;
    DB::writeVarUInt(run.size(), payload);
    DB::writeString(run, payload);
    DB::writeString(std::string(8, '\0'), payload);
    expectDecodeThrows(singleBlockStream(1, payload.str()), 1);
}
