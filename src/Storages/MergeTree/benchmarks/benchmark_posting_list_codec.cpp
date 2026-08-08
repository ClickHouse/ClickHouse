#include <Storages/MergeTree/IPostingListCodec.h>
#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/MergeTreeIndexTextPostingListCodec.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <config.h>
#include <benchmark/benchmark.h>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>
#include <random>

using namespace DB;

namespace
{

/// Production default for the `text_index_posting_list_block_size` setting (the per-segment size). A posting
/// list with up to ~1M entries therefore fits in a single segment, which matches the common case in practice.
constexpr size_t SEGMENT_SIZE = 1u << 20;

/// Gap shapes between consecutive doc ids. FastPFOR's advantage over plain bit-packing is highly
/// distribution-dependent, so the comparison only means something across several of these.
enum class Distribution
{
    Dense,      /// gap ~ 1..2: a high-frequency token present in almost every row.
    Medium,     /// gap ~ 1..100: a moderately common token.
    Sparse,     /// gap ~ 1..1000: a rare token scattered across rows.
    Clustered,  /// bursts of consecutive rows separated by large gaps: typical of real text.
};

const char * distributionName(Distribution dist)
{
    switch (dist)
    {
        case Distribution::Dense: return "dense";
        case Distribution::Medium: return "medium";
        case Distribution::Sparse: return "sparse";
        case Distribution::Clustered: return "clustered";
    }
    return "unknown";
}

/// Generate `count` strictly increasing doc ids following `dist`. Seeds are fixed so every run is reproducible,
/// and gap ranges keep the largest id below 2^32 even at the largest cardinality.
std::vector<uint32_t> generateIds(Distribution dist, size_t count, uint32_t seed)
{
    std::mt19937 rng(seed);
    std::vector<uint32_t> ids;
    ids.reserve(count);

    uint64_t cur = 0;
    auto push = [&](uint64_t gap)
    {
        cur += gap;
        ids.push_back(static_cast<uint32_t>(cur));
    };

    switch (dist)
    {
        case Distribution::Dense:
        {
            std::uniform_int_distribution<uint32_t> gap(1, 2);
            for (size_t i = 0; i < count; ++i)
                push(gap(rng));
            break;
        }
        case Distribution::Medium:
        {
            std::uniform_int_distribution<uint32_t> gap(1, 100);
            for (size_t i = 0; i < count; ++i)
                push(gap(rng));
            break;
        }
        case Distribution::Sparse:
        {
            std::uniform_int_distribution<uint32_t> gap(1, 1000);
            for (size_t i = 0; i < count; ++i)
                push(gap(rng));
            break;
        }
        case Distribution::Clustered:
        {
            std::uniform_int_distribution<uint32_t> run_len(1, 200);
            std::uniform_int_distribution<uint32_t> jump(1000, 100000);
            size_t i = 0;
            while (i < count)
            {
                /// Start a new cluster with a large jump (the first one starts at a small offset).
                push(i == 0 ? 1 : jump(rng));
                ++i;
                /// Fill the cluster with consecutive (gap == 1) ids.
                uint32_t length = run_len(rng);
                for (uint32_t r = 1; r < length && i < count; ++r, ++i)
                    push(1);
            }
            break;
        }
    }

    return ids;
}

PostingList makePostings(Distribution dist, size_t count)
{
    /// Fold the shape and size into the seed so different cases use independent streams but stay reproducible.
    const uint32_t seed = 0xC0DEC ^ (static_cast<uint32_t>(dist) << 24) ^ static_cast<uint32_t>(count);
    PostingList postings;
    for (uint32_t id : generateIds(dist, count, seed))
        postings.add(id);
    return postings;
}

void benchEncode(benchmark::State & state, IPostingListCodec::Type type, Distribution dist, size_t count)
{
    auto codec = PostingListCodecFactory::createPostingListCodec(type);
    const PostingList postings = makePostings(dist, count);

    size_t encoded_bytes = 0;
    for (auto _ : state)
    {
        TokenPostingsInfo info;
        WriteBufferFromOwnString out;
        codec->encode(postings, SEGMENT_SIZE, info, out);
        out.finalize();
        encoded_bytes = out.count();
        benchmark::DoNotOptimize(encoded_bytes);
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(count));
    state.counters["encoded_bytes"] = static_cast<double>(encoded_bytes);
    state.counters["bytes/value"] = static_cast<double>(encoded_bytes) / static_cast<double>(count);
}

void benchDecode(benchmark::State & state, IPostingListCodec::Type type, Distribution dist, size_t count)
{
    auto codec = PostingListCodecFactory::createPostingListCodec(type);
    const PostingList postings = makePostings(dist, count);

    TokenPostingsInfo info;
    WriteBufferFromOwnString out;
    codec->encode(postings, SEGMENT_SIZE, info, out);
    const std::string serialized = out.str();

    for (auto _ : state)
    {
        ReadBufferFromString in(serialized);
        PostingList decoded;
        codec->decode(in, decoded);
        benchmark::DoNotOptimize(decoded.cardinality());
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations()) * static_cast<int64_t>(count));
    state.counters["encoded_bytes"] = static_cast<double>(serialized.size());
    state.counters["bytes/value"] = static_cast<double>(serialized.size()) / static_cast<double>(count);
}

}

int main(int argc, char ** argv)
{
    /// Only the two real per-block codecs are compared. Type::None is a no-op at this interface (the raw
    /// roaring bitmap is serialized by the surrounding code, not by the codec), so it has nothing to measure.
    const std::vector<std::pair<const char *, IPostingListCodec::Type>> codecs = {
        {"bitpacking", IPostingListCodec::Type::Bitpacking},
#if USE_FASTPFOR
        {"fastpfor", IPostingListCodec::Type::FastPFOR},
#endif
    };
    const std::vector<Distribution> distributions = {
        Distribution::Dense, Distribution::Medium, Distribution::Sparse, Distribution::Clustered};
    const std::vector<size_t> sizes = {10'000, 100'000, 1'000'000};

    for (const auto & [codec_name, type] : codecs)
        for (Distribution dist : distributions)
            for (size_t count : sizes)
            {
                const std::string suffix = std::string(codec_name) + "/" + distributionName(dist) + "/" + std::to_string(count);
                const std::string encode_name = "encode/" + suffix;
                const std::string decode_name = "decode/" + suffix;
                benchmark::RegisterBenchmark(encode_name.c_str(), benchEncode, type, dist, count);
                benchmark::RegisterBenchmark(decode_name.c_str(), benchDecode, type, dist, count);
            }

    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
    return 0;
}
