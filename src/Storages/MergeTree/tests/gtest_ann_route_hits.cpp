#include "config.h"
#if USE_DISKANN

#include <gtest/gtest.h>

#include <Storages/MergeTree/ANNIndex/ANNHitRouting.h>
#include <Storages/MergeTree/ANNIndex/ANNIndexManager.h>
#include <Storages/MergeTree/ANNIndex/PartRowId.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartState.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/tests/MergeTreeTestHarness.h>
#include <Storages/StorageMergeTree.h>

#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

using namespace DB;

namespace
{
namespace fs = std::filesystem;

fs::path makeUniqueTempDir(const std::string & tag)
{
    static std::atomic<uint64_t> counter{0};
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = fs::temp_directory_path()
           / ("clickhouse-ann-route-" + tag + "-"
              + std::to_string(now) + "-"
              + std::to_string(counter.fetch_add(1)));
    fs::create_directories(p);
    return p;
}

class TempDirScope
{
public:
    explicit TempDirScope(const std::string & tag) : path(makeUniqueTempDir(tag)) {}
    ~TempDirScope() { std::error_code ec; fs::remove_all(path, ec); }
    fs::path path;
};

class StorageScope
{
public:
    explicit StorageScope(MergeTreeTestHarness::TestStorage s_) : s(std::move(s_)) {}
    ~StorageScope() { if (s.storage) s.storage->flushAndShutdown(); }
    MergeTreeTestHarness::TestStorage & get() { return s; }
private:
    MergeTreeTestHarness::TestStorage s;
};

std::vector<std::vector<float>> fillVectors(size_t rows, size_t dim, float base)
{
    std::vector<std::vector<float>> out(rows, std::vector<float>(dim, 0.f));
    for (size_t r = 0; r < rows; ++r)
        for (size_t c = 0; c < dim; ++c)
            out[r][c] = base + 0.1f * static_cast<float>(r) + 0.01f * static_cast<float>(c);
    return out;
}

DataPartsVector activeParts(const StorageMergeTree & storage)
{
    return storage.getDataPartsVectorForInternalUsage({MergeTreeDataPartState::Active});
}

/// Build a `RangesInDataParts` from a subset of active parts. Distance values / block coords are
/// populated by `routeANNHitsToParts` — here we only need the parts and their info.
RangesInDataParts makeRangesFromParts(const DataPartsVector & parts)
{
    RangesInDataParts out;
    out.reserve(parts.size());
    for (const auto & part : parts)
        out.emplace_back(part);
    return out;
}

/// Fake hash: one-to-one function on the partition id string. Tests drive specific values by
/// pre-seeding a map so `routeANNHitsToParts` can look up parts deterministically.
struct PartitionHashOracle
{
    std::unordered_map<String, UInt64> mapping;
    UInt64 operator()(const String & pid) const
    {
        auto it = mapping.find(pid);
        if (it != mapping.end())
            return it->second;
        /// Stable fallback so unknown ids still route consistently but to a distinct bucket.
        UInt64 acc = 0xcbf29ce484222325ULL;
        for (char c : pid)
        {
            acc ^= static_cast<UInt64>(static_cast<unsigned char>(c));
            acc *= 0x100000001b3ULL;
        }
        return acc;
    }
};

}

/// Q-T1: all hits belong to the same partition; every hit should land in the sole candidate part.
TEST(ANNRouteHitsTest, RouteHitsAllToOnePart)
{
    TempDirScope disk_scope("q-t1");
    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk_scope.path.string(), "store/route_q_t1", "emb", /*dim=*/ 4, /*partition_key_column=*/ "pk"));
    auto & setup = storage_scope.get();

    /// Three parts, three distinct partitions.
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 3, 1, fillVectors(3, 4, 1.0f));
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 3, 2, fillVectors(3, 4, 2.0f));
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 3, 3, fillVectors(3, 4, 3.0f));

    auto parts = activeParts(*setup.storage);
    ASSERT_EQ(parts.size(), 3u);

    PartitionHashOracle oracle;
    oracle.mapping[parts[0]->info.getPartitionId()] = 111;
    oracle.mapping[parts[1]->info.getPartitionId()] = 222;
    oracle.mapping[parts[2]->info.getPartitionId()] = 333;

    /// 5 hits all pointing at parts[0]: same partition_hash, block within its [min, max].
    std::vector<ANNSearchHit> hits;
    for (size_t i = 0; i < 5; ++i)
        hits.push_back({PartRowId{111, static_cast<UInt64>(parts[0]->info.min_block), i}, 0.1f * static_cast<float>(i)});

    auto ranges = makeRangesFromParts(parts);
    routeANNHitsToParts(
        ranges, hits,
        [&oracle](const String & pid) { return oracle(pid); },
        [](const DataPartPtr &) { return true; });

    ASSERT_TRUE(ranges[0].read_hints.ann_search_results.has_value());
    EXPECT_EQ(ranges[0].read_hints.ann_search_results->block_coords.size(), 5u);
    EXPECT_EQ(ranges[0].read_hints.ann_search_results->distances.size(), 5u);

    /// Other parts are "covered" (per the predicate) so they get empty results — no hits there.
    ASSERT_TRUE(ranges[1].read_hints.ann_search_results.has_value());
    EXPECT_TRUE(ranges[1].read_hints.ann_search_results->block_coords.empty());
    ASSERT_TRUE(ranges[2].read_hints.ann_search_results.has_value());
    EXPECT_TRUE(ranges[2].read_hints.ann_search_results->block_coords.empty());
}

/// Q-T1a: a hit whose block_number is outside the part's [min_block, max_block] is silently
/// skipped; parts stay with no hits.
TEST(ANNRouteHitsTest, RouteHitsBlockNumberOutsideRange)
{
    TempDirScope disk_scope("q-t1a");
    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk_scope.path.string(), "store/route_q_t1a", "emb", /*dim=*/ 4));
    auto & setup = storage_scope.get();

    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 2, 10, fillVectors(2, 4, 1.0f));

    auto parts = activeParts(*setup.storage);
    ASSERT_EQ(parts.size(), 1u);

    PartitionHashOracle oracle;
    oracle.mapping[parts[0]->info.getPartitionId()] = 0xDEAD;

    /// Pick a block_number that is definitely outside the part's range.
    const UInt64 out_of_range_block = static_cast<UInt64>(parts[0]->info.max_block) + 1000;
    std::vector<ANNSearchHit> hits = {{PartRowId{0xDEAD, out_of_range_block, 0}, 0.5f}};

    auto ranges = makeRangesFromParts(parts);
    routeANNHitsToParts(
        ranges, hits,
        [&oracle](const String & pid) { return oracle(pid); },
        [](const DataPartPtr &) { return true; });

    /// Covered (predicate says so), but no hits matched the range.
    ASSERT_TRUE(ranges[0].read_hints.ann_search_results.has_value());
    EXPECT_TRUE(ranges[0].read_hints.ann_search_results->block_coords.empty());
}

/// Q-T1b: multiple parts in the same partition have disjoint block-number intervals. Each hit's
/// block_number must route to exactly the one part whose interval contains it; no cross-routing.
TEST(ANNRouteHitsTest, RouteHitsSamePartitionMultipleParts)
{
    TempDirScope disk_scope("q-t1b");
    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk_scope.path.string(), "store/route_q_t1b", "emb", /*dim=*/ 4));
    auto & setup = storage_scope.get();

    /// All three inserts into partition id 7 create three successive parts with non-overlapping
    /// min/max block intervals assigned by the storage.
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 2, 7, fillVectors(2, 4, 1.0f));
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 2, 7, fillVectors(2, 4, 2.0f));
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 2, 7, fillVectors(2, 4, 3.0f));

    auto parts = activeParts(*setup.storage);
    ASSERT_EQ(parts.size(), 3u);

    /// Same partition hash for all three (because the partition id is identical).
    const UInt64 ph = 0xAB123456CDULL;
    PartitionHashOracle oracle;
    for (const auto & part : parts)
        oracle.mapping[part->info.getPartitionId()] = ph;

    std::vector<ANNSearchHit> hits;
    /// One hit targeting each part's min_block.
    for (size_t i = 0; i < parts.size(); ++i)
        hits.push_back({PartRowId{ph, static_cast<UInt64>(parts[i]->info.min_block), i * 100}, static_cast<float>(i)});

    auto ranges = makeRangesFromParts(parts);
    routeANNHitsToParts(
        ranges, hits,
        [&oracle](const String & pid) { return oracle(pid); },
        [](const DataPartPtr &) { return true; });

    /// Invariant: each part receives exactly one hit, and it is the one whose block_number is
    /// inside the part's interval.
    for (size_t i = 0; i < parts.size(); ++i)
    {
        ASSERT_TRUE(ranges[i].read_hints.ann_search_results.has_value()) << "part " << i;
        EXPECT_EQ(ranges[i].read_hints.ann_search_results->block_coords.size(), 1u) << "part " << i;
        EXPECT_EQ(ranges[i].read_hints.ann_search_results->block_coords[0].first,
                  static_cast<UInt64>(parts[i]->info.min_block)) << "part " << i;
        EXPECT_EQ(ranges[i].read_hints.ann_search_results->distances.size(), 1u) << "part " << i;
    }
}

/// Q-T2: empty hits + every part covered => every part gets an empty (non-nullopt) result.
TEST(ANNRouteHitsTest, RouteHitsZeroHits)
{
    TempDirScope disk_scope("q-t2");
    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk_scope.path.string(), "store/route_q_t2", "emb", /*dim=*/ 4));
    auto & setup = storage_scope.get();

    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 2, 1, fillVectors(2, 4, 1.0f));
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 2, 2, fillVectors(2, 4, 2.0f));
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 2, 3, fillVectors(2, 4, 3.0f));

    auto parts = activeParts(*setup.storage);
    ASSERT_EQ(parts.size(), 3u);

    PartitionHashOracle oracle;
    for (size_t i = 0; i < parts.size(); ++i)
        oracle.mapping[parts[i]->info.getPartitionId()] = 100 + i;

    auto ranges = makeRangesFromParts(parts);
    routeANNHitsToParts(
        ranges, {},
        [&oracle](const String & pid) { return oracle(pid); },
        [](const DataPartPtr &) { return true; });

    for (const auto & r : ranges)
    {
        ASSERT_TRUE(r.read_hints.ann_search_results.has_value());
        EXPECT_TRUE(r.read_hints.ann_search_results->block_coords.empty());
        EXPECT_TRUE(r.read_hints.ann_search_results->distances.empty());
    }
}

/// Q-T3: hits spread across two partitions with one part each; no cross-partition interleaving.
TEST(ANNRouteHitsTest, RouteHitsMultiPartitionsNoInterleave)
{
    TempDirScope disk_scope("q-t3");
    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk_scope.path.string(), "store/route_q_t3", "emb", /*dim=*/ 4));
    auto & setup = storage_scope.get();

    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 4, 100, fillVectors(4, 4, 1.0f));
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 4, 200, fillVectors(4, 4, 2.0f));

    auto parts = activeParts(*setup.storage);
    ASSERT_EQ(parts.size(), 2u);

    PartitionHashOracle oracle;
    const UInt64 ph_a = 0xA000ULL;
    const UInt64 ph_b = 0xB000ULL;
    oracle.mapping[parts[0]->info.getPartitionId()] = ph_a;
    oracle.mapping[parts[1]->info.getPartitionId()] = ph_b;

    std::vector<ANNSearchHit> hits;
    /// 6 hits to partition A, 4 hits to partition B; distinct block_offsets to detect interleaving.
    for (size_t i = 0; i < 6; ++i)
        hits.push_back({PartRowId{ph_a, static_cast<UInt64>(parts[0]->info.min_block), 1000 + i}, 0.1f});
    for (size_t i = 0; i < 4; ++i)
        hits.push_back({PartRowId{ph_b, static_cast<UInt64>(parts[1]->info.min_block), 2000 + i}, 0.2f});

    auto ranges = makeRangesFromParts(parts);
    routeANNHitsToParts(
        ranges, hits,
        [&oracle](const String & pid) { return oracle(pid); },
        [](const DataPartPtr &) { return true; });

    ASSERT_TRUE(ranges[0].read_hints.ann_search_results.has_value());
    EXPECT_EQ(ranges[0].read_hints.ann_search_results->block_coords.size(), 6u);
    for (const auto & [bn, bo] : ranges[0].read_hints.ann_search_results->block_coords)
    {
        EXPECT_EQ(bn, static_cast<UInt64>(parts[0]->info.min_block));
        EXPECT_GE(bo, 1000u);
        EXPECT_LT(bo, 1006u);
    }

    ASSERT_TRUE(ranges[1].read_hints.ann_search_results.has_value());
    EXPECT_EQ(ranges[1].read_hints.ann_search_results->block_coords.size(), 4u);
    for (const auto & [bn, bo] : ranges[1].read_hints.ann_search_results->block_coords)
    {
        EXPECT_EQ(bn, static_cast<UInt64>(parts[1]->info.min_block));
        EXPECT_GE(bo, 2000u);
        EXPECT_LT(bo, 2004u);
    }
}

/// When a part returns `false` for `is_part_covered`, it must stay at `nullopt` — the range
/// reader relies on this to fall back to runtime `_distance` computation for unindexed parts.
TEST(ANNRouteHitsTest, UncoveredPartStaysNullopt)
{
    TempDirScope disk_scope("uncovered");
    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk_scope.path.string(), "store/route_uncovered", "emb", /*dim=*/ 4));
    auto & setup = storage_scope.get();

    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 2, 1, fillVectors(2, 4, 1.0f));
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 2, 2, fillVectors(2, 4, 2.0f));

    auto parts = activeParts(*setup.storage);
    ASSERT_EQ(parts.size(), 2u);

    PartitionHashOracle oracle;
    oracle.mapping[parts[0]->info.getPartitionId()] = 1;
    oracle.mapping[parts[1]->info.getPartitionId()] = 2;

    /// Only parts[0] is covered; parts[1] must keep `ann_search_results == nullopt`.
    auto coverage = [&](const DataPartPtr & part) { return part.get() == parts[0].get(); };

    auto ranges = makeRangesFromParts(parts);
    routeANNHitsToParts(ranges, {}, [&oracle](const String & pid) { return oracle(pid); }, coverage);

    ASSERT_TRUE(ranges[0].read_hints.ann_search_results.has_value());
    EXPECT_TRUE(ranges[0].read_hints.ann_search_results->block_coords.empty());
    EXPECT_FALSE(ranges[1].read_hints.ann_search_results.has_value());
}

#endif
