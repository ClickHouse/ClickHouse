#include "config.h"
#if USE_DISKANN

#include <gtest/gtest.h>

#include <Storages/MergeTree/ANNIndex/ANNGroupCoverage.h>
#include <Storages/MergeTree/ANNIndex/ANNIndexGroup.h>
#include <Storages/MergeTree/ANNIndex/ANNIndexManager.h>
#include <Storages/MergeTree/ANNIndex/DiskANNIndexSearcherAdapter.h>
#include <Storages/MergeTree/ANNIndex/ANNIndexTableMeta.h>
#include <Storages/MergeTree/ANNIndex/PartRowId.h>

#include <Disks/DiskLocal.h>
#include <Disks/IDisk.h>
#include <Disks/SingleDiskVolume.h>
#include <IO/WriteBufferFromFile.h>

#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
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
           / ("clickhouse-ann-manager-" + tag + "-"
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

VolumePtr makeLocalVolume(const fs::path & root, const std::string & tag)
{
    auto disk = std::make_shared<DiskLocal>("ann_mgr_disk_" + tag, root.string() + "/");
    return std::make_shared<SingleDiskVolume>("ann_mgr_vol_" + tag, disk);
}

ANNIndexShapeFingerprint makeShape(UInt32 dim = 4)
{
    ANNIndexShapeFingerprint s;
    s.dim = dim;
    s.metric = 0;
    s.algorithm = "diskann";
    s.params_hash = 0xABCDEFull;
    return s;
}

ANNIndexManager::Config makeConfig(const VolumePtr & volume, const ANNIndexShapeFingerprint & shape,
                                   UInt64 hash_seed = 0x1234u, const std::string & rel = "anns")
{
    ANNIndexManager::Config cfg;
    cfg.volume = volume;
    cfg.relative_root_path = rel;
    cfg.shape = shape;
    cfg.hash_algo = "sipHash64";
    cfg.hash_seed = hash_seed;
    DiskANNSearchOptions disk_defaults;
    disk_defaults.num_threads = 1;
    disk_defaults.search_io_limit = 1;
    disk_defaults.num_nodes_to_cache = 0;
    disk_defaults.default_search_list_size = 8;
    disk_defaults.default_beam_width = 1;
    cfg.search_defaults = std::make_shared<DiskANNSearchDefaults>(disk_defaults);
    cfg.log = getLogger("gtest_ann_index_manager");
    return cfg;
}

/// In-memory stand-in for `ANNIndexGroup` that bypasses the FFI searcher. All virtuals are
/// overridden to return preset state so the manager's lifecycle and merging logic can be
/// exercised without touching real index artefacts.
class FakeANNIndexGroup : public ANNIndexGroup
{
public:
    FakeANNIndexGroup(
        ANNIndexShapeFingerprint shape_,
        UInt64 hash_seed_,
        std::string group_dir_,
        ANNGroupCoverage coverage_)
        : ANNIndexGroup(TestOnlyTag{}, std::move(shape_), hash_seed_, std::move(coverage_))
        , fake_group_dir(std::move(group_dir_))
    {
    }

    std::vector<SearchHit> search(const float *, size_t, size_t k, const ANNSearchOverrides &) const override
    {
        if (k == 0)
            return {};
        /// Return up to `k` preset hits, preserving the order configured by the test.
        std::vector<SearchHit> out;
        out.reserve(std::min(k, preset_hits.size()));
        for (size_t i = 0; i < preset_hits.size() && i < k; ++i)
            out.push_back(preset_hits[i]);
        return out;
    }

    PartRowId lookup(UInt32 internal_id) const override
    {
        auto it = preset_map.find(internal_id);
        if (it == preset_map.end())
            return PartRowId{};
        return it->second;
    }

    bool containsPart(UInt64 partition_hash, UInt64 min_block, UInt64 max_block) const override
    {
        /// Delegate to the real coverage structure embedded in the base class for parity
        /// with the production implementation.
        return ANNIndexGroup::containsPart(partition_hash, min_block, max_block);
    }

    std::string getGroupDir() const override { return fake_group_dir; }

    void rebindStorage(ANNGroupStoragePtr /*new_storage*/) override
    {
        /// The fake carries no real storage, so the rebind performed by the manager during
        /// shape-change invalidation is a no-op. The test only checks the group-dir string
        /// through `getGroupDir`.
    }

    std::vector<SearchHit> preset_hits;
    std::unordered_map<UInt32, PartRowId> preset_map;
    std::string fake_group_dir;
};

std::shared_ptr<FakeANNIndexGroup> makeFakeGroup(
    const ANNIndexShapeFingerprint & shape,
    UInt64 hash_seed,
    const std::string & group_dir,
    UInt64 partition_hash,
    UInt64 min_block,
    UInt64 max_block)
{
    ANNGroupCoverage cov;
    cov.addPart(partition_hash, min_block, max_block);
    return std::make_shared<FakeANNIndexGroup>(shape, hash_seed, group_dir, std::move(cov));
}

PartRowId makeRowId(UInt64 ph, UInt64 bn, UInt64 bo)
{
    return PartRowId{ph, bn, bo};
}

/// Write a hand-crafted `meta.json` to `<root>/meta.json`. Used by the shape-mismatch /
/// malformed-meta fixtures.
void writeTableMetaJson(const fs::path & path, UInt32 dim, UInt8 metric,
                        UInt64 params_hash, UInt64 hash_seed)
{
    std::ostringstream oss;
    oss << "{\n"
        << "  \"version\": 1,\n"
        << "  \"shape\": {\n"
        << "    \"dim\": " << dim << ",\n"
        << "    \"metric\": " << static_cast<UInt32>(metric) << ",\n"
        << "    \"algorithm\": \"diskann\",\n"
        << R"(    "params_hash": "0x)" << std::hex << params_hash << std::dec << "\"\n"
        << "  },\n"
        << R"(  "hash_algo": "sipHash64",)" << "\n"
        << R"(  "hash_seed": "0x)" << std::hex << hash_seed << std::dec << "\"\n"
        << "}\n";

    std::ofstream out(path);
    out << oss.str();
}

}

/// ------------------------------- tests -------------------------------

TEST(ANNIndexManagerTest, ConstructorInitializesEmpty)
{
    TempDirScope scope("ctor");
    auto vol = makeLocalVolume(scope.path, "ctor");
    ANNIndexManager mgr(makeConfig(vol, makeShape()));

    auto snap = mgr.getActiveSnapshot();
    ASSERT_TRUE(snap);
    EXPECT_TRUE(snap->empty());
    EXPECT_TRUE(mgr.getRetiredGroupDirs().empty());
}

TEST(ANNIndexManagerTest, RegisterGroupAppendsToActive)
{
    TempDirScope scope("reg");
    auto vol = makeLocalVolume(scope.path, "reg");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape, 0x1u));

    for (int i = 0; i < 3; ++i)
    {
        auto g = makeFakeGroup(shape, 0x1u, "ann_" + std::to_string(i), 0x100, 0, 10);
        mgr.registerGroup(std::move(g));
    }

    auto snap = mgr.getActiveSnapshot();
    EXPECT_EQ(snap->size(), 3u);
}

TEST(ANNIndexManagerTest, RegisterGroupIsAtomicMultithreaded)
{
    TempDirScope scope("mt");
    auto vol = makeLocalVolume(scope.path, "mt");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape, 0x1u));

    constexpr int num_threads = 100;
    std::vector<std::thread> ths;
    ths.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i)
    {
        ths.emplace_back([&, i]()
        {
            auto g = makeFakeGroup(shape, 0x1u, "ann_" + std::to_string(i), 0x100 + i, 0, 10);
            mgr.registerGroup(std::move(g));
        });
    }
    for (auto & t : ths)
        t.join();

    auto snap = mgr.getActiveSnapshot();
    EXPECT_EQ(snap->size(), static_cast<size_t>(num_threads));
}

TEST(ANNIndexManagerTest, InvalidateAllForShapeChangeClearsActive)
{
    TempDirScope scope("inv_all");
    auto vol = makeLocalVolume(scope.path, "inv_all");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape));

    /// Materialise the directories the manager will rename — otherwise the underlying
    /// `moveDirectory` on `DiskLocal` would fail with "source does not exist".
    const auto anns_root = scope.path / "anns";
    fs::create_directories(anns_root);
    for (int i = 0; i < 3; ++i)
    {
        fs::create_directories(anns_root / ("ann_" + std::to_string(i)));
        auto g = makeFakeGroup(shape, mgr.getHashSeed(), "ann_" + std::to_string(i), 1, 0, 10);
        mgr.registerGroup(std::move(g));
    }

    mgr.invalidateAllGroupsForShapeChange();

    EXPECT_TRUE(mgr.getActiveSnapshot()->empty());
    auto retired = mgr.getRetiredGroupDirs();
    EXPECT_EQ(retired.size(), 3u);

    /// Each retired entry must start with the deleting prefix, and the corresponding directory
    /// must have been renamed on disk.
    for (const auto & r : retired)
    {
        EXPECT_TRUE(std::string_view(r).starts_with("deleting_ann_"));
        EXPECT_TRUE(fs::exists(anns_root / r));
    }

    /// The active directories must be gone.
    for (int i = 0; i < 3; ++i)
        EXPECT_FALSE(fs::exists(anns_root / ("ann_" + std::to_string(i))));

    /// `meta.json` should have been written with the current shape.
    auto meta = ANNIndexTableMeta::loadOrEmpty(vol, "anns");
    EXPECT_EQ(meta.shape, shape);
}

TEST(ANNIndexManagerTest, InvalidateForMutationRemovesOnlyCoveringGroups)
{
    TempDirScope scope("inv_mut");
    auto vol = makeLocalVolume(scope.path, "inv_mut");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape, 0xABCDu));

    const auto anns_root = scope.path / "anns";
    fs::create_directories(anns_root / "ann_A");
    fs::create_directories(anns_root / "ann_B");

    /// Group A covers partition_hash=1 blocks [0,100]; Group B covers partition_hash=2 [0,50].
    mgr.registerGroup(makeFakeGroup(shape, mgr.getHashSeed(), "ann_A", 1, 0, 100));
    mgr.registerGroup(makeFakeGroup(shape, mgr.getHashSeed(), "ann_B", 2, 0, 50));

    /// Affected range: partition_hash=1, blocks [50, 60] — overlaps A only.
    mgr.invalidateGroupsForRanges({{1, 50, 60}});

    auto snap = mgr.getActiveSnapshot();
    ASSERT_EQ(snap->size(), 1u);
    EXPECT_EQ(snap->groups[0]->getGroupDir(), "ann_B");

    auto retired = mgr.getRetiredGroupDirs();
    ASSERT_EQ(retired.size(), 1u);
    EXPECT_EQ(retired.front(), "deleting_ann_A");

    /// On-disk rename must have happened.
    EXPECT_FALSE(fs::exists(anns_root / "ann_A"));
    EXPECT_TRUE(fs::exists(anns_root / "deleting_ann_A"));
    EXPECT_TRUE(fs::exists(anns_root / "ann_B"));
}

TEST(ANNIndexManagerTest, InvalidateForMutationAnotherPartitionKeepsAll)
{
    TempDirScope scope("inv_other");
    auto vol = makeLocalVolume(scope.path, "inv_other");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape));

    const auto anns_root = scope.path / "anns";
    fs::create_directories(anns_root / "ann_A");
    fs::create_directories(anns_root / "ann_B");

    mgr.registerGroup(makeFakeGroup(shape, mgr.getHashSeed(), "ann_A", 1, 0, 100));
    mgr.registerGroup(makeFakeGroup(shape, mgr.getHashSeed(), "ann_B", 2, 0, 50));

    /// Affected range targets a completely unrelated partition.
    mgr.invalidateGroupsForRanges({{3, 0, 10}});

    EXPECT_EQ(mgr.getActiveSnapshot()->size(), 2u);
    EXPECT_TRUE(mgr.getRetiredGroupDirs().empty());

    /// Directories are left untouched.
    EXPECT_TRUE(fs::exists(anns_root / "ann_A"));
    EXPECT_TRUE(fs::exists(anns_root / "ann_B"));
}

TEST(ANNIndexManagerTest, InvalidateForMutationEmptyNoop)
{
    TempDirScope scope("inv_empty");
    auto vol = makeLocalVolume(scope.path, "inv_empty");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape));

    mgr.registerGroup(makeFakeGroup(shape, mgr.getHashSeed(), "ann_A", 1, 0, 10));

    mgr.invalidateGroupsForMutation({});

    EXPECT_EQ(mgr.getActiveSnapshot()->size(), 1u);
    EXPECT_TRUE(mgr.getRetiredGroupDirs().empty());
}

TEST(ANNIndexManagerTest, IsPartCoveredNullPartIsFalse)
{
    TempDirScope scope("cov_null");
    auto vol = makeLocalVolume(scope.path, "cov_null");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape));

    mgr.registerGroup(makeFakeGroup(shape, mgr.getHashSeed(), "ann_A", 1, 0, 10));

    EXPECT_FALSE(mgr.isPartCovered(nullptr));
}

TEST(ANNIndexManagerTest, IsPartCoveredEmptySnapshotIsFalse)
{
    TempDirScope scope("cov_empty");
    auto vol = makeLocalVolume(scope.path, "cov_empty");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape));

    EXPECT_FALSE(mgr.isPartCovered(nullptr));
    EXPECT_FALSE(mgr.isRangeCovered(1, 0, 10));
}

TEST(ANNIndexManagerTest, IsRangeCoveredTrueWhenAnyGroupCovers)
{
    TempDirScope scope("cov_true");
    auto vol = makeLocalVolume(scope.path, "cov_true");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape));

    mgr.registerGroup(makeFakeGroup(shape, mgr.getHashSeed(), "ann_A", 1, 0, 100));
    mgr.registerGroup(makeFakeGroup(shape, mgr.getHashSeed(), "ann_B", 2, 0, 50));

    EXPECT_TRUE(mgr.isRangeCovered(1, 10, 20));
    EXPECT_TRUE(mgr.isRangeCovered(2, 0, 50));
    EXPECT_FALSE(mgr.isRangeCovered(3, 0, 10));
    /// Out of range in the covered partition.
    EXPECT_FALSE(mgr.isRangeCovered(2, 60, 70));
}

TEST(ANNIndexManagerTest, IsRangeCoveredConsistentHashSeed)
{
    TempDirScope scope("cov_seed");
    auto vol = makeLocalVolume(scope.path, "cov_seed");
    auto shape = makeShape();

    const UInt64 seed = 0xFEED;
    ANNIndexManager mgr(makeConfig(vol, shape, seed));
    const UInt64 partition_hash = mgr.hashPartitionId("20260422");
    mgr.registerGroup(makeFakeGroup(shape, seed, "ann_A", partition_hash, 0, 10));
    EXPECT_TRUE(mgr.isRangeCovered(partition_hash, 0, 10));

    /// A second manager with a different seed computes a different hash for the same
    /// partition id — the group registered under the first manager's hash is irrelevant.
    ANNIndexManager mgr2(makeConfig(vol, shape, seed ^ 0xFFull, "anns2"));
    const UInt64 partition_hash_b = mgr2.hashPartitionId("20260422");
    EXPECT_NE(partition_hash, partition_hash_b);
    EXPECT_FALSE(mgr2.isRangeCovered(partition_hash, 0, 10));
}

TEST(ANNIndexManagerTest, HashPartitionIdConsistency)
{
    TempDirScope scope("hash");
    auto vol = makeLocalVolume(scope.path, "hash");
    auto shape = makeShape();
    ANNIndexManager mgr_a(makeConfig(vol, shape, 0xDEADBEEFu, "anns_a"));
    ANNIndexManager mgr_b(makeConfig(vol, shape, 0xDEADBEEFu, "anns_b"));

    const String pid = "20260422";
    const UInt64 h1 = mgr_a.hashPartitionId(pid);
    const UInt64 h2 = mgr_b.hashPartitionId(pid);
    EXPECT_EQ(h1, h2);
    /// Same manager returns the same value on repeated calls.
    EXPECT_EQ(mgr_a.hashPartitionId(pid), h1);

    ANNIndexManager mgr_c(makeConfig(vol, shape, 0xCAFEu, "anns_c"));
    EXPECT_NE(mgr_c.hashPartitionId(pid), h1);
}

TEST(ANNIndexManagerTest, HashPartitionIdMatchesSipHash64Keyed)
{
    TempDirScope scope("hash2");
    auto vol = makeLocalVolume(scope.path, "hash2");
    auto shape = makeShape();
    const UInt64 seed = 0x11223344u;
    ANNIndexManager mgr(makeConfig(vol, shape, seed));

    const String pid = "p1";
    SipHash h(seed, 0);
    h.update(pid.data(), pid.size());
    EXPECT_EQ(mgr.hashPartitionId(pid), h.get64());
}

TEST(ANNIndexManagerTest, SearchEmptyActiveReturnsEmpty)
{
    TempDirScope scope("search_empty");
    auto vol = makeLocalVolume(scope.path, "search_empty");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape));

    float q[4] = {0, 0, 0, 0};
    auto hits = mgr.search(q, 4, /*k=*/5);
    EXPECT_TRUE(hits.empty());
}

TEST(ANNIndexManagerTest, SearchZeroKReturnsEmpty)
{
    TempDirScope scope("search_zero_k");
    auto vol = makeLocalVolume(scope.path, "search_zero_k");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape));

    auto g = makeFakeGroup(shape, mgr.getHashSeed(), "ann_0", 1, 0, 0);
    g->preset_hits = {{0, 1.0f}};
    g->preset_map = {{0, makeRowId(1, 2, 3)}};
    mgr.registerGroup(std::move(g));

    float q[4] = {0, 0, 0, 0};
    auto hits = mgr.search(q, 4, /*k=*/0);
    EXPECT_TRUE(hits.empty());
}

TEST(ANNIndexManagerTest, SearchMergesTopKFromAllGroups)
{
    TempDirScope scope("search_merge");
    auto vol = makeLocalVolume(scope.path, "search_merge");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape));

    auto g0 = makeFakeGroup(shape, mgr.getHashSeed(), "ann_0", 10, 0, 0);
    g0->preset_hits = {{0, 1.0f}, {1, 3.0f}};
    g0->preset_map = {{0, makeRowId(10, 0, 0)}, {1, makeRowId(10, 0, 1)}};

    auto g1 = makeFakeGroup(shape, mgr.getHashSeed(), "ann_1", 20, 0, 0);
    g1->preset_hits = {{0, 2.0f}, {1, 4.0f}};
    g1->preset_map = {{0, makeRowId(20, 0, 0)}, {1, makeRowId(20, 0, 1)}};

    mgr.registerGroup(std::move(g0));
    mgr.registerGroup(std::move(g1));

    float q[4] = {0, 0, 0, 0};
    auto hits = mgr.search(q, 4, /*k=*/2, /*rescoring_factor=*/1);
    ASSERT_EQ(hits.size(), 2u);
    EXPECT_FLOAT_EQ(hits[0].distance, 1.0f);
    EXPECT_EQ(hits[0].row, makeRowId(10, 0, 0));
    EXPECT_FLOAT_EQ(hits[1].distance, 2.0f);
    EXPECT_EQ(hits[1].row, makeRowId(20, 0, 0));
}

TEST(ANNIndexManagerTest, SearchRespectsRescoringFactor)
{
    TempDirScope scope("search_rescore");
    auto vol = makeLocalVolume(scope.path, "search_rescore");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape));

    auto g0 = makeFakeGroup(shape, mgr.getHashSeed(), "ann_0", 10, 0, 0);
    g0->preset_hits = {{0, 1.0f}, {1, 2.0f}, {2, 3.0f}, {3, 4.0f}, {4, 5.0f}, {5, 6.0f}, {6, 7.0f}};
    for (UInt32 i = 0; i < 7; ++i)
        g0->preset_map[i] = makeRowId(10, 0, i);

    auto g1 = makeFakeGroup(shape, mgr.getHashSeed(), "ann_1", 20, 0, 0);
    g1->preset_hits = {{0, 1.5f}, {1, 2.5f}, {2, 3.5f}, {3, 4.5f}, {4, 5.5f}, {5, 6.5f}, {6, 7.5f}};
    for (UInt32 i = 0; i < 7; ++i)
        g1->preset_map[i] = makeRowId(20, 0, i);

    mgr.registerGroup(std::move(g0));
    mgr.registerGroup(std::move(g1));

    float q[4] = {0, 0, 0, 0};
    auto hits = mgr.search(q, 4, /*k=*/2, /*rescoring_factor=*/3);
    /// target_k = 6; each group returns at most 6 hits (we provided 7); merged up to 12,
    /// truncated to 6.
    EXPECT_EQ(hits.size(), 6u);
    /// Verify distance is globally ascending.
    for (size_t i = 1; i < hits.size(); ++i)
        EXPECT_LE(hits[i - 1].distance, hits[i].distance);
}

TEST(ANNIndexManagerTest, CreateGroupStorageHonoursSuppliedTmpDir)
{
    TempDirScope scope("tmp_store");
    auto vol = makeLocalVolume(scope.path, "tmp_store");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape));

    auto r1 = mgr.tryReserveBuildSlot();
    ASSERT_TRUE(r1);
    EXPECT_TRUE(r1->tmpDir().starts_with("tmp_ann_"));

    auto s1 = mgr.createGroupStorage(r1->tmpDir());
    ASSERT_TRUE(s1);
    EXPECT_EQ(s1->getGroupDir(), r1->tmpDir());

    /// While the reservation is held, a second `tryReserveBuildSlot` must fail (one slot
    /// only). Drop r1 and try again to verify the slot is reusable.
    EXPECT_FALSE(mgr.tryReserveBuildSlot());
    r1.reset();
    auto r2 = mgr.tryReserveBuildSlot();
    ASSERT_TRUE(r2);
    EXPECT_NE(s1->getGroupDir(), r2->tmpDir());
}

TEST(ANNIndexManagerTest, OpenGroupStorageReturnsCorrectDir)
{
    TempDirScope scope("open_store");
    auto vol = makeLocalVolume(scope.path, "open_store");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape));

    fs::create_directories(scope.path / "anns" / "ann_foo");
    auto s = mgr.openGroupStorage("ann_foo");
    ASSERT_TRUE(s);
    EXPECT_EQ(s->getGroupDir(), "ann_foo");
    EXPECT_TRUE(s->exists());
}

TEST(ANNIndexManagerTest, ListGroupDirsFiltersByPrefixAndDirectoriesOnly)
{
    TempDirScope scope("list");
    auto vol = makeLocalVolume(scope.path, "list");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape));

    fs::create_directories(scope.path / "anns" / "ann_a");
    fs::create_directories(scope.path / "anns" / "tmp_ann_b");
    fs::create_directories(scope.path / "anns" / "deleting_ann_c");
    fs::create_directories(scope.path / "anns" / "something_else");
    fs::create_directories(scope.path / "anns" / "ann_d");
    /// A regular file whose name *starts* with `ann_` — must be ignored because it is not a
    /// directory.
    std::ofstream(scope.path / "anns" / "ann_x.txt").put('x');

    auto dirs = mgr.listGroupDirsOnDisk();
    /// Expected: sorted list of all ANN-prefixed directories.
    std::vector<std::string> expected = {"ann_a", "ann_d", "deleting_ann_c", "tmp_ann_b"};
    EXPECT_EQ(dirs, expected);
}

TEST(ANNIndexManagerTest, ListGroupDirsOnMissingRootReturnsEmpty)
{
    TempDirScope scope("list_missing");
    auto vol = makeLocalVolume(scope.path, "list_missing");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape));
    /// No `anns/` directory created.
    EXPECT_TRUE(mgr.listGroupDirsOnDisk().empty());
}

TEST(ANNIndexManagerTest, LoadFromDiskNoMetaYieldsEmptyAndWritesMeta)
{
    TempDirScope scope("load_empty");
    auto vol = makeLocalVolume(scope.path, "load_empty");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape));

    EXPECT_NO_THROW(mgr.loadFromDisk());
    EXPECT_TRUE(mgr.getActiveSnapshot()->empty());
    EXPECT_TRUE(mgr.getRetiredGroupDirs().empty());

    /// On first load the manager writes a `meta.json` that reflects its configured shape.
    auto meta = ANNIndexTableMeta::loadOrEmpty(vol, "anns");
    EXPECT_EQ(meta.shape, shape);
}

TEST(ANNIndexManagerTest, LoadFromDiskShapeMismatchRenamesActiveToDeleting)
{
    TempDirScope scope("load_shape");
    auto vol = makeLocalVolume(scope.path, "load_shape");
    auto shape = makeShape(/*dim=*/128);
    ANNIndexManager mgr(makeConfig(vol, shape, 0xAAAAu));

    const auto anns_root = scope.path / "anns";
    fs::create_directories(anns_root);

    /// Plant two active-looking directories whose shape does not match the manager's.
    fs::create_directories(anns_root / "ann_a");
    fs::create_directories(anns_root / "ann_b");

    /// Write a `meta.json` whose `shape.dim` differs from `config.shape.dim`.
    writeTableMetaJson(anns_root / "meta.json",
                       /*dim=*/64, /*metric=*/0, /*params_hash=*/0x1,
                       /*hash_seed=*/0xAAAA);

    EXPECT_NO_THROW(mgr.loadFromDisk());
    EXPECT_TRUE(mgr.getActiveSnapshot()->empty());

    auto retired = mgr.getRetiredGroupDirs();
    ASSERT_EQ(retired.size(), 2u);
    EXPECT_EQ(retired[0], "deleting_ann_a");
    EXPECT_EQ(retired[1], "deleting_ann_b");

    /// On-disk directories have been renamed.
    EXPECT_FALSE(fs::exists(anns_root / "ann_a"));
    EXPECT_FALSE(fs::exists(anns_root / "ann_b"));
    EXPECT_TRUE(fs::exists(anns_root / "deleting_ann_a"));
    EXPECT_TRUE(fs::exists(anns_root / "deleting_ann_b"));

    /// `meta.json` has been rewritten with the new shape.
    auto meta = ANNIndexTableMeta::loadOrEmpty(vol, "anns");
    EXPECT_EQ(meta.shape, shape);
}

TEST(ANNIndexManagerTest, LoadFromDiskPicksUpExistingDeletingDirs)
{
    TempDirScope scope("load_deleting");
    auto vol = makeLocalVolume(scope.path, "load_deleting");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape, 0xBEEFu));

    const auto anns_root = scope.path / "anns";
    fs::create_directories(anns_root / "deleting_ann_x");
    fs::create_directories(anns_root / "deleting_ann_y");

    writeTableMetaJson(anns_root / "meta.json",
                       shape.dim, shape.metric, shape.params_hash, 0xBEEFu);

    EXPECT_NO_THROW(mgr.loadFromDisk());
    EXPECT_TRUE(mgr.getActiveSnapshot()->empty());

    auto retired = mgr.getRetiredGroupDirs();
    ASSERT_EQ(retired.size(), 2u);
    EXPECT_EQ(retired[0], "deleting_ann_x");
    EXPECT_EQ(retired[1], "deleting_ann_y");
}

TEST(ANNIndexManagerTest, LoadFromDiskGroupLoadFailureRetiresGroup)
{
    TempDirScope scope("load_group_fail");
    auto vol = makeLocalVolume(scope.path, "load_group_fail");
    auto shape = makeShape();
    const UInt64 seed = 0xBEEFu;
    ANNIndexManager mgr(makeConfig(vol, shape, seed));

    const auto anns_root = scope.path / "anns";
    fs::create_directories(anns_root);
    /// An empty `ann_x/` directory: `ANNIndexGroup::load` will fail because `meta.json` is
    /// missing, and the manager must tolerate the failure by retiring the directory in-memory
    /// (without touching the disk — the directory stays where it is until GC).
    fs::create_directories(anns_root / "ann_x");
    writeTableMetaJson(anns_root / "meta.json",
                       shape.dim, shape.metric, shape.params_hash, seed);

    EXPECT_NO_THROW(mgr.loadFromDisk());
    EXPECT_TRUE(mgr.getActiveSnapshot()->empty());
    auto retired = mgr.getRetiredGroupDirs();
    ASSERT_EQ(retired.size(), 1u);
    EXPECT_EQ(retired.front(), "ann_x");
}

TEST(ANNIndexManagerTest, BuildSlotCASExclusivity)
{
    TempDirScope scope("buildslot");
    auto vol = makeLocalVolume(scope.path, "buildslot");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape));

    {
        auto r1 = mgr.tryReserveBuildSlot();
        ASSERT_TRUE(r1);
        EXPECT_FALSE(mgr.tryReserveBuildSlot());
    }   /// r1 destructor releases the slot

    {
        auto r2 = mgr.tryReserveBuildSlot();
        ASSERT_TRUE(r2);
    }
}

TEST(ANNIndexManagerTest, BuildReservationCommitReleasesSlot)
{
    TempDirScope scope("buildslot_commit");
    auto vol = makeLocalVolume(scope.path, "buildslot_commit");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape));

    auto r = mgr.tryReserveBuildSlot();
    ASSERT_TRUE(r);
    const std::string tmp_dir = r->tmpDir();
    EXPECT_TRUE(mgr.isPathKnown(tmp_dir));

    r->commit();
    EXPECT_FALSE(mgr.isPathKnown(tmp_dir));

    /// After commit, slot is released so a new reservation succeeds.
    auto r2 = mgr.tryReserveBuildSlot();
    ASSERT_TRUE(r2);
    EXPECT_NE(r2->tmpDir(), tmp_dir);
}

TEST(ANNIndexManagerTest, BuildReservationRollbackOnDestroy)
{
    TempDirScope scope("buildslot_rollback");
    auto vol = makeLocalVolume(scope.path, "buildslot_rollback");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape));

    std::string tmp_dir;
    {
        auto r = mgr.tryReserveBuildSlot();
        ASSERT_TRUE(r);
        tmp_dir = r->tmpDir();
        EXPECT_TRUE(mgr.isPathKnown(tmp_dir));
    }   /// r dtor without commit → rollback
    EXPECT_FALSE(mgr.isPathKnown(tmp_dir));

    /// Slot is reusable after rollback.
    auto r2 = mgr.tryReserveBuildSlot();
    ASSERT_TRUE(r2);
}

TEST(ANNIndexManagerTest, IsPathKnownCoversInFlightActiveAndRetired)
{
    TempDirScope scope("path_known");
    auto vol = makeLocalVolume(scope.path, "path_known");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape));

    /// Unknown name: false.
    EXPECT_FALSE(mgr.isPathKnown("ann_unknown"));

    /// In-flight: true.
    auto r = mgr.tryReserveBuildSlot();
    ASSERT_TRUE(r);
    EXPECT_TRUE(mgr.isPathKnown(r->tmpDir()));
    r->commit();

    /// Active: true.
    const auto anns_root = scope.path / "anns";
    fs::create_directories(anns_root / "ann_active");
    mgr.registerGroup(makeFakeGroup(shape, mgr.getHashSeed(), "ann_active", 1, 0, 10));
    EXPECT_TRUE(mgr.isPathKnown("ann_active"));

    /// Retired: true. Triggered via shape-change invalidation.
    mgr.invalidateAllGroupsForShapeChange();
    EXPECT_TRUE(mgr.isPathKnown("deleting_ann_active"));
    EXPECT_FALSE(mgr.isPathKnown("ann_active"));
}

TEST(ANNIndexManagerTest, RetiredMetaGettersBehaveWellOnUnknownDir)
{
    TempDirScope scope("retired_meta");
    auto vol = makeLocalVolume(scope.path, "retired_meta");
    auto shape = makeShape();
    ANNIndexManager mgr(makeConfig(vol, shape));

    EXPECT_EQ(mgr.getRetiredAt("nonexistent"), std::chrono::steady_clock::time_point{});
    EXPECT_EQ(mgr.getRetiredGroupPtr("nonexistent"), nullptr);
    /// forget is a no-op on unknown keys.
    mgr.forgetRetiredGroup("nonexistent");

    /// After retiring one group it becomes visible, and `forget` removes it again.
    const auto anns_root = scope.path / "anns";
    fs::create_directories(anns_root / "ann_A");
    mgr.registerGroup(makeFakeGroup(shape, mgr.getHashSeed(), "ann_A", 1, 0, 10));
    mgr.invalidateAllGroupsForShapeChange();

    auto retired = mgr.getRetiredGroupDirs();
    ASSERT_EQ(retired.size(), 1u);
    EXPECT_EQ(retired.front(), "deleting_ann_A");
    EXPECT_NE(mgr.getRetiredGroupPtr("deleting_ann_A"), nullptr);
    EXPECT_NE(mgr.getRetiredAt("deleting_ann_A"), std::chrono::steady_clock::time_point{});

    mgr.forgetRetiredGroup("deleting_ann_A");
    EXPECT_TRUE(mgr.getRetiredGroupDirs().empty());
}

#endif
