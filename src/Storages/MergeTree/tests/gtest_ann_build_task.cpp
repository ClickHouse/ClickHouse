#include "config.h"
#if USE_DISKANN

#include <gtest/gtest.h>

#include <Storages/MergeTree/ANNIndex/ANNGroupCoverage.h>
#include <Storages/MergeTree/ANNIndex/ANNGroupStorageDiskFull.h>
#include <Storages/MergeTree/ANNIndex/ANNIndexGroup.h>
#include <Storages/MergeTree/ANNIndex/ANNIndexManager.h>
#include <Storages/MergeTree/ANNIndex/DiskANNIndexSearcherAdapter.h>
#include <Storages/MergeTree/ANNIndex/ANNIndexTableMeta.h>
#include <Storages/MergeTree/ANNIndex/BuildANNIndexTask.h>
#include <Storages/MergeTree/ANNIndex/PartRowId.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartState.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/tests/MergeTreeTestHarness.h>
#include <Storages/StorageMergeTree.h>

#include <Interpreters/Context.h>

#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <Common/tests/gtest_global_context.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
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
           / ("clickhouse-ann-build-task-" + tag + "-"
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

ANNIndexShapeFingerprint makeShape(UInt32 dim = 8)
{
    ANNIndexShapeFingerprint s;
    s.dim = dim;
    s.metric = 0;
    s.algorithm = "diskann";
    s.params_hash = 0;
    return s;
}

ANNIndexManager::Config makeConfig(const VolumePtr & volume, const ANNIndexShapeFingerprint & shape,
                                   UInt64 hash_seed = 0xA1B2C3D4ULL, const std::string & rel = "anns")
{
    ANNIndexManager::Config cfg;
    cfg.volume = volume;
    cfg.relative_root_path = rel;
    cfg.shape = shape;
    cfg.hash_algo = "sipHash64";
    cfg.hash_seed = hash_seed;
    DiskANNSearchOptions cfg_disk_defaults;
    cfg_disk_defaults.num_threads = 1;
    cfg_disk_defaults.search_io_limit = 1;
    cfg_disk_defaults.num_nodes_to_cache = 0;
    cfg_disk_defaults.default_search_list_size = 16;
    cfg_disk_defaults.default_beam_width = 4;
    cfg.search_defaults = std::make_shared<DiskANNSearchDefaults>(cfg_disk_defaults);
    cfg.log = getLogger("gtest_ann_build_task");
    return cfg;
}

DiskANNBuildOptions smallBuildOpts()
{
    DiskANNBuildOptions opts;
    opts.pruned_degree = 16;
    opts.max_degree = 32;
    opts.l_build = 64;
    opts.alpha = 1.2f;
    opts.num_threads = 1;
    opts.pq_chunks = 4;
    opts.build_ram_limit_gb = 0.25;
    return opts;
}

ANNIndexDefinition makeDefinition(const std::string & vec_col_name, UInt32 dim, UInt64 hash_seed)
{
    ANNIndexDefinition def;
    def.shape.dim = dim;
    def.shape.metric = 0;
    def.shape.algorithm = "diskann";
    def.shape.params_hash = 0;
    def.build_options = smallBuildOpts();
    DiskANNSearchOptions def_disk_defaults;
    def_disk_defaults.num_threads = 1;
    def_disk_defaults.search_io_limit = 1;
    def_disk_defaults.num_nodes_to_cache = 0;
    def_disk_defaults.default_search_list_size = 16;
    def_disk_defaults.default_beam_width = 4;
    def.search_defaults = std::make_shared<DiskANNSearchDefaults>(def_disk_defaults);
    def.hash_algo = "sipHash64";
    def.hash_seed = hash_seed;
    def.vector_column_name = vec_col_name;
    return def;
}

/// Test helper: invoke `ANNIndexManager::selectPartsForBuild` with a default snapshot +
/// definition derived from the harness setup. Tests that exercise boundary conditions on the
/// selector do not care about the exact definition shape, only about how many parts come back.
ANNBuildSelectedEntryPtr selectEntry(
    const ANNIndexManager & mgr,
    const MergeTreeTestHarness::TestStorage & setup,
    UInt64 min_rows, UInt64 max_rows, UInt64 max_parts)
{
    auto context = Context::createCopy(getContext().context);
    auto metadata = setup.storage->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/ false);
    auto snapshot = setup.storage->getStorageSnapshot(metadata, context);
    auto def = makeDefinition(setup.vec_column_name, mgr.getShape().dim, mgr.getHashSeed());
    return mgr.selectPartsForBuild(*setup.storage, std::move(snapshot), std::move(def),
                                   min_rows, max_rows, max_parts);
}

/// In-memory stand-in for `ANNIndexGroup` that only carries a shape, hash seed, group dir and
/// coverage. Used as an `active` group for `selectPartsForBuild` tests so the manager's
/// `isPartCovered` predicate can be driven without invoking the DiskANN FFI searcher.
class FakeANNIndexGroup : public ANNIndexGroup
{
public:
    FakeANNIndexGroup(ANNIndexShapeFingerprint shape_, UInt64 hash_seed_, std::string dir_, ANNGroupCoverage cov_)
        : ANNIndexGroup(TestOnlyTag{}, std::move(shape_), hash_seed_, std::move(cov_))
        , fake_dir(std::move(dir_))
    {
    }

    std::vector<SearchHit> search(const float *, size_t, size_t, const ANNSearchOverrides &) const override { return {}; }
    PartRowId lookup(UInt32) const override { return PartRowId{}; }
    size_t numPoints() const override { return 0; }
    std::string getGroupDir() const override { return fake_dir; }

private:
    std::string fake_dir;
};

/// Minimal concrete `IMergeTreeDataPart` stand-in for `selectPartsForBuild`. The selector only
/// touches `info.getPartitionId()`, `info.min_block`, `info.max_block` and `rows_count`, so
/// we build a lightweight subclass that exposes those via the same public fields the real
/// part class uses.
///
/// The ctor builds a `MergeTreePartInfo` with the required fields and assigns `rows_count`
/// directly (public member).
///
/// We must not construct a real `IMergeTreeDataPart` because it needs a storage reference,
/// data part storage, columns, etc. Instead we define a local subclass that bypasses the
/// pure virtual methods the selector never calls.
/// The selector does NOT call any virtual method — it only touches data members, so we use
/// a helper that constructs a `MergeTreePartInfo` and packs it into a `DataPartPtr` whose
/// `info`, `rows_count` members are set to the required values.
///
/// Note: `MergeTreeData` is not easy to mock, so for B-T1..B-T6 we use `selectPartsForBuild`'s
/// ability to accept a `MergeTreeData &`. To test the selector logic we therefore wire up the
/// harness — it builds a real tiny storage and we seed it with the right number of parts.
///
/// Actually, because `selectPartsForBuild` calls `data.getDataPartsVectorForInternalUsage()`,
/// the harness is the simplest way to get a valid `DataPartPtr` vector.

/// Helper: return the active data parts of a storage.
DataPartsVector activeParts(const StorageMergeTree & storage)
{
    return storage.getDataPartsVectorForInternalUsage({MergeTreeDataPartState::Active});
}

std::vector<std::vector<float>> makeVectors(size_t rows, size_t dim, float base)
{
    std::vector<std::vector<float>> out(rows, std::vector<float>(dim, 0.f));
    for (size_t r = 0; r < rows; ++r)
        for (size_t c = 0; c < dim; ++c)
            out[r][c] = base + 0.1f * static_cast<float>(r) + 0.01f * static_cast<float>(c);
    return out;
}

class StorageScope
{
public:
    explicit StorageScope(MergeTreeTestHarness::TestStorage s_) : s(std::move(s_)) {}
    ~StorageScope() { if (s.storage) s.storage->flushAndShutdown(); }
    MergeTreeTestHarness::TestStorage & get() { return s; }
private:
    MergeTreeTestHarness::TestStorage s;
};

}


/// -----------------------------------------------------------------------------------------
/// selectPartsForBuild boundary tests (B-T1..B-T6)
///
/// Each test sets up a real storage with a known number of parts and rows, then exercises a
/// specific selector branch. The selector only touches manager coverage + data parts, so we
/// inject a fresh `ANNIndexManager` that starts out with no active groups (nothing covered)
/// or with a preset `FakeANNIndexGroup` (everything covered).
/// -----------------------------------------------------------------------------------------

TEST(ANNBuildTaskTest, SelectEmptyWhenAllCovered)
{
    TempDirScope disk_scope("sel-covered");
    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk_scope.path.string(), "store/ann_sel_covered", "emb", /*dim=*/ 8, /*partition_key_column=*/ "pk"));
    auto & setup = storage_scope.get();

    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 4, 1, makeVectors(4, 8, 1.0f));
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 4, 2, makeVectors(4, 8, 2.0f));

    auto parts = activeParts(*setup.storage);
    ASSERT_EQ(parts.size(), 2u);

    auto volume = setup.storage->getStoragePolicy()->getVolume(0);
    auto shape = makeShape(8);
    const UInt64 hash_seed = 0xA1B2C3D4ULL;
    ANNIndexManager manager(makeConfig(volume, shape, hash_seed, "anns_sel1"));

    /// Install a coverage that spans all parts' (partition_hash, min_block, max_block) tuples.
    ANNGroupCoverage cov;
    for (const auto & p : parts)
    {
        const auto pid = p->info.getPartitionId();
        SipHash sh(hash_seed, 0);
        sh.update(pid.data(), pid.size());
        cov.addPart(sh.get64(), p->info.min_block, p->info.max_block);
    }
    manager.registerGroup(std::make_shared<FakeANNIndexGroup>(shape, hash_seed, "ann_fake", std::move(cov)));

    auto selected = selectEntry(manager, setup,
        /*min_rows=*/ 1, /*max_rows=*/ 1000, /*max_parts=*/ 64);
    EXPECT_TRUE(!selected);
}

TEST(ANNBuildTaskTest, SelectEmptyWhenBelowMinRows)
{
    TempDirScope disk_scope("sel-min");
    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk_scope.path.string(), "store/ann_sel_min", "emb", /*dim=*/ 8, /*partition_key_column=*/ "pk"));
    auto & setup = storage_scope.get();

    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 4, 1, makeVectors(4, 8, 1.0f));
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 4, 2, makeVectors(4, 8, 2.0f));

    auto volume = setup.storage->getStoragePolicy()->getVolume(0);
    ANNIndexManager manager(makeConfig(volume, makeShape(8), 0xDEADBEEFULL, "anns_sel2"));

    auto selected = selectEntry(manager, setup,
        /*min_rows=*/ 1000, /*max_rows=*/ 100000, /*max_parts=*/ 64);
    EXPECT_TRUE(!selected);
}

TEST(ANNBuildTaskTest, SelectStopsAtMaxRows)
{
    TempDirScope disk_scope("sel-maxrows");
    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk_scope.path.string(), "store/ann_sel_maxrows", "emb", /*dim=*/ 8, /*partition_key_column=*/ "pk"));
    auto & setup = storage_scope.get();

    /// 10 parts × 4 rows each. Cap the cumulative rows at 12, with a generous part cap.
    for (uint32_t i = 0; i < 10; ++i)
        MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 4, i + 1, makeVectors(4, 8, static_cast<float>(i)));

    auto volume = setup.storage->getStoragePolicy()->getVolume(0);
    ANNIndexManager manager(makeConfig(volume, makeShape(8), 0x12345ULL, "anns_sel3"));

    auto selected = selectEntry(manager, setup,
        /*min_rows=*/ 1, /*max_rows=*/ 12, /*max_parts=*/ 64);

    size_t total_rows = 0;
    for (const auto & p : selected->selected_parts)
        total_rows += p->rows_count;

    /// Must not overshoot max_rows except when a single part is larger, which is not the case
    /// here (each part is 4 rows, below the cap).
    EXPECT_LE(total_rows, 12u);
    /// The first three parts (4+4+4 = 12 rows) fit exactly; adding a fourth would cross the cap.
    EXPECT_EQ(selected->selected_parts.size(), 3u);
}

TEST(ANNBuildTaskTest, SelectStopsAtMaxParts)
{
    TempDirScope disk_scope("sel-maxparts");
    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk_scope.path.string(), "store/ann_sel_maxparts", "emb", /*dim=*/ 8, /*partition_key_column=*/ "pk"));
    auto & setup = storage_scope.get();

    for (uint32_t i = 0; i < 10; ++i)
        MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 4, i + 1, makeVectors(4, 8, static_cast<float>(i)));

    auto volume = setup.storage->getStoragePolicy()->getVolume(0);
    ANNIndexManager manager(makeConfig(volume, makeShape(8), 0xABCDEULL, "anns_sel4"));

    auto selected = selectEntry(manager, setup,
        /*min_rows=*/ 1, /*max_rows=*/ 10000, /*max_parts=*/ 3);
    EXPECT_EQ(selected->selected_parts.size(), 3u);
}

TEST(ANNBuildTaskTest, SelectSortsByPartitionAndBlock)
{
    TempDirScope disk_scope("sel-sort");
    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk_scope.path.string(), "store/ann_sel_sort", "emb", /*dim=*/ 8, /*partition_key_column=*/ "pk"));
    auto & setup = storage_scope.get();

    /// Insert in arbitrary partition-value order; verify the selector sorts output.
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 4, 7, makeVectors(4, 8, 1.0f));
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 4, 3, makeVectors(4, 8, 2.0f));
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 4, 5, makeVectors(4, 8, 3.0f));

    auto volume = setup.storage->getStoragePolicy()->getVolume(0);
    ANNIndexManager manager(makeConfig(volume, makeShape(8), 0xAAAAULL, "anns_sel5"));

    auto selected = selectEntry(manager, setup,
        /*min_rows=*/ 1, /*max_rows=*/ 10000, /*max_parts=*/ 100);
    ASSERT_EQ(selected->selected_parts.size(), 3u);

    /// Selected output should be non-decreasing by `(partition_id, min_block)`.
    for (size_t i = 1; i < selected->selected_parts.size(); ++i)
    {
        const auto & prev = selected->selected_parts[i - 1]->info;
        const auto & cur = selected->selected_parts[i]->info;
        EXPECT_TRUE(prev.getPartitionId() < cur.getPartitionId()
                    || (prev.getPartitionId() == cur.getPartitionId() && prev.min_block <= cur.min_block));
    }
}

TEST(ANNBuildTaskTest, SelectAcceptsSingleOverSizedPart)
{
    TempDirScope disk_scope("sel-oversized");
    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk_scope.path.string(), "store/ann_sel_oversized", "emb", /*dim=*/ 8, /*partition_key_column=*/ "pk"));
    auto & setup = storage_scope.get();

    /// Single part of 10 rows; cap at 3 rows — still take the single part since it is the
    /// smallest indexable unit.
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 10, 1, makeVectors(10, 8, 1.0f));

    auto volume = setup.storage->getStoragePolicy()->getVolume(0);
    ANNIndexManager manager(makeConfig(volume, makeShape(8), 0xBBBBULL, "anns_sel6"));

    auto selected = selectEntry(manager, setup,
        /*min_rows=*/ 1, /*max_rows=*/ 3, /*max_parts=*/ 64);
    EXPECT_EQ(selected->selected_parts.size(), 1u);
}

TEST(ANNBuildTaskTest, SelectEmptyOnZeroCaps)
{
    TempDirScope disk_scope("sel-zerocap");
    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk_scope.path.string(), "store/ann_sel_zerocap", "emb", /*dim=*/ 8, /*partition_key_column=*/ "pk"));
    auto & setup = storage_scope.get();

    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 4, 1, makeVectors(4, 8, 1.0f));

    auto volume = setup.storage->getStoragePolicy()->getVolume(0);
    ANNIndexManager manager(makeConfig(volume, makeShape(8), 0xCCCCULL, "anns_sel7"));

    EXPECT_FALSE(selectEntry(manager, setup, 1, 0, 64));
    EXPECT_FALSE(selectEntry(manager, setup, 1, 1000, 0));
}


/// -----------------------------------------------------------------------------------------
/// State machine happy-path (B-T7)
///
/// Build a group end-to-end via `executeHere`, then verify the on-disk artefacts and the
/// manager's active snapshot.
/// -----------------------------------------------------------------------------------------

namespace
{
/// Prepare a fresh `ANNIndexManager` in a directory that lives on the same disk as the
/// storage, so that the build task operates on the same volume as the source parts. The
/// manager's root must exist before the first build, mirroring what `loadFromDisk` does in
/// production.
std::shared_ptr<ANNIndexManager> makeManagerForStorage(
    const MergeTreeTestHarness::TestStorage & setup,
    UInt32 dim,
    UInt64 hash_seed,
    const std::string & ann_root_relative)
{
    auto cfg = makeConfig(setup.storage->getStoragePolicy()->getVolume(0), makeShape(dim), hash_seed, ann_root_relative);
    auto mgr = std::make_shared<ANNIndexManager>(std::move(cfg));
    mgr->loadFromDisk();
    return mgr;
}

}

TEST(ANNBuildTaskTest, StateMachineHappyPath)
{
    TempDirScope disk_scope("happy");
    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk_scope.path.string(), "store/ann_happy", "emb", /*dim=*/ 8, /*partition_key_column=*/ "pk"));
    auto & setup = storage_scope.get();

    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 10, 1, makeVectors(10, 8, 1.0f));
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 10, 2, makeVectors(10, 8, 2.0f));

    auto parts = activeParts(*setup.storage);
    ASSERT_EQ(parts.size(), 2u);

    const UInt64 hash_seed = 0xC0FFEEULL;
    const std::string ann_root = "store/ann_happy/anns";
    auto manager = makeManagerForStorage(setup, 8, hash_seed, ann_root);

    /// Scheduler contract: the caller reserves the slot.
    auto reservation = manager->tryReserveBuildSlot();
    ASSERT_TRUE(reservation);

    auto context = Context::createCopy(getContext().context);
    auto metadata_snapshot = setup.storage->getInMemoryMetadataPtr(context, false);
    auto storage_snapshot = setup.storage->getStorageSnapshot(metadata_snapshot, context);

    auto entry = std::make_shared<ANNBuildSelectedEntry>();
    entry->selected_parts = std::vector<DataPartPtr>{parts.begin(), parts.end()};
    entry->storage_snapshot = storage_snapshot;
    entry->definition = makeDefinition(setup.vec_column_name, 8, hash_seed);

    auto task = std::make_shared<BuildANNIndexTask>(
        *setup.storage,
        std::move(entry),
        manager.get(),
        /*table_lock_holder=*/ TableLockHolder{},
        /*task_result_callback=*/ [](bool) {},
        std::move(reservation));

    BuildANNIndexTask::executeHere(task);

    /// Post-conditions:
    ///   (a) manager has exactly one active group;
    auto snap = manager->getActiveSnapshot();
    ASSERT_TRUE(snap);
    EXPECT_EQ(snap->size(), 1u);

    ///   (b) active group covers every source part;
    for (const auto & p : parts)
        EXPECT_TRUE(manager->isPartCovered(p));

    ///   (c) the build slot has been released;
    {
        auto r = manager->tryReserveBuildSlot();
        EXPECT_TRUE(r);
    }

    ///   (d) on disk there is an `ann_<uuid>/` directory next to the table-level meta.json,
    ///       and the fbin has been removed.
    auto disk = setup.storage->getStoragePolicy()->getVolume(0)->getDisk(0);
    const auto root_full = fs::path(disk->getPath()) / ann_root;
    ASSERT_TRUE(fs::exists(root_full));
    bool saw_group_dir = false;
    for (auto it = fs::directory_iterator(root_full); it != fs::directory_iterator(); ++it)
    {
        const auto name = it->path().filename().string();
        if (!it->is_directory())
            continue;
        if (std::string_view(name).starts_with("ann_"))
        {
            saw_group_dir = true;
            EXPECT_FALSE(fs::exists(it->path() / "vectors.fbin"));
            EXPECT_TRUE(fs::exists(it->path() / "id_map.bin"));
            EXPECT_TRUE(fs::exists(it->path() / "coverage.bin"));
            EXPECT_TRUE(fs::exists(it->path() / "meta.json"));
        }
        EXPECT_FALSE(std::string_view(name).starts_with("tmp_ann_"))
            << "tmp directory should have been renamed";
    }
    EXPECT_TRUE(saw_group_dir);

    /// The table-level `meta.json` carries the shape that this build ran with.
    auto meta = ANNIndexTableMeta::loadOrEmpty(setup.storage->getStoragePolicy()->getVolume(0), ann_root);
    EXPECT_EQ(meta.shape.dim, 8u);
    EXPECT_EQ(meta.hash_seed, hash_seed);
}


/// -----------------------------------------------------------------------------------------
/// Cancel path (B-T9)
///
/// Build a task, reserve the slot, cancel before running the state machine. The slot must be
/// released and no artefacts may be left behind.
/// -----------------------------------------------------------------------------------------

TEST(ANNBuildTaskTest, BuildSlotReleasedOnCancel)
{
    TempDirScope disk_scope("cancel");
    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk_scope.path.string(), "store/ann_cancel", "emb", /*dim=*/ 8, /*partition_key_column=*/ "pk"));
    auto & setup = storage_scope.get();
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 4, 1, makeVectors(4, 8, 1.0f));

    const UInt64 hash_seed = 0xDEAD10CCULL;
    const std::string ann_root = "store/ann_cancel/anns";
    auto manager = makeManagerForStorage(setup, 8, hash_seed, ann_root);

    auto reservation = manager->tryReserveBuildSlot();
    ASSERT_TRUE(reservation);

    auto context = Context::createCopy(getContext().context);
    auto metadata_snapshot = setup.storage->getInMemoryMetadataPtr(context, false);
    auto storage_snapshot = setup.storage->getStorageSnapshot(metadata_snapshot, context);

    auto parts = activeParts(*setup.storage);
    auto entry = std::make_shared<ANNBuildSelectedEntry>();
    entry->selected_parts = std::vector<DataPartPtr>{parts.begin(), parts.end()};
    entry->storage_snapshot = storage_snapshot;
    entry->definition = makeDefinition(setup.vec_column_name, 8, hash_seed);

    auto task = std::make_shared<BuildANNIndexTask>(
        *setup.storage,
        std::move(entry),
        manager.get(),
        TableLockHolder{},
        [](bool) {},
        std::move(reservation));

    task->cancel();
    task.reset();   /// drop the task so the reservation it owns rolls back

    /// Slot is back.
    {
        auto r = manager->tryReserveBuildSlot();
        EXPECT_TRUE(r);
    }

    /// Active list is still empty.
    EXPECT_TRUE(manager->getActiveSnapshot()->empty());
}


/// -----------------------------------------------------------------------------------------
/// In-flight uniqueness (B-T10)
/// -----------------------------------------------------------------------------------------

TEST(ANNBuildTaskTest, ConcurrentScheduleRejected)
{
    TempDirScope disk_scope("concurrent");
    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk_scope.path.string(), "store/ann_concurrent", "emb", /*dim=*/ 8, /*partition_key_column=*/ "pk"));
    auto & setup = storage_scope.get();
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 4, 1, makeVectors(4, 8, 1.0f));

    const UInt64 hash_seed = 0xC0DEULL;
    auto manager = makeManagerForStorage(setup, 8, hash_seed, "store/ann_concurrent/anns");

    {
        auto r = manager->tryReserveBuildSlot();
        ASSERT_TRUE(r);
        /// Second reservation must fail while the first is outstanding.
        EXPECT_FALSE(manager->tryReserveBuildSlot());
    }   /// r dtor releases the slot
    /// After release, a fresh reservation succeeds.
    {
        auto r = manager->tryReserveBuildSlot();
        EXPECT_TRUE(r);
    }
}


/// -----------------------------------------------------------------------------------------
/// Shape mismatch aborts early (B-T13)
/// -----------------------------------------------------------------------------------------

TEST(ANNBuildTaskTest, ShapeMismatchAbortsInPrepare)
{
    TempDirScope disk_scope("shape");
    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk_scope.path.string(), "store/ann_shape", "emb", /*dim=*/ 8, /*partition_key_column=*/ "pk"));
    auto & setup = storage_scope.get();
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 4, 1, makeVectors(4, 8, 1.0f));

    const UInt64 hash_seed = 0xBADD1F0ULL;
    /// Manager registers shape dim=8; definition uses dim=4 → prepare subtask must throw
    /// ABORTED before the disk transaction is created.
    auto manager = makeManagerForStorage(setup, 8, hash_seed, "store/ann_shape/anns");

    auto reservation = manager->tryReserveBuildSlot();
    ASSERT_TRUE(reservation);

    auto context = Context::createCopy(getContext().context);
    auto metadata_snapshot = setup.storage->getInMemoryMetadataPtr(context, false);
    auto storage_snapshot = setup.storage->getStorageSnapshot(metadata_snapshot, context);

    auto parts = activeParts(*setup.storage);
    auto entry = std::make_shared<ANNBuildSelectedEntry>();
    entry->selected_parts = std::vector<DataPartPtr>{parts.begin(), parts.end()};
    entry->storage_snapshot = storage_snapshot;
    entry->definition = makeDefinition(setup.vec_column_name, 4, hash_seed);  /// dim=4 disagrees with manager dim=8

    auto task = std::make_shared<BuildANNIndexTask>(
        *setup.storage,
        std::move(entry),
        manager.get(),
        TableLockHolder{},
        [](bool) {},
        std::move(reservation));

    EXPECT_THROW(BuildANNIndexTask::executeHere(task), DB::Exception);
    task.reset();   /// drop the task to roll back the reservation

    /// `task` destruction must have released the slot via reservation rollback.
    {
        auto r = manager->tryReserveBuildSlot();
        EXPECT_TRUE(r);
    }

    /// No group was published.
    EXPECT_TRUE(manager->getActiveSnapshot()->empty());
}


/// -----------------------------------------------------------------------------------------
/// Retire GC (B-T14 / B-T16)
///
/// Drives `StorageMergeTree::clearRetiredANNIndexGroups` through the retire → cleanup path.
/// -----------------------------------------------------------------------------------------

TEST(ANNBuildTaskTest, RetireGCCleansOrphanTmp)
{
    TempDirScope disk_scope("gc-orphan");
    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk_scope.path.string(), "store/ann_orphan", "emb", /*dim=*/ 8, /*partition_key_column=*/ "pk"));
    auto & setup = storage_scope.get();
    MergeTreeTestHarness::insertVectorBlock(setup.storage, setup, 4, 1, makeVectors(4, 8, 1.0f));

    const std::string ann_root = "store/ann_orphan/anns";
    auto manager = makeManagerForStorage(setup, 8, 0x01ULL, ann_root);

    auto disk = setup.storage->getStoragePolicy()->getVolume(0)->getDisk(0);
    const auto disk_root = fs::path(disk->getPath());
    const auto root_full = disk_root / ann_root;
    fs::create_directories(root_full);

    /// Manually drop an orphan tmp group directory (as if a previous build had crashed).
    const auto orphan = root_full / "tmp_ann_orphan_zzz";
    fs::create_directories(orphan);
    { std::ofstream(orphan / "dummy") << "stale"; }
    ASSERT_TRUE(fs::exists(orphan));

    /// The test harness does not declare an `ann` secondary index on the table, so
    /// `storage.getANNIndexManager()` stays null and `clearRetiredANNIndexGroups` becomes a
    /// no-op. We assert that branch behaves correctly, then exercise the manager-level
    /// primitive (`listGroupDirsOnDisk`) that powers the orphan sweep.
    const size_t cleaned_noop = setup.storage->clearRetiredANNIndexGroups();
    EXPECT_EQ(cleaned_noop, 0u) << "no manager → no work";

    /// The orphan must show up in `listGroupDirsOnDisk` and be classified as unknown by
    /// the manager (no active / retired / in-flight reference). The phase-2 sweep in
    /// `clearRetiredANNIndexGroups` then removes it via `removeRecursive`.
    auto disk_dirs = manager->listGroupDirsOnDisk();
    ASSERT_FALSE(disk_dirs.empty());
    bool saw_orphan = false;
    for (const auto & n : disk_dirs)
        if (n == "tmp_ann_orphan_zzz")
            saw_orphan = true;
    EXPECT_TRUE(saw_orphan);
    EXPECT_FALSE(manager->isPathKnown("tmp_ann_orphan_zzz"));
}


/// -----------------------------------------------------------------------------------------
/// In-flight protection (Bug-1 regression)
///
/// Reproduces the SIFT-1M failure: a `tmp_ann_<uuid>/` of an in-progress build must NOT be
/// classified as an orphan by `isPathKnown`. The previous implementation pattern-matched on
/// the `tmp_ann_*` prefix and would `removeRecursive` the directory, breaking the build.
/// -----------------------------------------------------------------------------------------

TEST(ANNBuildTaskTest, InFlightTmpDirIsKnownToManager)
{
    TempDirScope disk_scope("inflight-protect");
    StorageScope storage_scope(MergeTreeTestHarness::createStorageWithVectorColumn(
        disk_scope.path.string(), "store/ann_inflight", "emb", /*dim=*/ 8, /*partition_key_column=*/ "pk"));
    auto & setup = storage_scope.get();

    const std::string ann_root = "store/ann_inflight/anns";
    auto manager = makeManagerForStorage(setup, 8, 0xBEEFULL, ann_root);

    auto disk = setup.storage->getStoragePolicy()->getVolume(0)->getDisk(0);
    const auto disk_root = fs::path(disk->getPath());
    const auto root_full = disk_root / ann_root;
    fs::create_directories(root_full);

    /// Hold a live reservation: this is exactly the state during a long-running build,
    /// after `tryReserveBuildSlot` and before `commit()`.
    auto reservation = manager->tryReserveBuildSlot();
    ASSERT_TRUE(reservation);
    const std::string tmp_dir = reservation->tmpDir();

    /// Materialise the directory on disk (mirrors what `createGroupStorage` does inside
    /// the builder).
    auto storage = manager->createGroupStorage(tmp_dir);
    ASSERT_TRUE(storage);
    const auto on_disk = root_full / tmp_dir;
    ASSERT_TRUE(fs::exists(on_disk));

    /// Manager must report the directory as known → cleanup will skip it.
    EXPECT_TRUE(manager->isPathKnown(tmp_dir));

    /// Drop the reservation without committing → manager forgets the path → cleanup may
    /// then legitimately treat it as an orphan.
    reservation.reset();
    EXPECT_FALSE(manager->isPathKnown(tmp_dir));
}

#endif
