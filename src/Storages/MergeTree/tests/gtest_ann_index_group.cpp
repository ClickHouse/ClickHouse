#include "config.h"
#if USE_DISKANN

#include <gtest/gtest.h>

#include <Storages/MergeTree/ANNIndex/ANNGroupCoverage.h>
#include <Storages/MergeTree/ANNIndex/ANNGroupStorageDiskFull.h>
#include <Storages/MergeTree/ANNIndex/ANNIndexGroup.h>
#include <Storages/MergeTree/ANNIndex/ANNIndexTableMeta.h>
#include <Storages/MergeTree/ANNIndex/PartRowId.h>
#include <Storages/MergeTree/ANNIndex/PartRowIdMapWriter.h>
#include <Storages/MergeTree/ANNIndex/DiskANNIndexSearcherAdapter.h>
#include <Storages/MergeTree/DiskANNIndex.h>

#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>

#include <IO/WriteBufferFromFile.h>

#include <Common/Exception.h>

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <random>
#include <string>
#include <vector>

using namespace DB;

namespace
{
namespace fs = std::filesystem;

fs::path makeUniqueTempDir(const std::string & name)
{
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    auto p = fs::temp_directory_path() / ("clickhouse-anng-" + name + "-" + std::to_string(now));
    fs::create_directories(p);
    return p;
}

class TempDirScope
{
public:
    explicit TempDirScope(const std::string & name) : path(makeUniqueTempDir(name)) {}
    ~TempDirScope() { std::error_code ec; fs::remove_all(path, ec); }
    fs::path path;
};

void writeFbin(const fs::path & path, const std::vector<float> & data, uint32_t rows, uint32_t dim)
{
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char *>(&rows), sizeof(rows));
    out.write(reinterpret_cast<const char *>(&dim), sizeof(dim));
    out.write(reinterpret_cast<const char *>(data.data()),
              static_cast<std::streamsize>(data.size() * sizeof(float)));
}

DiskANNBuildOptions testBuildOpts()
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

ANNSearchDefaultsPtr testSearchOpts()
{
    DiskANNSearchOptions opts;
    opts.num_threads = 1;
    opts.search_io_limit = 4;
    opts.num_nodes_to_cache = 0;
    opts.default_search_list_size = 16;
    opts.default_beam_width = 4;
    return std::make_shared<DiskANNSearchDefaults>(opts);
}

/// Build a minimal self-consistent group directory:
///   - idx_*.* produced by the real FFI builder,
///   - id_map.bin with one PartRowId per data row,
///   - coverage.bin with a single (partition_hash, [0..0]) entry,
///   - meta.json matching the shape.
struct BuiltGroup
{
    fs::path root;
    ANNGroupStoragePtr storage;
    size_t rows;
    size_t dim;
    std::vector<float> data;
};

BuiltGroup buildGroup(const std::string & tag, size_t rows, size_t dim,
                      DiskANNMetric metric = DiskANNMetric::L2, uint64_t hash_seed = 0xAABBCCDDEEFF0011ULL)
{
    TempDirScope root("group-" + tag);
    fs::create_directories(root.path / "ann_test");
    const auto fbin = root.path / "ann_test" / "vectors.fbin";
    const auto prefix = root.path / "ann_test" / DiskANNArtifactNames::INDEX_PREFIX_BASENAME;

    std::mt19937 rng(42); // NOLINT(cert-msc32-c,cert-msc51-cpp) — deterministic test fixtures.
    std::uniform_real_distribution<float> dist(-1.f, 1.f);
    std::vector<float> data(rows * dim);
    for (auto & v : data)
        v = dist(rng);

    writeFbin(fbin, data, static_cast<uint32_t>(rows), static_cast<uint32_t>(dim));

    DiskANNDiskIndexBuilder builder(dim, metric, testBuildOpts());
    builder.setDataPath(fbin.string());
    builder.setIndexPrefix(prefix.string());
    builder.build();

    /// Wrap in a group storage under a volume rooted at `root`.
    auto disk = std::make_shared<DiskLocal>("anng_disk_" + tag, root.path.string() + "/");
    auto volume = std::make_shared<SingleDiskVolume>("anng_vol_" + tag, disk);
    auto storage = std::make_shared<ANNGroupStorageDiskFull>(volume, std::string("ann_test"));

    /// id_map.bin — one PartRowId per row, partition_hash = hash_seed for all rows.
    PartRowIdMapWriter id_map_writer;
    for (size_t i = 0; i < rows; ++i)
        id_map_writer.append(PartRowId{/*partition_hash=*/ hash_seed,
                                       /*block_number=*/ 0,
                                       /*block_offset=*/ i});
    id_map_writer.writeTo(*storage, WriteSettings{});

    /// coverage.bin — one partition covering [0, 0].
    ANNGroupCoverage coverage;
    coverage.addPart(hash_seed, 0, 0);
    coverage.writeTo(*storage);

    /// meta.json — hand-rolled so the test doesn't depend on the builder code path.
    const std::string meta =
        std::string("{\n") +
        R"(  "version": 1,)" "\n" +
        R"(  "shape": { "dim": )" + std::to_string(dim) +
            R"(, "metric": )" + std::to_string(static_cast<int>(metric)) +
            R"(, "algorithm": "diskann", "params_hash": "0x0" },)" "\n" +
        R"(  "hash_algo": "sipHash64",)" "\n" +
        R"(  "hash_seed": "0x)" + [&](){
            std::string out; out.reserve(16);
            for (int shift = 60; shift >= 0; shift -= 4)
                out.push_back("0123456789abcdef"[(hash_seed >> shift) & 0xF]);
            return out;
        }() + R"(",)" "\n" +
        R"(  "num_points": )" + std::to_string(rows) + "\n" +
        "}\n";

    auto meta_out = storage->writeFile("meta.json", 4096, WriteMode::Rewrite, WriteSettings{});
    meta_out->write(meta.data(), meta.size());
    meta_out->finalize();

    BuiltGroup g;
    g.root = root.path;
    g.storage = storage;
    g.rows = rows;
    g.dim = dim;
    g.data = std::move(data);

    /// NOTE: `TempDirScope` destroyed on return — make the dir "persistent" by
    /// creating it outside its lifetime. The simplest fix: rename into a stable
    /// location before returning.
    auto stable = fs::temp_directory_path() /
        ("clickhouse-anng-stable-" + tag + "-" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()));
    fs::rename(root.path, stable);
    g.root = stable;
    /// Recreate storage/disk rooted at the new path.
    auto disk2 = std::make_shared<DiskLocal>("anng_disk_s_" + tag, stable.string() + "/");
    auto volume2 = std::make_shared<SingleDiskVolume>("anng_vol_s_" + tag, disk2);
    g.storage = std::make_shared<ANNGroupStorageDiskFull>(volume2, std::string("ann_test"));
    return g;
}

void cleanupGroup(BuiltGroup & g)
{
    g.storage.reset();
    std::error_code ec;
    fs::remove_all(g.root, ec);
}
}

TEST(ANNIndexGroupTest, LoadAndSearchL2)
{
    auto g = buildGroup("load-l2", /*rows=*/ 128, /*dim=*/ 16);

    auto group = ANNIndexGroup::load(g.storage, testSearchOpts());
    ASSERT_NE(group, nullptr);
    EXPECT_EQ(group->numPoints(), g.rows);
    EXPECT_EQ(group->getShape().dim, g.dim);

    /// Use the 0-th row as a query; expect it to be returned.
    std::vector<float> query(g.data.begin(), g.data.begin() + g.dim);
    auto hits = group->search(query.data(), g.dim, /*k=*/ 5, ANNSearchOverrides{});
    ASSERT_EQ(hits.size(), 5u);
    for (const auto & h : hits)
    {
        EXPECT_LT(h.internal_id, g.rows);
        EXPECT_GE(h.distance, 0.f);
        auto rowid = group->lookup(h.internal_id);
        EXPECT_EQ(rowid.block_number, 0u);
        EXPECT_LT(rowid.block_offset, g.rows);
    }

    cleanupGroup(g);
}

TEST(ANNIndexGroupTest, RejectsInconsistentMetaDim)
{
    auto g = buildGroup("inconsistent-dim", /*rows=*/ 64, /*dim=*/ 8);

    /// Overwrite meta.json with a shape whose `dim` disagrees with the built index.
    const std::string bad_meta =
        "{\"version\":1,\"shape\":{\"dim\":9,\"metric\":0,\"algorithm\":\"diskann\",\"params_hash\":\"0x0\"},"
        "\"hash_algo\":\"sipHash64\",\"hash_seed\":\"0x0\",\"num_points\":64}";
    auto out = g.storage->writeFile("meta.json", 4096, WriteMode::Rewrite, WriteSettings{});
    out->write(bad_meta.data(), bad_meta.size());
    out->finalize();

    EXPECT_THROW(ANNIndexGroup::load(g.storage, testSearchOpts()), Exception);

    cleanupGroup(g);
}

TEST(ANNIndexGroupTest, RejectsIdMapRowCountMismatch)
{
    auto g = buildGroup("idmap-size-mismatch", /*rows=*/ 64, /*dim=*/ 8);

    /// Overwrite meta.json with `num_points` that disagrees with the id_map.bin record count.
    const std::string bad_meta =
        "{\"version\":1,\"shape\":{\"dim\":8,\"metric\":0,\"algorithm\":\"diskann\",\"params_hash\":\"0x0\"},"
        "\"hash_algo\":\"sipHash64\",\"hash_seed\":\"0x0\",\"num_points\":65}";
    auto out = g.storage->writeFile("meta.json", 4096, WriteMode::Rewrite, WriteSettings{});
    out->write(bad_meta.data(), bad_meta.size());
    out->finalize();

    EXPECT_THROW(ANNIndexGroup::load(g.storage, testSearchOpts()), Exception);

    cleanupGroup(g);
}

TEST(ANNIndexGroupTest, CosineDistancesAreWithinRange)
{
    auto g = buildGroup("cosine", /*rows=*/ 64, /*dim=*/ 8, DiskANNMetric::Cosine);

    auto group = ANNIndexGroup::load(g.storage, testSearchOpts());
    std::vector<float> query(g.data.begin(), g.data.begin() + g.dim);
    auto hits = group->search(query.data(), g.dim, /*k=*/ 3, ANNSearchOverrides{});

    ASSERT_EQ(hits.size(), 3u);
    for (const auto & h : hits)
    {
        EXPECT_GE(h.distance, 0.f);
        EXPECT_LE(h.distance, 2.f);
    }

    cleanupGroup(g);
}

/// Reload the group through a brand-new storage object rooted at the same on-disk
/// path. Proves the persisted group directory is self-describing and the
/// `meta.json` / `id_map.bin` / `coverage.bin` / `idx_*.*` on-disk formats are
/// stable across process lifetimes — no in-memory disk/volume caches are shared
/// between the two load calls.
///
/// Determinism note: the hit-by-hit bitwise comparison below (`EXPECT_FLOAT_EQ`
/// on distances, equality on `internal_id`) relies on single-threaded search
/// (`num_threads=1` in `testSearchOpts`). If that option is ever raised for
/// speed, DiskANN's BFS ordering may become nondeterministic and this case will
/// flake. Keep the search thread count at 1 here regardless of what other
/// suites choose.
TEST(ANNIndexGroupTest, ReloadFromFreshStorageMatches)
{
    const uint64_t seed = 0xDEADBEEFCAFEBABEULL;
    auto g = buildGroup("reload", /*rows=*/ 96, /*dim=*/ 12, DiskANNMetric::L2, seed);

    /// First load + record observable state.
    auto group_first = ANNIndexGroup::load(g.storage, testSearchOpts());
    ASSERT_NE(group_first, nullptr);
    const size_t num_points_first = group_first->numPoints();
    const UInt32 dim_first = group_first->getShape().dim;
    EXPECT_EQ(num_points_first, g.rows);
    EXPECT_EQ(dim_first, g.dim);
    EXPECT_TRUE(group_first->containsPart(seed, 0, 0));
    EXPECT_EQ(group_first->getHashSeed(), seed);

    std::vector<float> query(g.data.begin() + g.dim, g.data.begin() + 2 * g.dim);
    auto hits_first = group_first->search(query.data(), g.dim, /*k=*/ 5, ANNSearchOverrides{});
    ASSERT_EQ(hits_first.size(), 5u);

    /// Drop all in-memory state bound to the first storage. The persistent
    /// directory at `g.root` stays on disk.
    group_first.reset();
    g.storage.reset();

    /// Recreate Disk/Volume/Storage from scratch at the same path — no pointer
    /// equality with the originals.
    auto disk_reopen = std::make_shared<DiskLocal>("anng_disk_reopen", g.root.string() + "/");
    auto volume_reopen = std::make_shared<SingleDiskVolume>("anng_vol_reopen", disk_reopen);
    auto storage_reopen = std::make_shared<ANNGroupStorageDiskFull>(volume_reopen, std::string("ann_test"));

    /// Second load through the fresh storage.
    auto group_second = ANNIndexGroup::load(storage_reopen, testSearchOpts());
    ASSERT_NE(group_second, nullptr);
    EXPECT_EQ(group_second->numPoints(), num_points_first);
    EXPECT_EQ(group_second->getShape().dim, dim_first);
    EXPECT_TRUE(group_second->containsPart(seed, 0, 0));
    EXPECT_EQ(group_second->getHashSeed(), seed);

    /// Search determinism: same index + same search options + same query ⇒
    /// bitwise-identical hits.
    auto hits_second = group_second->search(query.data(), g.dim, /*k=*/ 5, ANNSearchOverrides{});
    ASSERT_EQ(hits_second.size(), hits_first.size());
    for (size_t i = 0; i < hits_first.size(); ++i)
    {
        EXPECT_EQ(hits_second[i].internal_id, hits_first[i].internal_id)
            << "hit " << i << " internal_id diverged after reload";
        EXPECT_FLOAT_EQ(hits_second[i].distance, hits_first[i].distance)
            << "hit " << i << " distance diverged after reload";
        auto row_first = group_second->lookup(hits_first[i].internal_id);
        auto row_second = group_second->lookup(hits_second[i].internal_id);
        EXPECT_EQ(row_first.partition_hash, seed);
        EXPECT_EQ(row_second.partition_hash, seed);
    }

    /// Explicitly drop the second group so the reopen-side disk releases files
    /// before `cleanupGroup` removes the tree.
    group_second.reset();
    storage_reopen.reset();
    std::error_code ec;
    fs::remove_all(g.root, ec);
}

#endif
