#include <gtest/gtest.h>

#include <config.h>

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Paimon/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonClient.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Common/tests/gtest_global_context.h>

#include <base/scope_guard.h>

#include <unistd.h> /// for ::getpid

#include <atomic>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>

namespace fs = std::filesystem;
using namespace DB;

namespace
{

/// A scoped temp directory that cleans itself up on destruction.
struct ScopedTempDir
{
    fs::path path;
    explicit ScopedTempDir(const std::string & name_hint)
        : path(fs::temp_directory_path() / fs::path(name_hint + "_" + std::to_string(::getpid())))
    {
        std::error_code ec;
        fs::remove_all(path, ec);
        fs::create_directories(path);
    }
    ~ScopedTempDir()
    {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

void writeFile(const fs::path & p, const std::string & content)
{
    fs::create_directories(p.parent_path());
    std::ofstream f(p, std::ios::binary | std::ios::trunc);
    f << content;
}

ObjectStoragePtr makeLocalObjectStorage(const std::string & key_prefix)
{
    return std::make_shared<LocalObjectStorage>(
        LocalObjectStorageSettings(/*disk_name_=*/"test_paimon_local", /*key_prefix_=*/key_prefix, /*read_only_=*/false));
}

/// Lay out a minimal Paimon table at `<root>/test.db/test_table` with the given
/// snapshot ids present and a LATEST hint pointing at `latest_hint`.
fs::path makePaimonTable(const fs::path & root, const std::vector<int> & snapshot_ids, const std::string & latest_hint)
{
    auto table = root / "test.db" / "test_table";
    for (int id : snapshot_ids)
        writeFile(table / Paimon::PAIMON_SNAPSHOT_DIR / (std::string(Paimon::PAIMON_SNAPSHOT_PREFIX) + std::to_string(id)), "{}");
    writeFile(table / Paimon::PAIMON_SNAPSHOT_DIR / Paimon::PAIMON_SNAPSHOT_LATEST_HINT, latest_hint);
    return table;
}

}

/// Happy path: the LATEST hint names the newest snapshot and there is no newer one,
/// so the hint is trusted directly.
TEST(PaimonLatestHint, ReadsLatestHintDirectly)
{
    ScopedTempDir tmp("ch_gtest_paimon_hint_direct");
    auto table = makePaimonTable(tmp.path, {1, 2}, "2");

    auto storage = makeLocalObjectStorage(tmp.path.string());
    PaimonTableClient client(storage, table.string(), getContext().context);

    auto info = client.getLatestTableSnapshotInfo();
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->first, 2);
    EXPECT_TRUE(fs::exists(info->second)) << info->second;
}

/// Stale hint: LATEST points at an older snapshot while a newer one exists on disk.
/// getLatestTableSnapshotInfo must fall through to snapshot listing and return the
/// real latest (snapshot-3), never the stale hint value.
TEST(PaimonLatestHint, FallsBackWhenHintIsStale)
{
    ScopedTempDir tmp("ch_gtest_paimon_hint_stale");
    auto table = makePaimonTable(tmp.path, {1, 2, 3}, "1");

    auto storage = makeLocalObjectStorage(tmp.path.string());
    PaimonTableClient client(storage, table.string(), getContext().context);

    auto info = client.getLatestTableSnapshotInfo();
    ASSERT_TRUE(info.has_value());
    EXPECT_EQ(info->first, 3);
    EXPECT_TRUE(fs::exists(info->second)) << info->second;
}

/// A writer rewrites the LATEST hint between "1" and "10" while the reader runs. The reader must
/// never abort and must always resolve to a snapshot that exists on disk. Before the fix this hit
/// the AsynchronousBoundedReadBuffer cached-size chassert (the test_paimon_incremental_read flake).
TEST(PaimonLatestHint, ConcurrentHintRewriteDoesNotCrash)
{
    ScopedTempDir tmp("ch_gtest_paimon_hint_race");
    auto table = makePaimonTable(tmp.path, {1, 10}, "1");
    auto hint = table / Paimon::PAIMON_SNAPSHOT_DIR / Paimon::PAIMON_SNAPSHOT_LATEST_HINT;

    auto storage = makeLocalObjectStorage(tmp.path.string());
    PaimonTableClient client(storage, table.string(), getContext().context);

    std::atomic<bool> stop{false};
    std::thread writer(
        [&]
        {
            const char * vals[] = {"1", "10"};
            size_t i = 0;
            while (!stop.load(std::memory_order_relaxed))
                writeFile(hint, vals[(i++) & 1u]);
        });
    /// Join on every exit path (failed ASSERT_* returns early, or the code under test throws):
    /// a still-joinable std::thread destructor would call std::terminate and turn a plain test
    /// failure into a process abort, which is the crash this test must distinguish from.
    SCOPE_EXIT({
        stop.store(true);
        writer.join();
    });

    for (int i = 0; i < 4000; ++i)
    {
        auto info = client.getLatestTableSnapshotInfo();
        /// A torn read of the hint is tolerated (the parse fails and we fall back to snapshot
        /// listing), so the result must still be a snapshot that exists on disk.
        ASSERT_TRUE(info.has_value());
        EXPECT_TRUE(fs::exists(info->second)) << info->second;
    }
}

#endif
