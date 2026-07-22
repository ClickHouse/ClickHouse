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

    /// The pre-fix chassert only fires when the hint read goes through createReadBuffer's
    /// AsynchronousBoundedReadBuffer, which is selected by remote_filesystem_read_method=threadpool
    /// + remote_filesystem_read_prefetch=1. Pin both explicitly so the repro stays tied to the
    /// failure mode rather than to whatever the global defaults happen to be.
    auto context = Context::createCopy(getContext().context);
    context->setSetting("remote_filesystem_read_method", String("threadpool"));
    context->setSetting("remote_filesystem_read_prefetch", Field(true));

    auto storage = makeLocalObjectStorage(tmp.path.string());
    PaimonTableClient client(storage, table.string(), context);

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

/// schema-0 is another mutable metadata object: validateTableIdentity reads it on every
/// background refresh to detect an external DROP + re-CREATE at the same path, and a recreate can
/// write a larger schema-0. A concurrent writer that grows schema-0 while the reader is mid-read
/// used to hit the same AsynchronousBoundedReadBuffer cached-size chassert as the LATEST hint.
/// The contract getTableSchemaJSON must uphold is fail-closed without process termination: a torn
/// read may throw a normal parse exception, but the process must never abort, and a clean read
/// must still return a valid schema.
TEST(PaimonLatestHint, ConcurrentSchemaZeroRewriteDoesNotCrash)
{
    ScopedTempDir tmp("ch_gtest_paimon_schema0_race");
    auto table = tmp.path / "test.db" / "test_table";
    auto schema0 = table / Paimon::PAIMON_SCHEMA_DIR / (std::string(Paimon::PAIMON_SCHEMA_PREFIX) + "0");

    /// A small and a larger schema JSON, so the file size changes across the rewrite. Padding lives
    /// in an ignored field so both documents remain parseable objects.
    const std::string small_schema = R"({"version":3,"timeMillis":1})";
    const std::string large_schema = R"({"version":3,"timeMillis":2,"_pad":")" + std::string(64 * 1024, 'x') + R"("})";
    writeFile(schema0, small_schema);

    /// Pin the async-prefetch read path (same as the LATEST test) so the repro stays tied to the
    /// failure mode rather than to whatever the global defaults happen to be.
    auto context = Context::createCopy(getContext().context);
    context->setSetting("remote_filesystem_read_method", String("threadpool"));
    context->setSetting("remote_filesystem_read_prefetch", Field(true));

    auto storage = makeLocalObjectStorage(tmp.path.string());
    PaimonTableClient client(storage, table.string(), context);

    auto schema0_info = client.getTableSchemaInfoById(0);

    {
        std::atomic<bool> stop{false};
        std::thread writer(
            [&]
            {
                size_t i = 0;
                while (!stop.load(std::memory_order_relaxed))
                    writeFile(schema0, ((i++) & 1u) ? large_schema : small_schema);
            });
        /// Join on every exit path (see the LATEST test): a still-joinable std::thread destructor
        /// would std::terminate and turn a plain test failure into a process abort.
        SCOPE_EXIT({
            stop.store(true);
            writer.join();
        });

        for (int i = 0; i < 4000; ++i)
        {
            /// A torn read yields an unparseable document; getTableSchemaJSON then throws a normal
            /// exception (never aborts). Either a valid schema object or a thrown parse error is
            /// tolerated here; the point is that the process must not terminate.
            try
            {
                auto schema_json = client.getTableSchemaJSON(schema0_info);
                EXPECT_FALSE(schema_json.isNull());
            }
            catch (...)
            {
            }
        }
    }

    /// Writer has stopped and schema-0 is a complete document again: a clean read must succeed and
    /// return the expected schema. This rejects a degenerate implementation that "passes" the loop
    /// above only by always throwing.
    writeFile(schema0, small_schema);
    auto schema_json = client.getTableSchemaJSON(schema0_info);
    ASSERT_FALSE(schema_json.isNull());
    EXPECT_EQ(schema_json->getValue<Int64>("timeMillis"), 1);
}

namespace
{
std::string makeSnapshotJson(Int64 id, const std::string & pad = "")
{
    return R"({"id":)" + std::to_string(id) + R"(,"schemaId":0,"baseManifestList":"b","deltaManifestList":"d",)"
        + R"("commitUser":"u","commitIdentifier":1,"commitKind":"APPEND","timeMillis":1)"
        + (pad.empty() ? "" : R"(,"_pad":")" + pad + R"(")") + "}";
}
}

/// snapshot-N is read on every refresh (in loadLatestState, before validateTableIdentity) at a fixed
/// path that an external DROP + re-CREATE reuses, so it is mutable in place just like schema-0. The
/// same contract applies: a concurrent grow may cause a torn parse (a normal exception) but must
/// never abort the process, and a clean read must still return a valid snapshot.
TEST(PaimonLatestHint, ConcurrentSnapshotRewriteDoesNotCrash)
{
    ScopedTempDir tmp("ch_gtest_paimon_snapshot_race");
    auto table = tmp.path / "test.db" / "test_table";
    auto snapshot1 = table / Paimon::PAIMON_SNAPSHOT_DIR / (std::string(Paimon::PAIMON_SNAPSHOT_PREFIX) + "1");

    const std::string small_snapshot = makeSnapshotJson(1);
    const std::string large_snapshot = makeSnapshotJson(1, std::string(64 * 1024, 'x'));
    writeFile(snapshot1, small_snapshot);

    auto context = Context::createCopy(getContext().context);
    context->setSetting("remote_filesystem_read_method", String("threadpool"));
    context->setSetting("remote_filesystem_read_prefetch", Field(true));

    auto storage = makeLocalObjectStorage(tmp.path.string());
    PaimonTableClient client(storage, table.string(), context);

    {
        std::atomic<bool> stop{false};
        std::thread writer(
            [&]
            {
                size_t i = 0;
                while (!stop.load(std::memory_order_relaxed))
                    writeFile(snapshot1, ((i++) & 1u) ? large_snapshot : small_snapshot);
            });
        SCOPE_EXIT({
            stop.store(true);
            writer.join();
        });

        for (int i = 0; i < 4000; ++i)
        {
            try
            {
                auto snapshot = client.getSnapshot({1, snapshot1.string()});
                EXPECT_EQ(snapshot.id, 1);
            }
            catch (...)
            {
            }
        }
    }

    /// Writer stopped: a clean read must succeed (rejects an always-throwing implementation).
    writeFile(snapshot1, small_snapshot);
    auto snapshot = client.getSnapshot({1, snapshot1.string()});
    EXPECT_EQ(snapshot.id, 1);
}

#endif
