#include <gtest/gtest.h>

#include <config.h>

#if USE_AVRO

#include <Storages/ObjectStorage/DataLakes/Paimon/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonClient.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonMetadata.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/CurrentThread.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Common/setThreadName.h>

#include <base/scope_guard.h>

#include <unistd.h> /// for ::getpid

#include <atomic>
#include <filesystem>
#include <fstream>
#include <functional>
#include <optional>
#include <string>
#include <thread>

namespace ProfileEvents
{
    extern const Event RemoteFSBuffers;
}

namespace DB::ErrorCodes
{
    extern const int NETWORK_ERROR;
}

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

/// Stands in for a backend that cannot answer an existence probe (HDFS with the NameNode
/// unreachable, S3/Azure on a 5xx or an auth failure). Real storages signal that by throwing
/// out of `tryGetObjectMetadata`; `LocalObjectStorage` alone never reaches that state under a
/// temp directory, hence this injection point.
struct ProbeFailingObjectStorage : public LocalObjectStorage
{
    std::atomic<bool> fail_probe{false};

    explicit ProbeFailingObjectStorage(const std::string & key_prefix)
        : LocalObjectStorage(
              LocalObjectStorageSettings(/*disk_name_=*/"test_paimon_probe_failing", /*key_prefix_=*/key_prefix, /*read_only_=*/false))
    {
    }

    std::optional<ObjectMetadata> tryGetObjectMetadata(const std::string & path, bool with_tags) const override
    {
        /// Not `LOGICAL_ERROR`: that aborts the process in debug and sanitizer builds
        /// (`Exception::handleErrorCode`), so the test could never observe a throw. A real
        /// unreachable backend reports a transport-level code, which is what this mimics.
        if (fail_probe)
            throw Exception(ErrorCodes::NETWORK_ERROR, "Injected backend failure while probing {}", path);
        return LocalObjectStorage::tryGetObjectMetadata(path, with_tags);
    }
};

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

/// RAII ThreadGroup with its own ProfileEvents counters so a read's event increments can be
/// observed in isolation (mirrors the helper in src/IO/tests/gtest_reader_executor_metric.cpp).
struct CountingThreadGroup
{
    std::optional<ThreadStatus> thread_status_holder{
        current_thread ? std::nullopt : std::optional<ThreadStatus>(std::in_place)};
    ThreadGroupPtr thread_group = ThreadGroup::createForQuery(getContext().context);
    ThreadGroupSwitcher switcher{thread_group, ThreadName::UNKNOWN, /*allow_existing_group=*/true};

    ProfileEvents::Count get(ProfileEvents::Event event) const { return thread_group->performance_counters[event]; }
};

/// A clean read of a mutable-metadata object must not go through AsynchronousBoundedReadBuffer (the
/// cached-size chassert path). Over LocalObjectStorage that buffer is the only reader that bumps
/// RemoteFSBuffers, so a zero delta is a deterministic, race-free signal (see the commit message).
void expectReadDoesNotUseAsyncBoundedBuffer(const std::function<void()> & read)
{
    CountingThreadGroup tg;
    auto before = tg.get(ProfileEvents::RemoteFSBuffers);
    read();
    EXPECT_EQ(tg.get(ProfileEvents::RemoteFSBuffers), before)
        << "mutable-metadata read went through AsynchronousBoundedReadBuffer (cached-size chassert path)";
}

}


/// schema-0 is another mutable metadata object: validateTableIdentity reads it on every
/// background refresh to detect an external DROP + re-CREATE at the same path, and a recreate can
/// write a larger schema-0. A concurrent writer that grows schema-0 while the reader is mid-read
/// used to hit the same AsynchronousBoundedReadBuffer cached-size chassert as the LATEST hint.
/// The contract getTableSchemaJSON must uphold is fail-closed without process termination: a torn
/// read may throw a normal parse exception, but the process must never abort, and a clean read
/// must still return a valid schema.
TEST(PaimonMutableMetadata, ConcurrentSchemaZeroRewriteDoesNotCrash)
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
            catch (...) // NOLINT(bugprone-empty-catch)
            {
                /// Ok: a torn read throws a normal parse exception; the test only requires no abort.
            }
        }
    }

    /// Writer has stopped and schema-0 is a complete document again: a clean read must succeed and
    /// return the expected schema. This rejects a degenerate implementation that "passes" the loop
    /// above only by always throwing.
    writeFile(schema0, small_schema);
    Poco::JSON::Object::Ptr schema_json;
    /// Race-free signal: the schema-0 read must not use the cached-size async buffer.
    expectReadDoesNotUseAsyncBoundedBuffer([&] { schema_json = client.getTableSchemaJSON(schema0_info); });
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
TEST(PaimonMutableMetadata, ConcurrentSnapshotRewriteDoesNotCrash)
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
            catch (...) // NOLINT(bugprone-empty-catch)
            {
                /// Ok: a torn read throws a normal parse exception; the test only requires no abort.
            }
        }
    }

    /// Writer stopped: a clean read must succeed (rejects an always-throwing implementation).
    writeFile(snapshot1, small_snapshot);
    std::optional<PaimonSnapshot> snapshot;
    /// Race-free signal: the snapshot read must not use the cached-size async buffer.
    expectReadDoesNotUseAsyncBoundedBuffer([&] { snapshot.emplace(client.getSnapshot({1, snapshot1.string()})); });
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->id, 1);
}

/// Incremental read may skip a snapshot whose load failed (and advance the watermark past it)
/// only when the `snapshot-N` file is genuinely gone, i.e. removed by Paimon compaction.
/// If the file still exists, the failure was a live-read problem (torn read during an external
/// recreate, transient backend error) and the caller must fail closed instead of losing data.
TEST(PaimonIncrementalRead, SkipsOnlyGenuinelyMissingSnapshots)
{
    ScopedTempDir tmp("paimon_snapshot_skip_test");
    auto table = makePaimonTable(tmp.path, /*snapshot_ids=*/{1}, /*latest_hint=*/"1");
    auto storage = makeLocalObjectStorage(tmp.path.string());

    const auto snapshot1 = table / Paimon::PAIMON_SNAPSHOT_DIR / (std::string(Paimon::PAIMON_SNAPSHOT_PREFIX) + "1");
    const auto snapshot2 = table / Paimon::PAIMON_SNAPSHOT_DIR / (std::string(Paimon::PAIMON_SNAPSHOT_PREFIX) + "2");

    /// The snapshot file is still there: a failed load must not be skipped (fail closed).
    EXPECT_FALSE(PaimonMetadata::isSnapshotLoadFailureSkippable(storage, snapshot1.string()));
    /// The snapshot file never existed / was removed by compaction: skipping is safe.
    EXPECT_TRUE(PaimonMetadata::isSnapshotLoadFailureSkippable(storage, snapshot2.string()));

    /// Removing the file flips the verdict for the same snapshot id.
    fs::remove(snapshot1);
    EXPECT_TRUE(PaimonMetadata::isSnapshotLoadFailureSkippable(storage, snapshot1.string()));
}

/// A backend that cannot answer the probe must not be read as "the snapshot is gone".
/// `LocalObjectStorage` cannot express that state (its own `exists`/stat throw on a real
/// backend error), so the unknown case needs an injecting storage: only a definite absence
/// is an empty optional, and everything else propagates and keeps the caller fail-closed.
TEST(PaimonIncrementalRead, TreatsUnknownExistenceAsNotSkippable)
{
    ScopedTempDir tmp("paimon_snapshot_probe_error_test");
    auto table = makePaimonTable(tmp.path, /*snapshot_ids=*/{1}, /*latest_hint=*/"1");
    const auto snapshot1 = table / Paimon::PAIMON_SNAPSHOT_DIR / (std::string(Paimon::PAIMON_SNAPSHOT_PREFIX) + "1");

    auto storage = std::make_shared<ProbeFailingObjectStorage>(tmp.path.string());

    /// Baseline: the delegate answers, so the verdict is the ordinary one.
    EXPECT_FALSE(PaimonMetadata::isSnapshotLoadFailureSkippable(storage, snapshot1.string()));

    /// The backend fails the probe: the failure must propagate, not become "skippable".
    storage->fail_probe = true;
    EXPECT_THROW(
        PaimonMetadata::isSnapshotLoadFailureSkippable(storage, snapshot1.string()), DB::Exception);

    /// The same holds when the object is genuinely absent: an unanswerable probe never
    /// licenses advancing the watermark, so absence must not be inferred from an error.
    fs::remove(snapshot1);
    EXPECT_THROW(
        PaimonMetadata::isSnapshotLoadFailureSkippable(storage, snapshot1.string()), DB::Exception);

    /// Once the backend answers again, a genuine absence is still skippable.
    storage->fail_probe = false;
    EXPECT_TRUE(PaimonMetadata::isSnapshotLoadFailureSkippable(storage, snapshot1.string()));
}

/// A targeted read (`paimon_target_snapshot_id`) loads `snapshot-N` from a path that an external
/// DROP + re-CREATE reuses, and snapshot numbering restarts from 1 in the recreated table.  The
/// snapshot itself therefore carries no evidence of the recreate: `snapshot-1` of the new table is
/// byte-comparable to `snapshot-1` of the old one.  What does change is `schema-0`'s `timeMillis`,
/// which is why `iterate` re-validates table identity after loading the targeted snapshot.  This
/// test pins that property of the reused path: the same snapshot id still loads cleanly after the
/// recreate, while the identity probe sees a different value and so fails the read closed.
TEST(PaimonIncrementalRead, DetectsRecreateOnReusedTargetSnapshotPath)
{
    ScopedTempDir tmp("paimon_target_snapshot_recreate");
    auto table = tmp.path / "test.db" / "test_table";
    auto schema0 = table / Paimon::PAIMON_SCHEMA_DIR / (std::string(Paimon::PAIMON_SCHEMA_PREFIX) + "0");
    auto snapshot1 = table / Paimon::PAIMON_SNAPSHOT_DIR / (std::string(Paimon::PAIMON_SNAPSHOT_PREFIX) + "1");

    writeFile(schema0, R"({"version":3,"timeMillis":1000})");
    writeFile(snapshot1, makeSnapshotJson(1));

    auto storage = makeLocalObjectStorage(tmp.path.string());
    PaimonTableClient client(storage, table.string(), getContext().context);

    /// Identity latched when the ClickHouse table was created.
    const Int64 latched_time_millis = PaimonMetadata::readSchemaZeroTimeMillis(client);
    EXPECT_EQ(latched_time_millis, 1000);
    EXPECT_EQ(client.getSnapshot({1, snapshot1.string()}).id, 1);

    /// External DROP + re-CREATE at the same path: snapshot numbering restarts, so `snapshot-1`
    /// exists again and loads fine, but it now belongs to a different table.
    fs::remove_all(table);
    writeFile(schema0, R"({"version":3,"timeMillis":2000})");
    writeFile(snapshot1, makeSnapshotJson(1));

    EXPECT_EQ(client.getSnapshot({1, snapshot1.string()}).id, 1)
        << "the recreated table reuses the snapshot path, so the load itself cannot detect the recreate";
    EXPECT_NE(PaimonMetadata::readSchemaZeroTimeMillis(client), latched_time_millis)
        << "identity probe must see the recreate that the targeted snapshot load cannot";
}

/// An external DROP is not atomic: it can remove `snapshot-N` files while `schema/schema-0` is still
/// the old one.  In that interleaving neither guard on the collecting side can tell the removal from
/// a Paimon compaction gap — the existence probe sees a missing file, and the pre-commit identity
/// re-validation still sees the unchanged old `timeMillis` — so the watermark does advance past
/// snapshots that the DROP, not compaction, removed.  What must not happen is the recreated table
/// inheriting that watermark under the reused `keeper_path` and skipping its first snapshots, so the
/// watermark is committed together with the table generation it belongs to and discarded when that
/// generation no longer matches.  This test walks exactly that ordering.
TEST(PaimonIncrementalRead, DiscardsWatermarkFromAnEarlierTableGeneration)
{
    ScopedTempDir tmp("paimon_watermark_generation");
    auto table = tmp.path / "test.db" / "test_table";
    auto schema0 = table / Paimon::PAIMON_SCHEMA_DIR / (std::string(Paimon::PAIMON_SCHEMA_PREFIX) + "0");
    const auto snapshot_path = [&](int id)
    { return table / Paimon::PAIMON_SNAPSHOT_DIR / (std::string(Paimon::PAIMON_SNAPSHOT_PREFIX) + std::to_string(id)); };

    writeFile(schema0, R"({"version":3,"timeMillis":1000})");
    for (int id : {1, 2, 3})
        writeFile(snapshot_path(id), makeSnapshotJson(id));

    auto storage = makeLocalObjectStorage(tmp.path.string());
    PaimonTableClient client(storage, table.string(), getContext().context);

    /// Identity latched when the ClickHouse table was created, and a watermark committed for it.
    const Int64 old_generation = PaimonMetadata::readSchemaZeroTimeMillis(client);
    EXPECT_EQ(old_generation, 1000);
    EXPECT_TRUE(PaimonMetadata::isCommittedWatermarkFromSameTable(old_generation, old_generation));

    /// First half of a non-atomic external DROP: `snapshot-2` is gone, `schema-0` is untouched.
    fs::remove(snapshot_path(2));
    EXPECT_TRUE(PaimonMetadata::isSnapshotLoadFailureSkippable(storage, snapshot_path(2).string()))
        << "a deleted snapshot file is indistinguishable from a compaction gap at this point";
    EXPECT_EQ(PaimonMetadata::readSchemaZeroTimeMillis(client), old_generation)
        << "the identity re-validation before the commit cannot see this half of the DROP either";
    /// So a watermark may legitimately be committed for snapshot 3 under the old generation here.

    /// Second half: the re-CREATE rewrites `schema-0` and restarts snapshot numbering from 1.  The
    /// recreated table's own snapshot-1 is byte-comparable to the old one, so only the generation
    /// marker distinguishes the two watermarks.
    fs::remove_all(table);
    writeFile(schema0, R"({"version":3,"timeMillis":2000})");
    writeFile(snapshot_path(1), makeSnapshotJson(1));

    const Int64 new_generation = PaimonMetadata::readSchemaZeroTimeMillis(client);
    EXPECT_EQ(new_generation, 2000);
    EXPECT_FALSE(PaimonMetadata::isCommittedWatermarkFromSameTable(old_generation, new_generation))
        << "the watermark of the dropped table must not be inherited by the recreated one";
    EXPECT_TRUE(PaimonMetadata::isCommittedWatermarkFromSameTable(new_generation, new_generation));

    /// A watermark written before generation tracking existed carries no marker, so it may equally
    /// be this table's progress or progress inherited from a dropped one: it is of unknown
    /// generation and must not be trusted, because trusting it is the only direction that can skip
    /// data.  Discarding it takes the initial-read branch, which always commits a watermark and so
    /// latches the marker - the unmarked state is left behind after a single batch, whether or not
    /// that batch had any new snapshots to read.
    EXPECT_FALSE(PaimonMetadata::isCommittedWatermarkFromSameTable(std::nullopt, new_generation))
        << "an unmarked watermark may have been inherited from a dropped table at the same keeper_path";
    /// A table whose identity could not be latched has nothing to compare against, so the watermark
    /// is taken at face value there.
    EXPECT_TRUE(PaimonMetadata::isCommittedWatermarkFromSameTable(old_generation, 0));
    EXPECT_TRUE(PaimonMetadata::isCommittedWatermarkFromSameTable(std::nullopt, 0));
}

#endif
