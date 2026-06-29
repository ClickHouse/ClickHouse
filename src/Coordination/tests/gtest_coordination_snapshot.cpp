#include "config.h"

#if USE_NURAFT
#include <Coordination/tests/gtest_coordination_common.h>

#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperLogStore.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/KeeperChunkedSnapshot.h>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/SnapshotableHashTable.h>
#include <Coordination/KeeperStorage.h>

#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>
#include <Common/tests/gtest_global_context.h>

#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDisk.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/DiskObjectStorage/Replication/ObjectStorageRouter.h>
#include <Disks/DiskLocal.h>

#include <Coordination/WriteBufferFromNuraftBuffer.h>

#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileDecorator.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

#include <Poco/Util/MapConfiguration.h>

#include <base/scope_guard.h>

#include <IO/ZstdInflatingReadBuffer.h>
#include <IO/ZstdDeflatingWriteBuffer.h>
#include <IO/copyData.h>

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <fstream>
#include <future>
#include <limits>
#include <stdexcept>
#include <thread>

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsBool compress_snapshots_with_zstd_format;
    extern const CoordinationSettingsUInt64 snapshot_chunk_size;
    extern const CoordinationSettingsUInt64 snapshot_deser_threads;
    extern const CoordinationSettingsUInt64 snapshot_transfer_chunk_size;
}

namespace ProfileEvents
{
    extern const Event KeeperSnapshotWrittenBytes;
}

namespace
{

class TestLocalObjectStorage : public DB::LocalObjectStorage
{
public:
    mutable std::atomic<int> read_count{0};

    explicit TestLocalObjectStorage(DB::LocalObjectStorageSettings settings)
        : DB::LocalObjectStorage(std::move(settings)) {}

    std::unique_ptr<DB::ReadBufferFromFileBase> readObject( /// NOLINT
        const DB::StoredObject & object,
        const DB::ReadSettings & read_settings,
        std::optional<size_t> read_hint,
        bool use_external_buffer,
        bool restrict_seek) const override
    {
        ++read_count;
        return DB::LocalObjectStorage::readObject(object, read_settings, read_hint, use_external_buffer, restrict_seek);
    }
};

enum class SnapshotDiskFailureMode
{
    OpenFileAfterCreate,
    SyncFile,
    SyncFileAndCleanupDataFileRemoveFailure,
    RemoveFileOnce,
};

class ThrowingSnapshotWriteBuffer : public DB::WriteBufferFromFileDecorator
{
public:
    explicit ThrowingSnapshotWriteBuffer(std::unique_ptr<DB::WriteBuffer> impl_)
        : DB::WriteBufferFromFileDecorator(std::move(impl_))
    {
    }

    void sync() override
    {
        /// Production snapshot paths call `finalize` before `sync`; this injects a failure after pending bytes are flushed.
        throw std::runtime_error("Injected snapshot sync failure");
    }
};

/// Snapshot files carry unique `snapshot_<idx>_<uuid>` names, so failure injection
/// matches by path prefix (e.g. "snapshot_50_" or "tmp_snapshot_52_"). A data-file
/// prefix does not match its `tmp_` marker, preserving selectivity.
class ThrowingSnapshotDisk : public DB::DiskLocal
{
public:
    ThrowingSnapshotDisk(
        const std::string & disk_name,
        const std::string & disk_path,
        std::string fail_path_prefix_,
        SnapshotDiskFailureMode failure_mode_)
        : DB::DiskLocal(disk_name, disk_path)
        , fail_path_prefix(std::move(fail_path_prefix_))
        , failure_mode(failure_mode_)
    {
    }

    void disarm()
    {
        failure_enabled = false;
    }

    void arm()
    {
        failure_enabled = true;
    }

    void setFailPathPrefix(std::string p)
    {
        fail_path_prefix = std::move(p);
    }

    std::unique_ptr<DB::WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size,
        DB::WriteMode mode,
        const DB::WriteSettings & settings) override
    {
        auto inner = DB::DiskLocal::writeFile(path, buf_size, mode, settings);

        if (failure_enabled && path.starts_with(fail_path_prefix) && failure_mode == SnapshotDiskFailureMode::OpenFileAfterCreate)
            throw std::runtime_error("Injected snapshot open failure");

        if (failure_enabled
            && path.starts_with(fail_path_prefix)
            && (failure_mode == SnapshotDiskFailureMode::SyncFile
                || failure_mode == SnapshotDiskFailureMode::SyncFileAndCleanupDataFileRemoveFailure))
            return std::make_unique<ThrowingSnapshotWriteBuffer>(std::move(inner));

        return inner;
    }

    void removeFile(const String & path) override
    {
        if (failure_enabled && path.starts_with(fail_path_prefix) && failure_mode == SnapshotDiskFailureMode::RemoveFileOnce && !remove_failed)
        {
            remove_failed = true;
            throw std::runtime_error("Injected snapshot remove failure");
        }

        DB::DiskLocal::removeFile(path);
    }

    void removeFileIfExists(const String & path) override
    {
        if (failure_enabled
            && path.starts_with(fail_path_prefix)
            && failure_mode == SnapshotDiskFailureMode::SyncFileAndCleanupDataFileRemoveFailure
            && !remove_if_exists_failed)
        {
            remove_if_exists_failed = true;
            throw std::runtime_error("Injected snapshot remove-if-exists failure");
        }

        DB::DiskLocal::removeFileIfExists(path);
    }

private:
    std::string fail_path_prefix;
    SnapshotDiskFailureMode failure_mode;
    bool failure_enabled = true;
    bool remove_failed = false;
    bool remove_if_exists_failed = false;
};

struct BlockingSnapshotWriteState
{
    std::mutex mutex;
    std::condition_variable cv;
    bool blocked = false;
    bool released = false;
    size_t sync_calls = 0;

    bool waitUntilBlocked(std::chrono::milliseconds timeout)
    {
        std::unique_lock lock(mutex);
        return cv.wait_for(lock, timeout, [&] { return blocked; });
    }

    void release()
    {
        {
            std::lock_guard lock(mutex);
            released = true;
        }
        cv.notify_all();
    }

    size_t getSyncCalls()
    {
        std::lock_guard lock(mutex);
        return sync_calls;
    }
};

class BlockingSnapshotWriteBuffer : public DB::WriteBufferFromFileDecorator
{
public:
    BlockingSnapshotWriteBuffer(std::unique_ptr<DB::WriteBuffer> impl_, std::shared_ptr<BlockingSnapshotWriteState> state_)
        : DB::WriteBufferFromFileDecorator(std::move(impl_))
        , state(std::move(state_))
    {
    }

    void sync() override
    {
        {
            std::unique_lock lock(state->mutex);
            ++state->sync_calls;
            state->blocked = true;
            state->cv.notify_all();
            state->cv.wait(lock, [&] { return state->released; });
        }

        DB::WriteBufferFromFileDecorator::sync();
    }

private:
    std::shared_ptr<BlockingSnapshotWriteState> state;
};

class BlockingSnapshotDisk : public DB::DiskLocal
{
public:
    BlockingSnapshotDisk(
        const std::string & disk_name,
        const std::string & disk_path,
        std::string block_path_prefix_,
        std::shared_ptr<BlockingSnapshotWriteState> state_)
        : DB::DiskLocal(disk_name, disk_path)
        , block_path_prefix(std::move(block_path_prefix_))
        , state(std::move(state_))
    {
    }

    std::unique_ptr<DB::WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size,
        DB::WriteMode mode,
        const DB::WriteSettings & settings) override
    {
        auto inner = DB::DiskLocal::writeFile(path, buf_size, mode, settings);
        if (path.starts_with(block_path_prefix))
        {
            bool expected = true;
            /// Wrap only the FIRST matching write (atomic: reached from two threads): in the
            /// same-index race tests both sides write `snapshot_<idx>_*` files and wrapping the
            /// receive too would deadlock (it blocks in sync() holding snapshots_lock).
            if (armed.compare_exchange_strong(expected, false))
                return std::make_unique<BlockingSnapshotWriteBuffer>(std::move(inner), state);
        }
        return inner;
    }

private:
    std::string block_path_prefix;
    std::shared_ptr<BlockingSnapshotWriteState> state;
    std::atomic_bool armed{true};
};

/// All persisted snapshot names are unique (snapshot_<idx>_<uuid>.bin[.zstd]) — tests
/// locate files by parsed index instead of literal names.
/// With `include_tmp_markers` the result also counts `tmp_` marker files for the index.
std::vector<std::string> snapshotFilesForIdx(const std::string & dir, uint64_t idx, bool include_tmp_markers = false)
{
    std::vector<std::string> result;
    for (const auto & entry : fs::directory_iterator(dir))
    {
        std::string name = entry.path().filename().string();
        if (name.starts_with(DB::tmp_keeper_file_prefix))
        {
            if (!include_tmp_markers)
                continue;
            name = name.substr(DB::tmp_keeper_file_prefix.size());
        }
        if (!name.starts_with("snapshot_"))
            continue;
        if (DB::getLogIdxFromSnapshotPath(name) == idx)
            result.push_back(entry.path().filename().string());
    }
    return result;
}

template <typename Manager>
void assertNoSnapshotArtifactsAndNoRegistration(Manager & manager, const std::string & dir, uint64_t idx)
{
    EXPECT_TRUE(snapshotFilesForIdx(dir, idx).empty());
    EXPECT_TRUE(snapshotFilesForIdx(dir, idx, /*include_tmp_markers=*/true).empty());
    EXPECT_EQ(manager.totalSnapshots(), 0);
    EXPECT_EQ(manager.getLatestSnapshotIndex(), 0);
    EXPECT_EQ(manager.getLatestSnapshotInfo(), nullptr);
}

std::pair<std::shared_ptr<DB::DiskObjectStorage>, std::shared_ptr<TestLocalObjectStorage>>
createLocalObjectStorageDisk(const std::string & meta_path, const std::string & obj_path)
{
    auto obj_storage = std::make_shared<TestLocalObjectStorage>(
        DB::LocalObjectStorageSettings("SnapshotDisk", obj_path, false));
    std::unordered_map<DB::Location, DB::LocationInfo> cluster_locations = {{"main", {true, true, ""}}};
    auto cluster = std::make_shared<DB::ClusterConfiguration>("SnapshotDisk", std::move(cluster_locations));
    auto router = std::make_shared<DB::ObjectStorageRouter>(
        std::unordered_map<DB::Location, DB::ObjectStoragePtr>{{"main", obj_storage}});
    auto meta_disk = std::make_shared<DB::DiskLocal>("SnapshotMetaDisk", meta_path);
    DB::MetadataStoragePtr metadata_storage = std::make_shared<DB::MetadataStorageFromDisk>(
        meta_disk, "", obj_storage->createKeyGenerator(), /*persist_removal_queue_=*/false, /*removal_log_compaction_threshold_=*/0);
    Poco::AutoPtr<Poco::Util::MapConfiguration> config_ptr(new Poco::Util::MapConfiguration);
    auto disk = std::make_shared<DB::DiskObjectStorage>(
        "SnapshotDisk", cluster, metadata_storage, router, /*wrapped_disk=*/nullptr, *config_ptr, "", /*use_fake_transaction=*/true);
    return {disk, obj_storage};
}

struct IntNode
{
    int value;
    IntNode(int value_) : value(value_) { } /// NOLINT(google-explicit-constructor)
    IntNode copyFromSnapshotNode() { return *this; }
    [[maybe_unused]] UInt64 sizeInBytes() const { return sizeof value; }
    [[maybe_unused]] bool operator==(const int & rhs) const { return value == rhs; }
    [[maybe_unused]] bool operator!=(const int & rhs) const { return rhs != this->value; }
};

}

TEST(ACLMapTest, OverflowWraparound)
{
    DB::ACLMap acl_map;

    auto id1 = acl_map.convertACLs({{1, "digest", "user1:pwd"}});
    EXPECT_EQ(id1, 1);

    /// Push max_acl_id to UINT32_MAX so the next allocation is at the boundary
    acl_map.addMapping(std::numeric_limits<DB::ACLId>::max() - 1, {{1, "digest", "placeholder"}});

    auto id2 = acl_map.convertACLs({{1, "digest", "user2:pwd"}});
    EXPECT_EQ(id2, std::numeric_limits<DB::ACLId>::max());

    auto id3 = acl_map.convertACLs({{1, "digest", "user3:pwd"}});
    EXPECT_EQ(id3, 2);
}

TEST(ACLMapTest, RemoveUnusedACLs)
{
    DB::ACLMap acl_map;

    /// Simulate snapshot deserialization: mappings are added with usage counter 0.
    acl_map.addMapping(1, {{1, "digest", "used:pwd"}});
    acl_map.addMapping(2, {{1, "digest", "unused:pwd"}});
    acl_map.addMapping(3, {{1, "digest", "also_used:pwd"}});

    /// Simulate reading nodes that reference only ids 1 and 3.
    acl_map.addUsage(1);
    acl_map.addUsage(3);
    acl_map.addUsage(3);

    acl_map.removeUnusedACLs();

    /// Only the referenced ACLs (ids 1 and 3) remain.
    auto mapping = acl_map.getMapping();
    EXPECT_EQ(mapping.size(), 2);

    bool has1 = false;
    bool has2 = false;
    bool has3 = false;
    for (const auto & [id, acls] : mapping)
    {
        has1 |= (id == 1);
        has2 |= (id == 2);
        has3 |= (id == 3);
    }
    EXPECT_TRUE(has1);
    EXPECT_FALSE(has2);
    EXPECT_TRUE(has3);

    /// Referenced ACLs keep their content.
    EXPECT_EQ(acl_map.convertNumber(1), (Coordination::ACLs{{1, "digest", "used:pwd"}}));
    EXPECT_EQ(acl_map.convertNumber(3), (Coordination::ACLs{{1, "digest", "also_used:pwd"}}));

    /// The unused ACL was also removed from the ACL-to-number map, so converting the same ACLs
    /// allocates a fresh id instead of returning the old (now dangling) one.
    auto new_id = acl_map.convertACLs({{1, "digest", "unused:pwd"}});
    EXPECT_NE(new_id, 2);
    EXPECT_EQ(acl_map.convertNumber(new_id), (Coordination::ACLs{{1, "digest", "unused:pwd"}}));
}

TYPED_TEST(CoordinationTest, SnapshotableHashMapSimple)
{
    DB::SnapshotableHashTable<IntNode> hello;
    EXPECT_TRUE(hello.insert("hello", 5).second);
    EXPECT_TRUE(hello.contains("hello"));
    EXPECT_EQ(hello.getValue("hello"), 5);
    EXPECT_FALSE(hello.insert("hello", 145).second);
    EXPECT_EQ(hello.getValue("hello"), 5);
    hello.updateValue("hello", [](IntNode & value) { value = 7; });
    EXPECT_EQ(hello.getValue("hello"), 7);
    EXPECT_EQ(hello.size(), 1);
    EXPECT_TRUE(hello.erase("hello"));
    EXPECT_EQ(hello.size(), 0);
}

TYPED_TEST(CoordinationTest, SnapshotableHashMapTrySnapshot)
{
    DB::SnapshotableHashTable<IntNode> map_snp;
    EXPECT_TRUE(map_snp.insert("/hello", 7).second);
    EXPECT_FALSE(map_snp.insert("/hello", 145).second);
    map_snp.enableSnapshotMode(map_snp.snapshotSizeWithVersion().second);
    EXPECT_FALSE(map_snp.insert("/hello", 145).second);
    map_snp.updateValue("/hello", [](IntNode & value) { value = 554; });
    EXPECT_EQ(map_snp.getValue("/hello"), 554);
    EXPECT_EQ(map_snp.snapshotSizeWithVersion().first, 2);
    EXPECT_EQ(map_snp.size(), 1);

    auto itr = map_snp.begin();
    EXPECT_EQ(itr->key, "/hello");
    EXPECT_EQ(itr->value, 7);
    EXPECT_EQ(itr->isActiveInMap(), false);
    itr = std::next(itr);
    EXPECT_EQ(itr->key, "/hello");
    EXPECT_EQ(itr->value, 554);
    EXPECT_EQ(itr->isActiveInMap(), true);
    itr = std::next(itr);
    EXPECT_EQ(itr, map_snp.end());
    for (int i = 0; i < 5; ++i)
    {
        EXPECT_TRUE(map_snp.insert("/hello" + std::to_string(i), i).second);
    }
    EXPECT_EQ(map_snp.getValue("/hello3"), 3);

    EXPECT_EQ(map_snp.snapshotSizeWithVersion().first, 7);
    EXPECT_EQ(map_snp.size(), 6);
    itr = std::next(map_snp.begin(), 2);
    for (size_t i = 0; i < 5; ++i)
    {
        EXPECT_EQ(itr->key, "/hello" + std::to_string(i));
        EXPECT_EQ(itr->value, i);
        EXPECT_EQ(itr->isActiveInMap(), true);
        itr = std::next(itr);
    }

    EXPECT_TRUE(map_snp.erase("/hello3"));
    EXPECT_TRUE(map_snp.erase("/hello2"));

    EXPECT_EQ(map_snp.snapshotSizeWithVersion().first, 7);
    EXPECT_EQ(map_snp.size(), 4);
    itr = std::next(map_snp.begin(), 2);
    for (size_t i = 0; i < 5; ++i)
    {
        EXPECT_EQ(itr->key, "/hello" + std::to_string(i));
        EXPECT_EQ(itr->value, i);
        EXPECT_EQ(itr->isActiveInMap(), i != 3 && i != 2);
        itr = std::next(itr);
    }
    map_snp.disableSnapshotMode();
    map_snp.clearOutdatedNodes();

    EXPECT_EQ(map_snp.snapshotSizeWithVersion().first, 4);
    EXPECT_EQ(map_snp.size(), 4);
    itr = map_snp.begin();
    EXPECT_EQ(itr->key, "/hello");
    EXPECT_EQ(itr->value, 554);
    EXPECT_EQ(itr->isActiveInMap(), true);
    itr = std::next(itr);
    EXPECT_EQ(itr->key, "/hello0");
    EXPECT_EQ(itr->value, 0);
    EXPECT_EQ(itr->isActiveInMap(), true);
    itr = std::next(itr);
    EXPECT_EQ(itr->key, "/hello1");
    EXPECT_EQ(itr->value, 1);
    EXPECT_EQ(itr->isActiveInMap(), true);
    itr = std::next(itr);
    EXPECT_EQ(itr->key, "/hello4");
    EXPECT_EQ(itr->value, 4);
    EXPECT_EQ(itr->isActiveInMap(), true);
    itr = std::next(itr);
    EXPECT_EQ(itr, map_snp.end());
}

TYPED_TEST(CoordinationTest, SnapshotableHashMapDataSize)
{
    /// int
    DB::SnapshotableHashTable<IntNode> hello;
    EXPECT_EQ(hello.getApproximateDataSize(), 0);

    hello.insert("hello", 1);
    EXPECT_EQ(hello.getApproximateDataSize(), 9);
    hello.updateValue("hello", [](IntNode & value) { value = 2; });
    EXPECT_EQ(hello.getApproximateDataSize(), 9);
    hello.insertOrReplace("hello", 3);
    EXPECT_EQ(hello.getApproximateDataSize(), 9);

    hello.erase("hello");
    EXPECT_EQ(hello.getApproximateDataSize(), 0);

    hello.clear();
    EXPECT_EQ(hello.getApproximateDataSize(), 0);

    /// Insert a node, then enable snapshot mode so the node is captured by the snapshot.
    hello.insert("hello", 1);
    EXPECT_EQ(hello.getApproximateDataSize(), 9);
    hello.enableSnapshotMode(hello.snapshotSizeWithVersion().second);
    hello.updateValue("hello", [](IntNode & value) { value = 2; });
    EXPECT_EQ(hello.getApproximateDataSize(), 18);
    /// The node was already updated (version > snapshot_up_to_version),
    /// so insertOrReplace does not create another snapshot copy.
    hello.insertOrReplace("hello", 1);
    EXPECT_EQ(hello.getApproximateDataSize(), 18);

    /// Must disable snapshot mode before clearing outdated nodes (matches production flow).
    hello.disableSnapshotMode();
    hello.clearOutdatedNodes();
    EXPECT_EQ(hello.getApproximateDataSize(), 9);

    /// Enable a new snapshot to test erase keeping outdated nodes.
    hello.enableSnapshotMode(hello.snapshotSizeWithVersion().second);
    hello.erase("hello");
    EXPECT_EQ(hello.getApproximateDataSize(), 9);

    hello.disableSnapshotMode();
    hello.clearOutdatedNodes();
    EXPECT_EQ(hello.getApproximateDataSize(), 0);

    /// Node
    using Node = DB::KeeperStorage::Node;
    DB::SnapshotableHashTable<Node> world;
    Node n1;
    n1.setData("1234");
    Node n2;
    n2.setData("123456");
    n2.addChild("c");

    /// Note: Below, we check in many cases only that getApproximateDataSize() > 0. This is because
    ///       the SnapshotableHashTable's approximate data size includes Node's `sizeInBytes`, which
    ///       includes `CompactChildrenSet::heapSizeInBytes` (0 for nodes with 0-1 children,
    ///       approximate for nodes with 2+ children). The approximate size is only used for
    ///       statistics accounting, so this should be okay.

    world.insert("world", n1);
    EXPECT_GT(world.getApproximateDataSize(), 0);
    world.updateValue("world", [&](Node & value) { value = n2; });
    EXPECT_GT(world.getApproximateDataSize(), 0);

    world.erase("world");
    EXPECT_EQ(world.getApproximateDataSize(), 0);

    world.insert("world", n1);
    EXPECT_GT(world.getApproximateDataSize(), 0);
    world.enableSnapshotMode(world.snapshotSizeWithVersion().second);
    world.updateValue("world", [&](Node & value) { value = n2; });
    EXPECT_GT(world.getApproximateDataSize(), 0);

    /// Erase while in snapshot mode — outdated nodes stay in list.
    world.erase("world");
    EXPECT_GT(world.getApproximateDataSize(), 0);

    world.disableSnapshotMode();
    world.clearOutdatedNodes();
    EXPECT_EQ(world.getApproximateDataSize(), 0);
}

TYPED_TEST(CoordinationTest, TestStorageSnapshotSimple)
{
    ChangelogDirTest test("./snapshots");
    this->setSnapshotDirectory("./snapshots");

    using Storage [[maybe_unused]] = DB::KeeperStorage;


    DB::KeeperSnapshotManager manager(3, this->keeper_context, this->enable_compression);

    Storage storage(500, "", this->keeper_context);

    /// Set ACLs on nodes to verify acl_id round-trips through V7 snapshots
    auto acl_id1 = storage.acl_map.convertACLs({{31, "world", "anyone"}});
    auto acl_id2 = storage.acl_map.convertACLs({{1, "digest", "user1:pwd"}});

    addNode(storage, "/hello1", "world", 1, acl_id1);
    addNode(storage, "/hello2", "somedata", 3, acl_id2);
    const int64_t large_seq_num = static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 100;
    storage.container.updateValue("/", [&](typename Storage::Node & node) { node.stats.setSeqNum(large_seq_num); });
    storage.session_id_counter = 5;
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 2;
    storage.committed_ephemerals[3] = {"/hello2"};
    storage.committed_ephemerals[1] = {"/hello1"};
    storage.getSessionID(130);
    storage.getSessionID(130);

    DB::KeeperStorageSnapshot snapshot(&storage, 2, nullptr, DB::SnapshotVersion::V7);

    EXPECT_EQ(snapshot.snapshot_meta->get_last_log_idx(), 2);
    EXPECT_EQ(snapshot.session_id, 7);
    EXPECT_EQ(snapshot.snapshot_container_size, 6);
    EXPECT_EQ(snapshot.session_and_timeout.size(), 2);

    auto buf = manager.serializeSnapshotToBuffer(snapshot);
    manager.serializeSnapshotBufferToDisk(*buf, 2);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 2).size(), 1);

    auto debuf = manager.deserializeSnapshotBufferFromDisk(2);

    auto deser_result = manager.deserializeSnapshotFromBuffer(debuf);
    const auto & restored_storage = deser_result.storage;

    EXPECT_EQ(restored_storage->container.size(), 6);
    EXPECT_EQ(restored_storage->container.getValue("/").getChildren().size(), 3);
    EXPECT_EQ(restored_storage->container.getValue("/hello1").getChildren().size(), 0);
    EXPECT_EQ(restored_storage->container.getValue("/hello2").getChildren().size(), 0);

    EXPECT_EQ(restored_storage->container.getValue("/").getData(), "");
    EXPECT_EQ(restored_storage->container.getValue("/hello1").getData(), "world");
    EXPECT_EQ(restored_storage->container.getValue("/hello2").getData(), "somedata");
    EXPECT_EQ(restored_storage->session_id_counter, 7);
    EXPECT_EQ(restored_storage->getZXID(), 2);
    EXPECT_EQ(restored_storage->committed_ephemerals.size(), 2);
    EXPECT_EQ(restored_storage->committed_ephemerals[3].size(), 1);
    EXPECT_EQ(restored_storage->committed_ephemerals[1].size(), 1);
    EXPECT_EQ(restored_storage->session_and_timeout.size(), 2);

    /// Verify ACL round-trip
    EXPECT_EQ(restored_storage->container.getValue("/hello1").acl_id, acl_id1);
    EXPECT_EQ(restored_storage->container.getValue("/hello2").acl_id, acl_id2);
    auto restored_acls = restored_storage->acl_map.convertNumber(acl_id2);
    EXPECT_EQ(restored_acls.size(), 1);
    EXPECT_EQ(restored_acls[0].scheme, "digest");

    /// Verify seq_num round-trip (int64_t, value > INT32_MAX)
    EXPECT_EQ(restored_storage->container.find("/")->value.stats.seqNum(), large_seq_num);
}

TYPED_TEST(CoordinationTest, TestStorageSnapshotMoreWrites)
{

    ChangelogDirTest test("./snapshots");
    this->setSnapshotDirectory("./snapshots");

    using Storage [[maybe_unused]] = DB::KeeperStorage;


    DB::KeeperSnapshotManager manager(3, this->keeper_context, this->enable_compression);

    Storage storage(500, "", this->keeper_context);
    storage.getSessionID(130);

    for (size_t i = 0; i < 50; ++i)
    {
        addNode(storage, "/hello_" + std::to_string(i), "world_" + std::to_string(i));
    }

    DB::KeeperStorageSnapshot snapshot(&storage, 50, nullptr, this->keeper_context->getWriteSnapshotVersion());
    EXPECT_EQ(snapshot.snapshot_meta->get_last_log_idx(), 50);
    EXPECT_EQ(snapshot.snapshot_container_size, 54);

    for (size_t i = 50; i < 100; ++i)
    {
        addNode(storage, "/hello_" + std::to_string(i), "world_" + std::to_string(i));
    }

    EXPECT_EQ(storage.container.size(), 104);

    auto buf = manager.serializeSnapshotToBuffer(snapshot);
    manager.serializeSnapshotBufferToDisk(*buf, 50);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 50).size(), 1);

    auto debuf = manager.deserializeSnapshotBufferFromDisk(50);
    auto deser_result = manager.deserializeSnapshotFromBuffer(debuf);
    const auto & restored_storage = deser_result.storage;

    EXPECT_EQ(restored_storage->container.size(), 54);
    for (size_t i = 0; i < 50; ++i)
    {
        EXPECT_EQ(restored_storage->container.getValue("/hello_" + std::to_string(i)).getData(), "world_" + std::to_string(i));
    }
}


TYPED_TEST(CoordinationTest, TestStorageSnapshotManySnapshots)
{

    ChangelogDirTest test("./snapshots");
    this->setSnapshotDirectory("./snapshots");

    using Storage [[maybe_unused]] = DB::KeeperStorage;


    DB::KeeperSnapshotManager manager(3, this->keeper_context, this->enable_compression);

    Storage storage(500, "", this->keeper_context);
    storage.getSessionID(130);

    for (size_t j = 1; j <= 5; ++j)
    {
        for (size_t i = (j - 1) * 50; i < j * 50; ++i)
        {
            addNode(storage, "/hello_" + std::to_string(i), "world_" + std::to_string(i));
        }

        DB::KeeperStorageSnapshot snapshot(&storage, j * 50, nullptr, this->keeper_context->getWriteSnapshotVersion());
        auto buf = manager.serializeSnapshotToBuffer(snapshot);
        manager.serializeSnapshotBufferToDisk(*buf, j * 50);
        EXPECT_EQ(snapshotFilesForIdx("./snapshots", j * 50).size(), 1);
    }

    EXPECT_TRUE(snapshotFilesForIdx("./snapshots", 50).empty());
    EXPECT_TRUE(snapshotFilesForIdx("./snapshots", 100).empty());
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 150).size(), 1);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 200).size(), 1);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 250).size(), 1);


    auto deser_result= manager.restoreFromLatestSnapshot();
    const auto & restored_storage = deser_result.storage;

    EXPECT_EQ(restored_storage->container.size(), 254);

    for (size_t i = 0; i < 250; ++i)
    {
        EXPECT_EQ(restored_storage->container.getValue("/hello_" + std::to_string(i)).getData(), "world_" + std::to_string(i));
    }
}

TYPED_TEST(CoordinationTest, TestStorageSnapshotMode)
{

    ChangelogDirTest test("./snapshots");
    this->setSnapshotDirectory("./snapshots");

    using Storage [[maybe_unused]] = DB::KeeperStorage;


    DB::KeeperSnapshotManager manager(3, this->keeper_context, this->enable_compression);
    Storage storage(500, "", this->keeper_context);

    for (size_t i = 0; i < 50; ++i)
    {
        addNode(storage, fmt::format("/hello_{}", i), fmt::format("world_{}", i));
    }
    {
        DB::KeeperStorageSnapshot snapshot(&storage, 50, nullptr, this->keeper_context->getWriteSnapshotVersion());
        for (size_t i = 0; i < 50; ++i)
        {
            storage.container.updateValue(fmt::format("/hello_{}", i), [&](auto & node) { node.setData(fmt::format("wrld_{}", i)); });
        }
        for (size_t i = 0; i < 50; ++i)
        {
            EXPECT_EQ(storage.container.getValue(fmt::format("/hello_{}", i)).getData(), fmt::format("wrld_{}", i));
        }
        for (size_t i = 0; i < 50; ++i)
        {
            if (i % 2 == 0)
                storage.container.erase(fmt::format("/hello_{}", i));
        }
        EXPECT_EQ(storage.container.size(), 29);
        EXPECT_EQ(storage.container.snapshotSizeWithVersion().first, 104);
        EXPECT_EQ(storage.container.snapshotSizeWithVersion().second, 1);
        auto buf = manager.serializeSnapshotToBuffer(snapshot);
        manager.serializeSnapshotBufferToDisk(*buf, 50);
    }
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 50).size(), 1);
    EXPECT_EQ(storage.container.size(), 29);
    storage.clearGarbageAfterSnapshot();
    EXPECT_EQ(storage.container.snapshotSizeWithVersion().first, 29);
    for (size_t i = 0; i < 50; ++i)
    {
        if (i % 2 != 0)
            EXPECT_EQ(storage.container.getValue(fmt::format("/hello_{}", i)).getData(), fmt::format("wrld_{}", i));
        else
            EXPECT_FALSE(storage.container.contains(fmt::format("/hello_{}", i)));
    }

    auto deser_result = manager.restoreFromLatestSnapshot();
    const auto & restored_storage = deser_result.storage;

    for (size_t i = 0; i < 50; ++i)
    {
        EXPECT_EQ(restored_storage->container.getValue(fmt::format("/hello_{}", i)).getData(), fmt::format("world_{}", i));
    }
}

TYPED_TEST(CoordinationTest, TestStorageSnapshotBroken)
{

    ChangelogDirTest test("./snapshots");
    this->setSnapshotDirectory("./snapshots");

    using Storage [[maybe_unused]] = DB::KeeperStorage;


    DB::KeeperSnapshotManager manager(3, this->keeper_context, this->enable_compression);
    Storage storage(500, "", this->keeper_context);
    for (size_t i = 0; i < 50; ++i)
    {
        addNode(storage, "/hello_" + std::to_string(i), "world_" + std::to_string(i));
    }
    {
        DB::KeeperStorageSnapshot snapshot(&storage, 50, nullptr, this->keeper_context->getWriteSnapshotVersion());
        auto buf = manager.serializeSnapshotToBuffer(snapshot);
        manager.serializeSnapshotBufferToDisk(*buf, 50);
    }
    auto snapshot_files = snapshotFilesForIdx("./snapshots", 50);
    ASSERT_EQ(snapshot_files.size(), 1);

    /// Let's corrupt file
    DB::WriteBufferFromFile plain_buf(
        "./snapshots/" + snapshot_files[0], DB::DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY);
    plain_buf.truncate(34);
    plain_buf.finalize();

    EXPECT_THROW(manager.restoreFromLatestSnapshot(), DB::Exception);
}

TYPED_TEST(CoordinationTest, TestStorageSnapshotDifferentCompressions)
{
    ChangelogDirTest test("./snapshots");
    this->setSnapshotDirectory("./snapshots");

    using Storage [[maybe_unused]] = DB::KeeperStorage;


    DB::KeeperSnapshotManager manager(3, this->keeper_context, this->enable_compression);

    Storage storage(500, "", this->keeper_context);
    addNode(storage, "/hello1", "world", 1);
    addNode(storage, "/hello2", "somedata", 3);
    storage.session_id_counter = 5;
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 2;
    storage.committed_ephemerals[3] = {"/hello2"};
    storage.committed_ephemerals[1] = {"/hello1"};
    storage.getSessionID(130);
    storage.getSessionID(130);

    DB::KeeperStorageSnapshot snapshot(&storage, 2, nullptr, this->keeper_context->getWriteSnapshotVersion());

    auto buf = manager.serializeSnapshotToBuffer(snapshot);
    manager.serializeSnapshotBufferToDisk(*buf, 2);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 2).size(), 1);

    DB::KeeperSnapshotManager new_manager(3, this->keeper_context, !this->enable_compression);

    auto debuf = new_manager.deserializeSnapshotBufferFromDisk(2);

    auto deser_result = new_manager.deserializeSnapshotFromBuffer(debuf);
    const auto & restored_storage = deser_result.storage;

    EXPECT_EQ(restored_storage->container.size(), 6);
    EXPECT_EQ(restored_storage->container.getValue("/").getChildren().size(), 3);
    EXPECT_EQ(restored_storage->container.getValue("/hello1").getChildren().size(), 0);
    EXPECT_EQ(restored_storage->container.getValue("/hello2").getChildren().size(), 0);

    EXPECT_EQ(restored_storage->container.getValue("/").getData(), "");
    EXPECT_EQ(restored_storage->container.getValue("/hello1").getData(), "world");
    EXPECT_EQ(restored_storage->container.getValue("/hello2").getData(), "somedata");
    EXPECT_EQ(restored_storage->session_id_counter, 7);
    EXPECT_EQ(restored_storage->getZXID(), 2);
    EXPECT_EQ(restored_storage->committed_ephemerals.size(), 2);
    EXPECT_EQ(restored_storage->committed_ephemerals[3].size(), 1);
    EXPECT_EQ(restored_storage->committed_ephemerals[1].size(), 1);
    EXPECT_EQ(restored_storage->session_and_timeout.size(), 2);
}

TYPED_TEST(CoordinationTest, TestStorageSnapshotEqual)
{
    ChangelogDirTest test("./snapshots");
    this->setSnapshotDirectory("./snapshots");

    using Storage [[maybe_unused]] = DB::KeeperStorage;


    std::optional<UInt128> snapshot_hash;
    for (size_t i = 0; i < 15; ++i)
    {
        DB::KeeperSnapshotManager manager(3, this->keeper_context, this->enable_compression);

        Storage storage(500, "", this->keeper_context);
        addNode(storage, "/hello", "");
        for (size_t j = 0; j < 5000; ++j)
        {
            addNode(storage, "/hello_" + std::to_string(j), "world", 1);
            addNode(storage, "/hello/somepath_" + std::to_string(j), "somedata", 3);
        }

        storage.session_id_counter = 5;

        storage.committed_ephemerals[3] = {"/hello"};
        storage.committed_ephemerals[1] = {"/hello/somepath"};

        for (size_t j = 0; j < 3333; ++j)
            storage.getSessionID(130 * j);

        DB::KeeperStorageSnapshot snapshot(&storage, storage.getZXID(), nullptr, this->keeper_context->getWriteSnapshotVersion());

        auto buf = manager.serializeSnapshotToBuffer(snapshot);

        auto new_hash = sipHash128(reinterpret_cast<char *>(buf->data()), buf->size());
        if (!snapshot_hash.has_value())
        {
            snapshot_hash = new_hash;
        }
        else
        {
            EXPECT_EQ(*snapshot_hash, new_hash);
        }
    }
}

TYPED_TEST(CoordinationTest, TestStorageSnapshotBlockACL)
{
    ChangelogDirTest test("./snapshots");
    this->setSnapshotDirectory("./snapshots");

    using Storage [[maybe_unused]] = DB::KeeperStorage;


    DB::KeeperSnapshotManager manager(3, this->keeper_context, this->enable_compression);

    Storage storage(500, "", this->keeper_context);
    static constexpr std::string_view path = "/hello";
    DB::ACLId acl_id = storage.acl_map.convertACLs({{1, "digest", "user1:pwd"}});
    EXPECT_NE(acl_id, 0);
    addNode(storage, std::string{path}, "world", /*ephemeral_owner=*/0, acl_id);
    DB::KeeperStorageSnapshot snapshot(&storage, 50, nullptr, this->keeper_context->getWriteSnapshotVersion());
    auto buf = manager.serializeSnapshotToBuffer(snapshot);
    manager.serializeSnapshotBufferToDisk(*buf, 50);

    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 50).size(), 1);
    {
        auto debuf = manager.deserializeSnapshotBufferFromDisk(50);
        auto deser_result = manager.deserializeSnapshotFromBuffer(debuf);
        const auto & restored_storage = deser_result.storage;

        EXPECT_EQ(restored_storage->container.size(), 5);
        EXPECT_EQ(restored_storage->container.getValue(path).acl_id, acl_id);
    }

    {
        this->keeper_context->setBlockACL(true);
        auto debuf = manager.deserializeSnapshotBufferFromDisk(50);
        auto deser_result = manager.deserializeSnapshotFromBuffer(debuf);
        const auto & restored_storage = deser_result.storage;

        EXPECT_EQ(restored_storage->container.size(), 5);
        EXPECT_EQ(restored_storage->container.getValue(path).acl_id, 0);
    }
}

static DB::KeeperContextPtr makeFollowerContext(int idx)
{
    auto settings = std::make_shared<DB::CoordinationSettings>();
    auto ctx = std::make_shared<DB::KeeperContext>(true, settings);
    ctx->setLocalLogsPreprocessed();
    ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>(
        fmt::format("SnapshotDisk_{}", idx), fmt::format("./snapshots_{}", idx)));
    return ctx;
}

static std::string runFollower(int idx, DB::KeeperStateMachine & leader, nuraft::snapshot & s)
{
    fs::create_directory(fmt::format("./snapshots_{}", idx));
    SCOPE_EXIT({
        fs::remove_all(fmt::format("./snapshots_{}", idx));
    });

    auto ctx = makeFollowerContext(idx);
    DB::SnapshotsQueue snapshots_queue{1};
    auto follower = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
    follower->init();

    void * user_snp_ctx = nullptr;
    uint64_t obj_id = 0;
    bool is_last = false;
    while (!is_last)
    {
        nuraft::ptr<nuraft::buffer> data_out;
        bool is_first = (obj_id == 0);
        if (leader.read_logical_snp_obj(s, user_snp_ctx, obj_id, data_out, is_last) < 0)
            break;
        std::this_thread::yield(); /// let other follower threads read from the already-loaded part
        follower->save_logical_snp_obj(s, obj_id, *data_out, is_first, is_last);
    }
    leader.free_user_snp_ctx(user_snp_ctx);

    EXPECT_TRUE(follower->apply_snapshot(s));
    return std::string(follower->getStorageUnsafe().container.getValue("/hello").getData());
}

static DB::KeeperContextPtr makeMemoryContextForSnapshotApply(const std::string & snapshots_path, const std::string & log_path = "")
{
    auto settings = std::make_shared<DB::CoordinationSettings>();
    (*settings)[DB::CoordinationSetting::compress_snapshots_with_zstd_format] = true;
    auto ctx = std::make_shared<DB::KeeperContext>(true, settings);
    ctx->setLocalLogsPreprocessed();
    ctx->setDigestEnabled(true);
    ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapshotDisk", snapshots_path));
    if (!log_path.empty())
        ctx->setLogDisk(std::make_shared<DB::DiskLocal>("LogDisk", log_path));
    return ctx;
}

static LogEntryPtr makeCreateEntry(
    DB::KeeperStateMachine & state_machine,
    const std::string & path,
    const std::string & data)
{
    auto request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
    request->path = path;
    request->data = data;
    return getLogEntryFromZKRequest(0, 1, state_machine.getNextZxid(), request);
}

static LogEntryPtr makeSetEntry(
    DB::KeeperStateMachine & state_machine,
    const std::string & path,
    const std::string & data)
{
    auto request = std::make_shared<Coordination::ZooKeeperSetRequest>();
    request->path = path;
    request->data = data;
    request->version = -1;
    return getLogEntryFromZKRequest(0, 1, state_machine.getNextZxid(), request);
}

static LogEntryPtr makeEphemeralCreateEntry(
    DB::KeeperStateMachine & state_machine,
    int64_t session_id,
    const std::string & path,
    const std::string & data)
{
    auto request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
    request->path = path;
    request->data = data;
    request->is_ephemeral = true;
    return getLogEntryFromZKRequest(0, session_id, state_machine.getNextZxid(), request);
}

static LogEntryPtr makeCloseEntry(DB::KeeperStateMachine & state_machine, int64_t session_id)
{
    auto request = std::make_shared<Coordination::ZooKeeperCloseRequest>();
    return getLogEntryFromZKRequest(0, session_id, state_machine.getNextZxid(), request);
}

static nuraft::ptr<nuraft::buffer> makeSnapshotBufferFromStorage(
    DB::KeeperStorage & storage,
    uint64_t last_log_idx,
    const DB::KeeperContextPtr & keeper_context)
{
    DB::KeeperSnapshotManager manager(3, keeper_context, true);
    DB::KeeperStorageSnapshot snapshot(
        &storage, last_log_idx, nullptr, keeper_context->getWriteSnapshotVersion());
    return manager.serializeSnapshotToBuffer(snapshot);
}

static nuraft::ptr<nuraft::buffer> sliceBuffer(const nuraft::ptr<nuraft::buffer> & source, size_t offset, size_t size)
{
    auto result = nuraft::buffer::alloc(size);
    std::memcpy(result->data_begin(), source->data_begin() + offset, size);
    result->pos(0);
    return result;
}

static void writeSnapshotBufferToFile(const DB::DiskPtr & disk, const std::string & path, const nuraft::ptr<nuraft::buffer> & buffer)
{
    auto out = disk->writeFile(path);
    out->write(reinterpret_cast<const char *>(buffer->data_begin()), buffer->size());
    out->finalize();
}

static void saveSingleObjectSnapshot(
    DB::KeeperStateMachine & state_machine,
    nuraft::snapshot & snapshot,
    nuraft::ptr<nuraft::buffer> snapshot_buf)
{
    uint64_t obj_id = 0;
    state_machine.save_logical_snp_obj(snapshot, obj_id, *snapshot_buf, /*is_first_obj=*/true, /*is_last_obj=*/true);
    ASSERT_EQ(obj_id, 1);
}

/// Defined below in the anonymous namespace; serializes over an isolated throwaway disk, so it
/// has no snapshot-directory scan side effects and is safe to call mid-test (e.g. while a
/// snapshot write is blocked).
namespace
{
nuraft::ptr<nuraft::buffer> makeInstallBuffer(
    DB::KeeperStorage & storage, uint64_t idx, const DB::KeeperContextPtr & version_ctx);
}

static nuraft::ptr<nuraft::buffer> makeSingleNodeSnapshotBuffer(
    const DB::KeeperContextPtr & ctx,
    uint64_t log_idx,
    const std::string & node_path,
    const std::string & node_data)
{
    DB::KeeperStorage storage(500, "", ctx);
    addNode(storage, node_path, node_data);
    return makeInstallBuffer(storage, log_idx, ctx);
}

/// Drain a queued create_snapshot task synchronously (the snapshot thread's job in production)
/// and return the resulting file info, mirroring the pattern used by the cleanup tests.
static DB::SnapshotFileInfoPtr executeCreateSnapshotTask(
    DB::KeeperStateMachine & state_machine,
    DB::SnapshotsQueue & snapshots_queue,
    nuraft::snapshot & s)
{
    nuraft::async_result<bool>::handler_type when_done = [](bool &, nuraft::ptr<std::exception> &) {};
    state_machine.create_snapshot(s, when_done);
    DB::CreateSnapshotTask snapshot_task;
    EXPECT_TRUE(snapshots_queue.pop(snapshot_task));
    return snapshot_task.create_snapshot(std::move(snapshot_task.snapshot), /*execute_only_cleanup=*/false);
}

TEST(KeeperMemorySnapshotApplyTest, ApplySnapshotReplacesCommittedState)
{
    ChangelogDirTest snapshots("./snapshots");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots");
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    auto old_entry = makeCreateEntry(*state_machine, "/old", "old");
    state_machine->pre_commit(1, old_entry->get_buf());
    state_machine->commit(1, old_entry->get_buf());

    DB::KeeperStorage snapshot_storage(500, "", ctx);
    addNode(snapshot_storage, "/committed", "from_snapshot");
    TSA_SUPPRESS_WARNING_FOR_WRITE(snapshot_storage.zxid) = 1;

    nuraft::snapshot snapshot(1, 0, std::make_shared<nuraft::cluster_config>());
    auto snapshot_buf = makeSnapshotBufferFromStorage(snapshot_storage, 1, ctx);
    saveSingleObjectSnapshot(*state_machine, snapshot, snapshot_buf);

    EXPECT_TRUE(state_machine->apply_snapshot(snapshot));

    auto & storage = state_machine->getStorageUnsafe();
    ASSERT_TRUE(storage.container.contains("/committed"));
    EXPECT_EQ(std::string(storage.container.getValue("/committed").getData()), "from_snapshot");
    EXPECT_FALSE(storage.container.contains("/old"));
    EXPECT_EQ(state_machine->last_commit_index(), 1);
}

TEST(KeeperMemorySnapshotApplyTest, ApplySnapshotPreservesPreprocessedTailAboveSnapshotIndex)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest test("./logs");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./logs");
    DB::KeeperLogStore changelog({}, {}, ctx);
    changelog.init(0, 1000);
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();
    state_machine->setLogStore(&changelog);

    auto base_entry = makeCreateEntry(*state_machine, "/committed", "base");
    changelog.append(base_entry);
    state_machine->pre_commit(1, base_entry->get_buf());
    state_machine->commit(1, base_entry->get_buf());

    auto set_entry = makeSetEntry(*state_machine, "/committed", "tail_update");
    changelog.append(set_entry);
    state_machine->pre_commit(2, set_entry->get_buf());

    auto create_tail_entry = makeCreateEntry(*state_machine, "/tail", "tail_create");
    changelog.append(create_tail_entry);
    state_machine->pre_commit(3, create_tail_entry->get_buf());

    changelog.end_of_append_batch(0, 0);

    DB::KeeperStorage snapshot_storage(500, "", ctx);
    addNode(snapshot_storage, "/committed", "base");
    TSA_SUPPRESS_WARNING_FOR_WRITE(snapshot_storage.zxid) = 1;

    nuraft::snapshot snapshot(1, 0, std::make_shared<nuraft::cluster_config>());
    auto snapshot_buf = makeSnapshotBufferFromStorage(snapshot_storage, 1, ctx);
    saveSingleObjectSnapshot(*state_machine, snapshot, snapshot_buf);

    EXPECT_TRUE(state_machine->apply_snapshot(snapshot));
    ASSERT_TRUE(state_machine->getStorageUnsafe().container.contains("/committed"));

    state_machine->commit(2, set_entry->get_buf());
    state_machine->commit(3, create_tail_entry->get_buf());

    auto & storage = state_machine->getStorageUnsafe();
    ASSERT_TRUE(storage.container.contains("/committed"));
    ASSERT_TRUE(storage.container.contains("/tail"));
    EXPECT_EQ(std::string(storage.container.getValue("/committed").getData()), "tail_update");
    EXPECT_EQ(std::string(storage.container.getValue("/tail").getData()), "tail_create");
    EXPECT_EQ(state_machine->last_commit_index(), 3);
}

TEST(KeeperMemorySnapshotApplyTest, ApplySnapshotPreservesEphemeralTailForClosePreprocess)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest test("./logs");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./logs");
    DB::KeeperLogStore changelog({}, {}, ctx);
    changelog.init(0, 1000);
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();
    state_machine->setLogStore(&changelog);

    auto base_entry = makeCreateEntry(*state_machine, "/base", "base");
    changelog.append(base_entry);
    state_machine->pre_commit(1, base_entry->get_buf());
    state_machine->commit(1, base_entry->get_buf());

    static constexpr int64_t session_id = 7;
    auto ephemeral_entry = makeEphemeralCreateEntry(*state_machine, session_id, "/ephemeral", "tail_ephemeral");
    changelog.append(ephemeral_entry);
    state_machine->pre_commit(2, ephemeral_entry->get_buf());

    changelog.end_of_append_batch(0, 0);

    DB::KeeperStorage snapshot_storage(500, "", ctx);
    addNode(snapshot_storage, "/base", "base");
    TSA_SUPPRESS_WARNING_FOR_WRITE(snapshot_storage.zxid) = 1;

    nuraft::snapshot snapshot(1, 0, std::make_shared<nuraft::cluster_config>());
    auto snapshot_buf = makeSnapshotBufferFromStorage(snapshot_storage, 1, ctx);
    saveSingleObjectSnapshot(*state_machine, snapshot, snapshot_buf);

    EXPECT_TRUE(state_machine->apply_snapshot(snapshot));

    auto close_entry = makeCloseEntry(*state_machine, session_id);
    changelog.append(close_entry);
    state_machine->pre_commit(3, close_entry->get_buf());

    state_machine->commit(2, ephemeral_entry->get_buf());
    ASSERT_TRUE(state_machine->getStorageUnsafe().container.contains("/ephemeral"));

    state_machine->commit(3, close_entry->get_buf());
    EXPECT_FALSE(state_machine->getStorageUnsafe().container.contains("/ephemeral"));
    EXPECT_EQ(state_machine->last_commit_index(), 3);
}

TEST(KeeperMemorySnapshotApplyTest, CorruptSnapshotPrefixFailsBeforeDroppingStorage)
{
    ChangelogDirTest snapshots("./snapshots");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots");
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    auto old_entry = makeCreateEntry(*state_machine, "/old", "old");
    state_machine->pre_commit(1, old_entry->get_buf());
    state_machine->commit(1, old_entry->get_buf());

    DB::KeeperStorage snapshot_storage(500, "", ctx);
    addNode(snapshot_storage, "/replacement", "replacement");
    TSA_SUPPRESS_WARNING_FOR_WRITE(snapshot_storage.zxid) = 1;

    nuraft::snapshot snapshot(1, 0, std::make_shared<nuraft::cluster_config>());
    auto snapshot_buf = makeSnapshotBufferFromStorage(snapshot_storage, 1, ctx);
    saveSingleObjectSnapshot(*state_machine, snapshot, snapshot_buf);

    auto snapshot_files = snapshotFilesForIdx("./snapshots", 1);
    ASSERT_EQ(snapshot_files.size(), 1);
    DB::WriteBufferFromFile plain_buf(
        "./snapshots/" + snapshot_files[0],
        DB::DBMS_DEFAULT_BUFFER_SIZE,
        O_APPEND | O_CREAT | O_WRONLY);
    plain_buf.truncate(0);
    plain_buf.finalize();

    EXPECT_THROW(state_machine->apply_snapshot(snapshot), DB::Exception);

    auto & storage = state_machine->getStorageUnsafe();
    ASSERT_TRUE(storage.container.contains("/old"));
    EXPECT_EQ(std::string(storage.container.getValue("/old").getData()), "old");
}

namespace
{
/// Serialize a snapshot of `storage` for log index `idx` into a buffer, using a manager over an
/// ISOLATED empty disk — a throwaway manager over the real snapshot disk would run a ctor
/// retention pass pruning the on-disk snapshots the test asserts about. Buffer content is
/// identical (serialization is disk-independent).
nuraft::ptr<nuraft::buffer> makeInstallBuffer(
    DB::KeeperStorage & storage, uint64_t idx, const DB::KeeperContextPtr & version_ctx)
{
    static int iso_counter = 0;
    const std::string iso_path = fmt::format("./iso_buf_{}", iso_counter++);
    fs::remove_all(iso_path);
    fs::create_directory(iso_path);
    SCOPE_EXIT({ fs::remove_all(iso_path); });

    auto iso_settings = std::make_shared<DB::CoordinationSettings>();
    auto iso_ctx = std::make_shared<DB::KeeperContext>(true, iso_settings);
    iso_ctx->setLocalLogsPreprocessed();
    iso_ctx->setDigestEnabled(true);
    iso_ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("IsoBufDisk", iso_path));

    DB::KeeperSnapshotManager manager(3, iso_ctx, true);
    DB::KeeperStorageSnapshot snapshot(
        &storage, idx, nullptr, version_ctx->getWriteSnapshotVersion());
    return manager.serializeSnapshotToBuffer(snapshot);
}

/// Build a state-equivalent "install" snapshot file for `idx` (term 0) holding a single marker
/// node and save it through the receive path without applying it. Mirrors a fully received but
/// not-yet-applied snapshot install.
void saveInstallSnapshot(
    DB::KeeperStateMachine & state_machine,
    const DB::KeeperContextPtr & ctx,
    uint64_t idx,
    const std::string & marker)
{
    DB::KeeperStorage storage(500, "", ctx);
    addNode(storage, marker, marker);
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = idx;
    nuraft::snapshot snap(idx, 0, std::make_shared<nuraft::cluster_config>());
    auto buf = makeInstallBuffer(storage, idx, ctx);
    saveSingleObjectSnapshot(state_machine, snap, buf);
}
}

/// HARD CONSTRAINT: snapshot 5 saved but never applied (leader died), then a new leader installs
/// the older snapshot 3 — must converge. A naive "only stamp the mark if monotonic" guard would
/// skip the apply of 3 and silently diverge.
TEST(KeeperMemorySnapshotApplyTest, InterruptedInstallThenOlderReinstallConverges)
{
    ChangelogDirTest snapshots("./snapshots");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots");
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    auto old_entry = makeCreateEntry(*state_machine, "/old", "old");
    state_machine->pre_commit(1, old_entry->get_buf());
    state_machine->commit(1, old_entry->get_buf());

    /// Local create win at 1 -> mark 1.
    nuraft::snapshot s1(1, 0, std::make_shared<nuraft::cluster_config>());
    {
        auto info = executeCreateSnapshotTask(*state_machine, snapshots_queue, s1);
        ASSERT_NE(info, nullptr);
    }
    ASSERT_NE(state_machine->last_snapshot(), nullptr);
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 1);

    /// Save a full install of 5, do NOT apply it (leader died).
    saveInstallSnapshot(*state_machine, ctx, 5, "/from_snap5");
    /// The fix's discriminator: the high-water mark stays at 1 (master would regress to 5).
    ASSERT_NE(state_machine->last_snapshot(), nullptr);
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 1);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 5).size(), 1u);

    /// New leader installs the older snapshot 3, then applies it.
    saveInstallSnapshot(*state_machine, ctx, 3, "/from_snap3");
    nuraft::snapshot s3(3, 0, std::make_shared<nuraft::cluster_config>());
    EXPECT_TRUE(state_machine->apply_snapshot(s3));

    auto & storage = state_machine->getStorageUnsafe();
    EXPECT_TRUE(storage.container.contains("/from_snap3"));
    EXPECT_FALSE(storage.container.contains("/from_snap5"));
    EXPECT_FALSE(storage.container.contains("/old"));
    EXPECT_EQ(state_machine->last_commit_index(), 3);
    ASSERT_NE(state_machine->last_snapshot(), nullptr);
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 3);

    /// Replay continues from idx 4.
    auto tail_entry = makeCreateEntry(*state_machine, "/tail", "tail");
    state_machine->pre_commit(4, tail_entry->get_buf());
    state_machine->commit(4, tail_entry->get_buf());
    EXPECT_TRUE(state_machine->getStorageUnsafe().container.contains("/tail"));
    EXPECT_EQ(state_machine->last_commit_index(), 4);
}

/// A stale duplicate install at a lower index must not regress the high-water mark or clobber the
/// cached snapshot size.
TEST(KeeperMemorySnapshotApplyTest, StaleDuplicateInstallKeepsHighWaterMarkAndSize)
{
    ChangelogDirTest snapshots("./snapshots");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots");
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    auto e1 = makeCreateEntry(*state_machine, "/n1", "v1");
    state_machine->pre_commit(1, e1->get_buf());
    state_machine->commit(1, e1->get_buf());
    auto e2 = makeCreateEntry(*state_machine, "/n2", "v2");
    state_machine->pre_commit(2, e2->get_buf());
    state_machine->commit(2, e2->get_buf());

    nuraft::snapshot s2(2, 0, std::make_shared<nuraft::cluster_config>());
    {
        auto info = executeCreateSnapshotTask(*state_machine, snapshots_queue, s2);
        ASSERT_NE(info, nullptr);
    }
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 2);
    const uint64_t expected_size = state_machine->getLatestSnapshotSize();
    EXPECT_GT(expected_size, 0u);

    /// Save a stale install of 1 -> map {1,2}, pending 1, mark still 2, size unchanged.
    saveInstallSnapshot(*state_machine, ctx, 1, "/stale");
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 2);
    EXPECT_EQ(state_machine->getLatestSnapshotSize(), expected_size);

    /// A later local create at 3 still works and advances the mark.
    auto e3 = makeCreateEntry(*state_machine, "/n3", "v3");
    state_machine->pre_commit(3, e3->get_buf());
    state_machine->commit(3, e3->get_buf());
    nuraft::snapshot s3(3, 0, std::make_shared<nuraft::cluster_config>());
    {
        auto info = executeCreateSnapshotTask(*state_machine, snapshots_queue, s3);
        ASSERT_NE(info, nullptr);
    }
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 3).size(), 1u);
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 3);
}

/// After a restart, a snapshot that was saved but never applied is the newest disk snapshot and
/// `init` recovers from it (a fully saved snapshot is a valid committed prefix).
TEST(KeeperMemorySnapshotApplyTest, RestartAfterSavedButNotAppliedRecoversNewestDiskSnapshot)
{
    ChangelogDirTest snapshots("./snapshots");

    {
        auto ctx = makeMemoryContextForSnapshotApply("./snapshots");
        DB::SnapshotsQueue snapshots_queue{1};
        auto sm1 = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
        sm1->init();

        auto e1 = makeCreateEntry(*sm1, "/old", "old");
        sm1->pre_commit(1, e1->get_buf());
        sm1->commit(1, e1->get_buf());

        saveInstallSnapshot(*sm1, ctx, 5, "/from_snap5");
        EXPECT_EQ(snapshotFilesForIdx("./snapshots", 5).size(), 1u);
        /// sm1 destroyed without applying snapshot 5.
    }

    {
        auto ctx2 = makeMemoryContextForSnapshotApply("./snapshots");
        DB::SnapshotsQueue snapshots_queue2{1};
        auto sm2 = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue2, ctx2, nullptr);
        sm2->init();

        ASSERT_NE(sm2->last_snapshot(), nullptr);
        EXPECT_EQ(sm2->last_snapshot()->get_last_log_idx(), 5);
        auto & storage = sm2->getStorageUnsafe();
        EXPECT_TRUE(storage.container.contains("/from_snap5"));
        EXPECT_FALSE(storage.container.contains("/old"));
        EXPECT_EQ(sm2->last_commit_index(), 5);

        /// A stale save at 3 after restart does not regress the mark.
        saveInstallSnapshot(*sm2, ctx2, 3, "/from_snap3");
        EXPECT_EQ(sm2->last_snapshot()->get_last_log_idx(), 5);
    }
}

/// The high-water mark's backing file stays servable (protected from retention) and the mark never
/// regresses under a sequence of saved-but-not-applied installs.
TEST(KeeperMemorySnapshotApplyTest, HighWaterMarkStaysServableAndNeverRegressesUnderReceiveSequences)
{
    ChangelogDirTest snapshots("./snapshots");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots");
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    /// (1) commits 1-2, create win at 2 -> map {2}, mark/protected 2.
    auto e1 = makeCreateEntry(*state_machine, "/n1", "v1");
    state_machine->pre_commit(1, e1->get_buf());
    state_machine->commit(1, e1->get_buf());
    auto e2 = makeCreateEntry(*state_machine, "/n2", "v2");
    state_machine->pre_commit(2, e2->get_buf());
    state_machine->commit(2, e2->get_buf());
    nuraft::snapshot s2(2, 0, std::make_shared<nuraft::cluster_config>());
    {
        auto info = executeCreateSnapshotTask(*state_machine, snapshots_queue, s2);
        ASSERT_NE(info, nullptr);
    }
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 2);

    /// (2) save full 5 then 3 -> {2,3,5}, mark 2 after each.
    saveInstallSnapshot(*state_machine, ctx, 5, "/snap5");
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 2);
    saveInstallSnapshot(*state_machine, ctx, 3, "/snap3");
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 2);

    /// (3) save 4 -> {2,3,4,5}: candidate 2 pinned, 4 <= keep(3)+1 -> nothing pruned.
    saveInstallSnapshot(*state_machine, ctx, 4, "/snap4");
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 2).size(), 1u);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 3).size(), 1u);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 4).size(), 1u);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 5).size(), 1u);

    /// (4) save 6 -> remove 3 -> {2,4,5,6}; the mark's file (2) stays servable.
    saveInstallSnapshot(*state_machine, ctx, 6, "/snap6");
    EXPECT_TRUE(snapshotFilesForIdx("./snapshots", 3).empty());
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 2).size(), 1u);
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 2);

    /// (5) create-skip servability: returns the mark's file (snapshot_2), not the map max.
    {
        nuraft::snapshot s2_again(2, 0, std::make_shared<nuraft::cluster_config>());
        auto info = executeCreateSnapshotTask(*state_machine, snapshots_queue, s2_again);
        ASSERT_NE(info, nullptr);
        EXPECT_EQ(DB::getLogIdxFromSnapshotPath(info->path), 2u);
    }

    /// (6) apply snapshot 6 -> mark advances to 6.
    {
        nuraft::snapshot s6(6, 0, std::make_shared<nuraft::cluster_config>());
        EXPECT_TRUE(state_machine->apply_snapshot(s6));
        EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 6);
    }

    /// (7) protection moved to 6: save 7 -> remove 2 then 4 -> {5,6,7}; mark 6 + its file survive.
    saveInstallSnapshot(*state_machine, ctx, 7, "/snap7");
    EXPECT_TRUE(snapshotFilesForIdx("./snapshots", 2).empty());
    EXPECT_TRUE(snapshotFilesForIdx("./snapshots", 4).empty());
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 6).size(), 1u);
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 6);
}

/// A covered stale apply (s.idx < pending, but local commits already cover s.idx) skips without
/// divergence and leaves the pending intact for its own apply.
TEST(KeeperMemorySnapshotApplyTest, CoveredStaleApplySkipsWithoutDivergence)
{
    ChangelogDirTest snapshots("./snapshots");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots");
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    for (uint64_t idx = 1; idx <= 3; ++idx)
    {
        auto entry = makeCreateEntry(*state_machine, fmt::format("/n{}", idx), "v");
        state_machine->pre_commit(idx, entry->get_buf());
        state_machine->commit(idx, entry->get_buf());
    }
    EXPECT_EQ(state_machine->last_commit_index(), 3);

    saveInstallSnapshot(*state_machine, ctx, 5, "/from_snap5"); /// pending 5, mark null

    /// Covered skip: 3 < pending 5, but last committed (3) >= 3.
    nuraft::snapshot s3(3, 0, std::make_shared<nuraft::cluster_config>());
    EXPECT_TRUE(state_machine->apply_snapshot(s3));
    EXPECT_TRUE(state_machine->getStorageUnsafe().container.contains("/n1"));
    EXPECT_FALSE(state_machine->getStorageUnsafe().container.contains("/from_snap5"));
    EXPECT_EQ(state_machine->last_commit_index(), 3);
    EXPECT_EQ(state_machine->last_snapshot(), nullptr);

    /// The pending survived the covered skip and applies its own snapshot.
    nuraft::snapshot s5(5, 0, std::make_shared<nuraft::cluster_config>());
    EXPECT_TRUE(state_machine->apply_snapshot(s5));
    EXPECT_TRUE(state_machine->getStorageUnsafe().container.contains("/from_snap5"));
    EXPECT_EQ(state_machine->last_commit_index(), 5);
    ASSERT_NE(state_machine->last_snapshot(), nullptr);
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 5);
}

/// A queued same-index create that drains after a state-equivalent install at the same index was
/// saved (but not applied) writes a fresh unique file, then publish adopts the registered install
/// entry and retires+unlinks the loser. The armed disk (failing any write whose path matches the
/// registered file's name) is the discriminator: a regression that rewrites it in place would throw.
TEST(KeeperMemorySnapshotApplyTest, QueuedSameIndexCreateAdoptsRegisteredInstallSnapshot)
{
    ChangelogDirTest snapshots("./snapshots");

    {
        auto settings = std::make_shared<DB::CoordinationSettings>();
        (*settings)[DB::CoordinationSetting::compress_snapshots_with_zstd_format] = true;
        auto ctx = std::make_shared<DB::KeeperContext>(true, settings);
        ctx->setLocalLogsPreprocessed();
        ctx->setDigestEnabled(true);
        /// The registered idx-5 file gets a unique name; its prefix is set after the install save.
        auto throwing_disk = std::make_shared<ThrowingSnapshotDisk>(
            "SnapshotDisk", "./snapshots", "__placeholder_set_after_install__", SnapshotDiskFailureMode::OpenFileAfterCreate);
        ctx->setSnapshotDisk(throwing_disk);
        throwing_disk->disarm();

        DB::SnapshotsQueue snapshots_queue{1};
        auto sm1 = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
        sm1->init();

        for (uint64_t idx = 1; idx <= 5; ++idx)
        {
            auto entry = makeCreateEntry(*sm1, fmt::format("/n{}", idx), "v");
            sm1->pre_commit(idx, entry->get_buf());
            sm1->commit(idx, entry->get_buf());

            if (idx == 1)
            {
                /// Local create win at 1 -> mark 1.
                nuraft::snapshot s1(1, 0, std::make_shared<nuraft::cluster_config>());
                auto info = executeCreateSnapshotTask(*sm1, snapshots_queue, s1);
                ASSERT_NE(info, nullptr);
            }
        }
        EXPECT_EQ(sm1->last_snapshot()->get_last_log_idx(), 1);

        /// Save a state-equivalent install of 5 (committed prefix /n1../n5); no apply (NuRaft covered skip).
        DB::KeeperStorage install5(500, "", ctx);
        for (uint64_t idx = 1; idx <= 5; ++idx)
            addNode(install5, fmt::format("/n{}", idx), "v");
        TSA_SUPPRESS_WARNING_FOR_WRITE(install5.zxid) = 5;
        nuraft::snapshot s5_save(5, 0, std::make_shared<nuraft::cluster_config>());
        auto buf5 = makeInstallBuffer(install5, 5, ctx);
        saveSingleObjectSnapshot(*sm1, s5_save, buf5);
        EXPECT_EQ(sm1->last_snapshot()->get_last_log_idx(), 1); /// mark still 1, pending 5

        /// Capture the install's unique file name and arm the disk to fail any write to it.
        auto registered_files = snapshotFilesForIdx("./snapshots", 5);
        ASSERT_EQ(registered_files.size(), 1u);
        const std::string & registered_name = registered_files.at(0);
        throwing_disk->setFailPathPrefix(registered_name);
        throwing_disk->arm();
        nuraft::snapshot s5_create(5, 0, std::make_shared<nuraft::cluster_config>());
        auto info = executeCreateSnapshotTask(*sm1, snapshots_queue, s5_create);
        ASSERT_NE(info, nullptr); /// adopted the registered install; the fresh write lost and was retired
        EXPECT_EQ(fs::path(info->path).filename().string(), registered_name);
        EXPECT_EQ(snapshotFilesForIdx("./snapshots", 5).size(), 1u); /// loser unlinked, registered file present
        ASSERT_NE(sm1->last_snapshot(), nullptr);
        EXPECT_EQ(sm1->last_snapshot()->get_last_log_idx(), 5);

        auto & storage = sm1->getStorageUnsafe();
        for (uint64_t idx = 1; idx <= 5; ++idx)
            EXPECT_TRUE(storage.container.contains(fmt::format("/n{}", idx)));
    }

    /// Restart with a plain disk: the adopted snapshot 5 is the newest and recovers cleanly.
    {
        auto ctx2 = makeMemoryContextForSnapshotApply("./snapshots");
        DB::SnapshotsQueue snapshots_queue2{1};
        auto sm2 = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue2, ctx2, nullptr);
        sm2->init();
        ASSERT_NE(sm2->last_snapshot(), nullptr);
        EXPECT_EQ(sm2->last_snapshot()->get_last_log_idx(), 5);
        auto & storage = sm2->getStorageUnsafe();
        for (uint64_t idx = 1; idx <= 5; ++idx)
            EXPECT_TRUE(storage.container.contains(fmt::format("/n{}", idx)));
    }
}

/// A create at or below the mark returns the file backing the high-water mark, not the manager's
/// map max (which may be a saved-but-not-applied install feeding S3/shutdown uploads).
TEST(KeeperMemorySnapshotApplyTest, CreateSkipReturnsHighWaterMarkFileNotMapMax)
{
    ChangelogDirTest snapshots("./snapshots");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots");
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    auto e1 = makeCreateEntry(*state_machine, "/n1", "v1");
    state_machine->pre_commit(1, e1->get_buf());
    state_machine->commit(1, e1->get_buf());
    auto e2 = makeCreateEntry(*state_machine, "/n2", "v2");
    state_machine->pre_commit(2, e2->get_buf());
    state_machine->commit(2, e2->get_buf());

    nuraft::snapshot s2(2, 0, std::make_shared<nuraft::cluster_config>());
    {
        auto info = executeCreateSnapshotTask(*state_machine, snapshots_queue, s2);
        ASSERT_NE(info, nullptr);
    }
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 2);

    saveInstallSnapshot(*state_machine, ctx, 5, "/from_snap5"); /// map {2,5}, pending 5, mark 2

    /// Saved index 5 is the map max -> the size cache reflects snapshot_5.
    EXPECT_EQ(
        state_machine->getLatestSnapshotSize(),
        fs::file_size(fs::path("./snapshots") / snapshotFilesForIdx("./snapshots", 5).at(0)));

    nuraft::snapshot s2_again(2, 0, std::make_shared<nuraft::cluster_config>());
    auto info = executeCreateSnapshotTask(*state_machine, snapshots_queue, s2_again);
    ASSERT_NE(info, nullptr);
    EXPECT_EQ(DB::getLogIdxFromSnapshotPath(info->path), 2u); /// not snapshot_5
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 2);
}

/// A local create below saved-but-not-applied installs survives its own retention pass (the
/// just-written pin) and does not clobber the size cache (map-max guard).
TEST(KeeperMemorySnapshotApplyTest, LocalCreateBelowSavedInstallsSurvivesRetention)
{
    ChangelogDirTest snapshots("./snapshots");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots");
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    /// (1) commits 1-2, create win at 2 -> mark/protected 2.
    auto e1 = makeCreateEntry(*state_machine, "/n1", "v1");
    state_machine->pre_commit(1, e1->get_buf());
    state_machine->commit(1, e1->get_buf());
    auto e2 = makeCreateEntry(*state_machine, "/n2", "v2");
    state_machine->pre_commit(2, e2->get_buf());
    state_machine->commit(2, e2->get_buf());
    nuraft::snapshot s2(2, 0, std::make_shared<nuraft::cluster_config>());
    {
        auto info = executeCreateSnapshotTask(*state_machine, snapshots_queue, s2);
        ASSERT_NE(info, nullptr);
    }

    /// (2) save full 10,11,12 -> {2,10,11,12}; baseline size is snapshot_12's.
    saveInstallSnapshot(*state_machine, ctx, 10, "/snap10");
    saveInstallSnapshot(*state_machine, ctx, 11, "/snap11");
    saveInstallSnapshot(*state_machine, ctx, 12, "/snap12");
    const uint64_t size_12 = state_machine->getLatestSnapshotSize();
    EXPECT_EQ(size_12, fs::file_size(fs::path("./snapshots") / snapshotFilesForIdx("./snapshots", 12).at(0)));
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 2);

    /// (3) commits 3-5.
    for (uint64_t idx = 3; idx <= 5; ++idx)
    {
        auto entry = makeCreateEntry(*state_machine, fmt::format("/n{}", idx), "v");
        state_machine->pre_commit(idx, entry->get_buf());
        state_machine->commit(idx, entry->get_buf());
    }

    /// (4) local create at 5: write wins, advancing the mark (and protection) to 5 inside
    /// publishWrittenSnapshot before this pass's maintenance. The old mark's entry (idx 2) is
    /// therefore no longer protected and is pruned now -> {5,10,11,12}; the just-written 5 survives.
    nuraft::snapshot s5(5, 0, std::make_shared<nuraft::cluster_config>());
    {
        auto info = executeCreateSnapshotTask(*state_machine, snapshots_queue, s5);
        ASSERT_NE(info, nullptr);
    }
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 5).size(), 1u);
    EXPECT_TRUE(snapshotFilesForIdx("./snapshots", 2).empty());
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 5);
    /// Size guard: the create at 5 is below the map max (12) -> must not clobber the cache.
    EXPECT_EQ(state_machine->getLatestSnapshotSize(), size_12);

    /// (5) servability: a create at 5 again hits the skip branch and returns a live pin.
    {
        nuraft::snapshot s5_again(5, 0, std::make_shared<nuraft::cluster_config>());
        auto info = executeCreateSnapshotTask(*state_machine, snapshots_queue, s5_again);
        ASSERT_NE(info, nullptr);
        EXPECT_EQ(DB::getLogIdxFromSnapshotPath(info->path), 5u);
    }

    /// (6) convergence: save full 13 -> remove 10 -> {5,11,12,13}; idx 5 survives (protected).
    saveInstallSnapshot(*state_machine, ctx, 13, "/snap13");
    EXPECT_TRUE(snapshotFilesForIdx("./snapshots", 2).empty());
    EXPECT_TRUE(snapshotFilesForIdx("./snapshots", 10).empty());
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 5).size(), 1u);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 13).size(), 1u);
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 5);
    EXPECT_EQ(
        state_machine->getLatestSnapshotSize(),
        fs::file_size(fs::path("./snapshots") / snapshotFilesForIdx("./snapshots", 13).at(0)));
}

TYPED_TEST(CoordinationTest, TestReadSnapshotParallelMultiChunk)
{
    getContext(); /// needed for DiskObjectStorage background threads

    ChangelogDirTest snap_meta("./snapshots");
    ChangelogDirTest snap_obj("./snapshots_obj");

    using Storage [[maybe_unused]] = DB::KeeperStorage;

    auto leader_settings = std::make_shared<DB::CoordinationSettings>();
    (*leader_settings)[DB::CoordinationSetting::snapshot_transfer_chunk_size] = 10;
    auto leader_ctx = std::make_shared<DB::KeeperContext>(true, leader_settings);
    leader_ctx->setLocalLogsPreprocessed();

    auto [snap_disk, obj_storage] = createLocalObjectStorageDisk("./snapshots", "./snapshots_obj/");
    leader_ctx->setSnapshotDisk(snap_disk);

    DB::KeeperSnapshotManager manager(3, leader_ctx, this->enable_compression);
    Storage storage(500, "", leader_ctx);
    addNode(storage, "/hello", "world");
    DB::KeeperStorageSnapshot snap(&storage, 50, nullptr, leader_ctx->getWriteSnapshotVersion());
    auto snap_buf = manager.serializeSnapshotToBuffer(snap);
    manager.serializeSnapshotBufferToDisk(*snap_buf, 50);

    DB::SnapshotsQueue leader_snapshots_queue{1};
    auto leader = std::make_shared<DB::KeeperStateMachine>(
        nullptr, leader_snapshots_queue, leader_ctx, nullptr);
    leader->init();

    nuraft::snapshot s(50, 0, std::make_shared<nuraft::cluster_config>());

    const int reads_after_init = obj_storage->read_count.load();

    constexpr int num_threads = 10;
    std::vector<std::string> loaded_data(num_threads);
    {
        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        for (int i = 0; i < num_threads; ++i)
            threads.emplace_back([&, i] { loaded_data[i] = runFollower(i, *leader, s); });
        for (auto & t : threads)
            t.join();
    }
    for (int i = 0; i < num_threads; ++i)
        EXPECT_EQ(loaded_data[i], "world") << "thread " << i;

    EXPECT_EQ(obj_storage->read_count.load() - reads_after_init, 1);

    snap_disk->shutdown();
}

TYPED_TEST(CoordinationTest, TestStorageSnapshotTTLRoundTrip)
{
    using namespace Coordination;
    using Storage [[maybe_unused]] = DB::KeeperStorage;

    ChangelogDirTest test("./snapshots");
    this->setSnapshotDirectory("./snapshots");

    DB::KeeperSnapshotManager manager(3, this->keeper_context, this->enable_compression);
    Storage storage(500, "", this->keeper_context);

    const int64_t session_id = 1;
    const int64_t ttl_ms = 5000;
    int64_t zxid = 0;

    auto create_request = std::make_shared<ZooKeeperCreateRequest>();
    create_request->path = "/ttl_node";
    create_request->include_ttl = true;
    create_request->ttl = ttl_ms;

    storage.preprocessRequest(create_request, session_id, /*time=*/0, ++zxid);
    auto responses = storage.processRequest(create_request, session_id, zxid);
    ASSERT_EQ(responses[0].response->error, Error::ZOK);

    ASSERT_TRUE(storage.containsTTLPath("/ttl_node"));

    DB::KeeperStorageSnapshot snapshot(&storage, zxid, nullptr, DB::SnapshotVersion::V9);
    auto buf = manager.serializeSnapshotToBuffer(snapshot);
    manager.serializeSnapshotBufferToDisk(*buf, zxid);

    auto debuf = manager.deserializeSnapshotBufferFromDisk(zxid);
    auto deser_result = manager.deserializeSnapshotFromBuffer(debuf);
    const auto & restored = deser_result.storage;

    EXPECT_TRUE(restored->containsTTLPath("/ttl_node"));

    auto node_it = restored->container.find("/ttl_node");
    ASSERT_NE(node_it, restored->container.end());
    ASSERT_TRUE(node_it->value.stats.isTTL());
    EXPECT_EQ(node_it->value.stats.destroyTime(), ttl_ms);

    EXPECT_TRUE(restored->collectExpiredTTLPaths(/*now_ms=*/0, 1000000).empty());
    auto expired = restored->collectExpiredTTLPaths(/*now_ms=*/ttl_ms + 1, 1000000);
    ASSERT_EQ(expired.size(), 1u);
    EXPECT_EQ(expired[0].first, "/ttl_node");
}

TYPED_TEST(CoordinationTest, SerializeSnapshotToDiskCleansPartialFilesOnOpenException)
{
    ChangelogDirTest snapshots("./snapshots");

    using Storage [[maybe_unused]] = DB::KeeperStorage;

    this->keeper_context->setSnapshotDisk(std::make_shared<ThrowingSnapshotDisk>(
        "SnapshotDisk", "./snapshots", "snapshot_50_", SnapshotDiskFailureMode::OpenFileAfterCreate));

    DB::KeeperSnapshotManager manager(3, this->keeper_context, this->enable_compression);
    Storage storage(500, "", this->keeper_context);
    addNode(storage, "/hello", "world");
    DB::KeeperStorageSnapshot snapshot(&storage, 50, nullptr, this->keeper_context->getWriteSnapshotVersion());

    EXPECT_THROW(manager.serializeSnapshotToDisk(snapshot), std::exception);
    assertNoSnapshotArtifactsAndNoRegistration(manager, "./snapshots", 50);
}

TYPED_TEST(CoordinationTest, SerializeSnapshotBufferToDiskCleansPartialFilesOnSyncException)
{
    ChangelogDirTest snapshots("./snapshots");

    using Storage [[maybe_unused]] = DB::KeeperStorage;

    this->keeper_context->setSnapshotDisk(std::make_shared<ThrowingSnapshotDisk>(
        "SnapshotDisk", "./snapshots", "snapshot_51_", SnapshotDiskFailureMode::SyncFile));

    DB::KeeperSnapshotManager manager(3, this->keeper_context, this->enable_compression);
    Storage storage(500, "", this->keeper_context);
    addNode(storage, "/hello", "world");
    DB::KeeperStorageSnapshot snapshot(&storage, 51, nullptr, this->keeper_context->getWriteSnapshotVersion());
    auto buf = manager.serializeSnapshotToBuffer(snapshot);

    EXPECT_THROW(manager.serializeSnapshotBufferToDisk(*buf, 51), std::exception);
    assertNoSnapshotArtifactsAndNoRegistration(manager, "./snapshots", 51);
}

TYPED_TEST(CoordinationTest, SerializeSnapshotBufferToDiskKeepsMarkerWhenCleanupCannotRemoveDataFile)
{
    ChangelogDirTest snapshots("./snapshots");

    using Storage [[maybe_unused]] = DB::KeeperStorage;

    this->keeper_context->setSnapshotDisk(std::make_shared<ThrowingSnapshotDisk>(
        "SnapshotDisk", "./snapshots", "snapshot_56_", SnapshotDiskFailureMode::SyncFileAndCleanupDataFileRemoveFailure));

    DB::KeeperSnapshotManager manager(3, this->keeper_context, this->enable_compression);
    Storage storage(500, "", this->keeper_context);
    addNode(storage, "/hello", "world");
    DB::KeeperStorageSnapshot snapshot(&storage, 56, nullptr, this->keeper_context->getWriteSnapshotVersion());
    auto buf = manager.serializeSnapshotToBuffer(snapshot);

    EXPECT_THROW(manager.serializeSnapshotBufferToDisk(*buf, 56), std::exception);
    /// The data file could not be removed, so its marker must stay too: data + marker.
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 56).size(), 1);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 56, /*include_tmp_markers=*/true).size(), 2);
    EXPECT_EQ(manager.totalSnapshots(), 0);
    EXPECT_EQ(manager.getLatestSnapshotIndex(), 0);
    EXPECT_EQ(manager.getLatestSnapshotInfo(), nullptr);
}

TYPED_TEST(CoordinationTest, SerializeSnapshotBufferToDiskCleansMarkerWhenMarkerCreationFails)
{
    ChangelogDirTest snapshots("./snapshots");

    using Storage [[maybe_unused]] = DB::KeeperStorage;

    this->keeper_context->setSnapshotDisk(std::make_shared<ThrowingSnapshotDisk>(
        "SnapshotDisk", "./snapshots", "tmp_snapshot_52_", SnapshotDiskFailureMode::OpenFileAfterCreate));

    DB::KeeperSnapshotManager manager(3, this->keeper_context, this->enable_compression);
    Storage storage(500, "", this->keeper_context);
    addNode(storage, "/hello", "world");
    DB::KeeperStorageSnapshot snapshot(&storage, 52, nullptr, this->keeper_context->getWriteSnapshotVersion());
    auto buf = manager.serializeSnapshotToBuffer(snapshot);

    EXPECT_THROW(manager.serializeSnapshotBufferToDisk(*buf, 52), std::exception);
    assertNoSnapshotArtifactsAndNoRegistration(manager, "./snapshots", 52);
}

TYPED_TEST(CoordinationTest, SerializeSnapshotBufferToDiskRemovesDataFileWhenMarkerRemovalFails)
{
    ChangelogDirTest snapshots("./snapshots");

    using Storage [[maybe_unused]] = DB::KeeperStorage;

    this->keeper_context->setSnapshotDisk(std::make_shared<ThrowingSnapshotDisk>(
        "SnapshotDisk", "./snapshots", "tmp_snapshot_53_", SnapshotDiskFailureMode::RemoveFileOnce));

    DB::KeeperSnapshotManager manager(3, this->keeper_context, this->enable_compression);
    Storage storage(500, "", this->keeper_context);
    addNode(storage, "/hello", "world");
    DB::KeeperStorageSnapshot snapshot(&storage, 53, nullptr, this->keeper_context->getWriteSnapshotVersion());
    auto buf = manager.serializeSnapshotToBuffer(snapshot);

    EXPECT_THROW(manager.serializeSnapshotBufferToDisk(*buf, 53), std::exception);
    assertNoSnapshotArtifactsAndNoRegistration(manager, "./snapshots", 53);
}

TYPED_TEST(CoordinationTest, BeginSnapshotReceiveToDiskCleansPartialFilesOnOpenException)
{
    ChangelogDirTest snapshots("./snapshots");

    using Storage [[maybe_unused]] = DB::KeeperStorage;

    this->keeper_context->setSnapshotDisk(std::make_shared<ThrowingSnapshotDisk>(
        "SnapshotDisk", "./snapshots", "snapshot_54_", SnapshotDiskFailureMode::OpenFileAfterCreate));

    DB::KeeperSnapshotManager manager(3, this->keeper_context, this->enable_compression);

    EXPECT_THROW(manager.beginSnapshotReceiveToDisk(54), std::exception);
    assertNoSnapshotArtifactsAndNoRegistration(manager, "./snapshots", 54);
}

TYPED_TEST(CoordinationTest, FinalizeSnapshotReceiveToDiskCleansPartialFilesOnSyncException)
{
    ChangelogDirTest snapshots("./snapshots");

    using Storage [[maybe_unused]] = DB::KeeperStorage;

    this->keeper_context->setSnapshotDisk(std::make_shared<ThrowingSnapshotDisk>(
        "SnapshotDisk", "./snapshots", "snapshot_55_", SnapshotDiskFailureMode::SyncFile));

    DB::KeeperSnapshotManager manager(3, this->keeper_context, this->enable_compression);
    auto receive_ctx = manager.beginSnapshotReceiveToDisk(55);
    /// The receive context knows its own unique file name.
    const std::string snapshot_file_name = receive_ctx->snapshot_file_name;
    const std::string partial_snapshot_bytes = "partial snapshot bytes";
    receive_ctx->write_buf->write(partial_snapshot_bytes.data(), partial_snapshot_bytes.size());

    EXPECT_THROW(manager.finalizeSnapshotReceiveToDisk(*receive_ctx), std::exception);
    EXPECT_FALSE(receive_ctx->write_buf);
    EXPECT_FALSE(fs::exists("./snapshots/" + snapshot_file_name));
    EXPECT_FALSE(fs::exists("./snapshots/tmp_" + snapshot_file_name));
    EXPECT_EQ(manager.totalSnapshots(), 0);
    EXPECT_EQ(manager.getLatestSnapshotIndex(), 0);
    EXPECT_EQ(manager.getLatestSnapshotInfo(), nullptr);
}

TEST(KeeperSnapshotManagerCleanupTest, CreateSnapshotKeepsPreviousMetadataAndAllowsRetryAfterFailedWrite)
{
    ChangelogDirTest snapshots("./snapshots");

    auto settings = std::make_shared<DB::CoordinationSettings>();
    auto ctx = std::make_shared<DB::KeeperContext>(true, settings);
    ctx->setLocalLogsPreprocessed();
    auto throwing_disk = std::make_shared<ThrowingSnapshotDisk>(
        "SnapshotDisk", "./snapshots", "snapshot_2_", SnapshotDiskFailureMode::OpenFileAfterCreate);
    ctx->setSnapshotDisk(throwing_disk);

    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    auto execute_snapshot_task = [&](nuraft::snapshot & snapshot, bool & callback_called, bool & callback_result)
    {
        nuraft::async_result<bool>::handler_type when_done
            = [&](bool & ret, nuraft::ptr<std::exception> &)
        {
            callback_called = true;
            callback_result = ret;
        };

        state_machine->create_snapshot(snapshot, when_done);
        DB::CreateSnapshotTask snapshot_task;
        EXPECT_TRUE(snapshots_queue.pop(snapshot_task));
        return snapshot_task.create_snapshot(std::move(snapshot_task.snapshot), /*execute_only_cleanup=*/false);
    };

    auto request1 = std::make_shared<Coordination::ZooKeeperCreateRequest>();
    request1->path = "/node1";
    auto entry1 = getLogEntryFromZKRequest(0, 1, state_machine->getNextZxid(), request1);
    state_machine->pre_commit(1, entry1->get_buf());
    state_machine->commit(1, entry1->get_buf());

    nuraft::snapshot s1(1, 0, std::make_shared<nuraft::cluster_config>());
    bool callback_called_1 = false;
    bool callback_result_1 = false;
    execute_snapshot_task(s1, callback_called_1, callback_result_1);
    EXPECT_TRUE(callback_called_1);
    EXPECT_TRUE(callback_result_1);
    ASSERT_NE(state_machine->last_snapshot(), nullptr);
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 1);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 1).size(), 1);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 1, /*include_tmp_markers=*/true).size(), 1);

    auto request2 = std::make_shared<Coordination::ZooKeeperCreateRequest>();
    request2->path = "/node2";
    auto entry2 = getLogEntryFromZKRequest(0, 1, state_machine->getNextZxid(), request2);
    state_machine->pre_commit(2, entry2->get_buf());
    state_machine->commit(2, entry2->get_buf());

    nuraft::snapshot s2(2, 0, std::make_shared<nuraft::cluster_config>());
    bool callback_called_2 = false;
    bool callback_result_2 = true;
    auto failed_snapshot_info = execute_snapshot_task(s2, callback_called_2, callback_result_2);
    EXPECT_TRUE(callback_called_2);
    EXPECT_FALSE(callback_result_2);
    EXPECT_EQ(failed_snapshot_info, nullptr);
    ASSERT_NE(state_machine->last_snapshot(), nullptr);
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 1);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 1).size(), 1);
    EXPECT_TRUE(snapshotFilesForIdx("./snapshots", 2, /*include_tmp_markers=*/true).empty());
    EXPECT_EQ(state_machine->last_commit_index(), 2);

    throwing_disk->disarm();
    bool callback_called_3 = false;
    bool callback_result_3 = false;
    execute_snapshot_task(s2, callback_called_3, callback_result_3);
    EXPECT_TRUE(callback_called_3);
    EXPECT_TRUE(callback_result_3);
    ASSERT_NE(state_machine->last_snapshot(), nullptr);
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 2);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 1).size(), 1);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 2).size(), 1);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 2, /*include_tmp_markers=*/true).size(), 1);
}

TEST(KeeperSnapshotManagerCleanupTest, ReceiveDuplicateSnapshotRepublishIsIdempotent)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    DB::KeeperStorage storage(500, "", ctx);

    DB::KeeperSnapshotManager manager(3, ctx, true);
    /// Only one `KeeperStorageSnapshot` may be alive per storage at a time (it holds snapshot
    /// mode on), so scope each one before creating the next.
    nuraft::ptr<nuraft::buffer> buf;
    {
        DB::KeeperStorageSnapshot snapshot(&storage, 100, nullptr, ctx->getWriteSnapshotVersion());
        buf = manager.serializeSnapshotToBuffer(snapshot);
    }

    addNode(storage, "/after_duplicate", "must not be written");
    nuraft::ptr<nuraft::buffer> changed_buf;
    {
        DB::KeeperStorageSnapshot changed_snapshot(&storage, 100, nullptr, ctx->getWriteSnapshotVersion());
        changed_buf = manager.serializeSnapshotToBuffer(changed_snapshot);
    }

    auto first_info = manager.serializeSnapshotBufferToDisk(*buf, 100);
    auto second_info = manager.serializeSnapshotBufferToDisk(*changed_buf, 100);

    EXPECT_EQ(manager.totalSnapshots(), 1);
    EXPECT_EQ(manager.getLatestSnapshotIndex(), 100);
    EXPECT_EQ(first_info, second_info);
    /// The second call hits the pre-check and writes nothing.
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 100).size(), 1);
    auto restored_buf = manager.deserializeSnapshotBufferFromDisk(100);
    auto restored = manager.deserializeSnapshotFromBuffer(restored_buf);
    ASSERT_NE(restored.storage, nullptr);
    EXPECT_TRUE(restored.storage->container.contains("/"));
    EXPECT_FALSE(restored.storage->container.contains("/after_duplicate"));
}

TEST(KeeperSnapshotManagerCleanupTest, StartupScanKeepsOneRegisteredSnapshotPerIndex)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    auto buf_55 = makeSingleNodeSnapshotBuffer(ctx, 55, "/kept55", "from_55");
    auto buf_66 = makeSingleNodeSnapshotBuffer(ctx, 66, "/kept66", "from_66");
    auto buf_77 = makeSingleNodeSnapshotBuffer(ctx, 77, "/kept77", "from_77");
    auto buf_88 = makeSingleNodeSnapshotBuffer(ctx, 88, "/kept88", "from_88");

    /// Short suffixes are fine in test fixture files — only production generation
    /// must use full UUIDs.
    auto disk = ctx->getSnapshotDisk();
    /// Outside the retained window (4 indexes, keep = 3): the duplicate is deleted by
    /// the dedup pass and the registered file by outdated-snapshot maintenance.
    writeSnapshotBufferToFile(disk, "snapshot_55.bin.zstd", buf_55);
    writeSnapshotBufferToFile(disk, "snapshot_55_aaaaaaaa.bin.zstd", buf_55);
    /// Retained indexes: duplicates are kept as unvalidated redundant recovery copies.
    writeSnapshotBufferToFile(disk, "snapshot_66_aaaaaaaa.bin.zstd", buf_66);
    writeSnapshotBufferToFile(disk, "snapshot_66_bbbbbbbb.bin.zstd", buf_66);
    /// Upgrade race: legacy deterministic + unique name for the same index.
    writeSnapshotBufferToFile(disk, "snapshot_77.bin.zstd", buf_77);
    writeSnapshotBufferToFile(disk, "snapshot_77_aaaaaaaa.bin.zstd", buf_77);
    /// Crashed publish-loses-race cleanup: two unique names for the same index.
    writeSnapshotBufferToFile(disk, "snapshot_88_aaaaaaaa.bin.zstd", buf_88);
    writeSnapshotBufferToFile(disk, "snapshot_88_bbbbbbbb.bin.zstd", buf_88);
    /// Incomplete write: data file + tmp_ marker — both must be removed.
    {
        auto out = disk->writeFile("tmp_snapshot_99_cccccccc.bin.zstd");
        out->finalize();
    }
    writeSnapshotBufferToFile(disk, "snapshot_99_cccccccc.bin.zstd", buf_88);

    DB::KeeperSnapshotManager manager(3, ctx, true);
    EXPECT_EQ(manager.totalSnapshots(), 3);
    EXPECT_EQ(manager.getLatestSnapshotIndex(), 88);
    /// idx 55 left the retained window: duplicate deleted by dedup, registered file
    /// retired by maintenance — nothing remains.
    EXPECT_TRUE(snapshotFilesForIdx("./snapshots", 55, /*include_tmp_markers=*/true).empty());
    /// Retained indexes keep BOTH copies (one registered + one unvalidated recovery copy).
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 66).size(), 2);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 77).size(), 2);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 88).size(), 2);
    EXPECT_TRUE(snapshotFilesForIdx("./snapshots", 99, /*include_tmp_markers=*/true).empty());

    {
        auto restored = manager.deserializeSnapshotFromBuffer(manager.deserializeSnapshotBufferFromDisk(66));
        ASSERT_NE(restored.storage, nullptr);
        EXPECT_TRUE(restored.storage->container.contains("/kept66"));
    }
    {
        auto restored = manager.deserializeSnapshotFromBuffer(manager.deserializeSnapshotBufferFromDisk(77));
        ASSERT_NE(restored.storage, nullptr);
        EXPECT_TRUE(restored.storage->container.contains("/kept77"));
    }
    {
        auto restored = manager.deserializeSnapshotFromBuffer(manager.deserializeSnapshotBufferFromDisk(88));
        ASSERT_NE(restored.storage, nullptr);
        EXPECT_TRUE(restored.storage->container.contains("/kept88"));
    }

    /// Parser pins: unique and legacy names parse to the same index.
    EXPECT_EQ(DB::getLogIdxFromSnapshotPath("snapshot_100_ab12cd34.bin.zstd"), 100);
    EXPECT_EQ(DB::getLogIdxFromSnapshotPath("snapshot_100.bin.zstd"), 100);
}

TEST(KeeperSnapshotManagerCleanupTest, StartupScanKeepsLatestIndexDuplicatesForRecovery)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    auto buf_80 = makeSingleNodeSnapshotBuffer(ctx, 80, "/old80", "old");
    auto buf_90 = makeSingleNodeSnapshotBuffer(ctx, 90, "/latest_valid", "valid");

    auto disk = ctx->getSnapshotDisk();
    /// Retained non-latest index (2 indexes, keep = 3): the duplicate is kept too —
    /// the operator drill can turn idx 80 into the boot point with one `rm`.
    writeSnapshotBufferToFile(disk, "snapshot_80_aaaaaaaa.bin.zstd", buf_80);
    writeSnapshotBufferToFile(disk, "snapshot_80_bbbbbbbb.bin.zstd", buf_80);
    /// Latest-index duplicate pair: one valid copy, one corrupt copy. Scan order is
    /// unspecified, so either may end up registered — the scan must not unlink the
    /// other (potentially the only readable) copy.
    constexpr auto valid_latest_file = "snapshot_90_valid000.bin.zstd";
    writeSnapshotBufferToFile(disk, valid_latest_file, buf_90);
    {
        auto out = disk->writeFile("snapshot_90_corrupt0.bin.zstd");
        const std::string garbage = "this is not a valid snapshot file";
        out->write(garbage.data(), garbage.size());
        out->finalize();
    }

    DB::KeeperSnapshotManager manager(3, ctx, true);
    EXPECT_EQ(manager.totalSnapshots(), 2);
    EXPECT_EQ(manager.getLatestSnapshotIndex(), 90);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 80).size(), 2);
    /// Both latest-index copies survive the scan; in particular the valid one.
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 90).size(), 2);
    EXPECT_TRUE(fs::exists(std::string("./snapshots/") + valid_latest_file));

    /// Recovery drill: whichever idx-90 copy got registered, the latest snapshot is
    /// recoverable — directly when the valid copy was registered, or by removing the
    /// corrupt copy and restarting otherwise (the documented operator procedure).
    bool registered_copy_is_valid = true;
    try
    {
        auto restored = manager.deserializeSnapshotFromBuffer(manager.deserializeSnapshotBufferFromDisk(90));
        ASSERT_NE(restored.storage, nullptr);
        EXPECT_TRUE(restored.storage->container.contains("/latest_valid"));
    }
    catch (...) // Ok: exception means the registered copy is corrupt; we use the boolean to drive the recovery path below
    {
        registered_copy_is_valid = false;
    }

    if (!registered_copy_is_valid)
    {
        disk->removeFileIfExists("snapshot_90_corrupt0.bin.zstd");
        DB::KeeperSnapshotManager recovered_manager(3, ctx, true);
        EXPECT_EQ(recovered_manager.getLatestSnapshotIndex(), 90);
        auto restored = recovered_manager.deserializeSnapshotFromBuffer(recovered_manager.deserializeSnapshotBufferFromDisk(90));
        ASSERT_NE(restored.storage, nullptr);
        EXPECT_TRUE(restored.storage->container.contains("/latest_valid"));
    }
}

TEST(KeeperSnapshotManagerCleanupTest, StartupScanKeepsRetainedIndexDuplicatesForDrillRecovery)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    auto buf_80 = makeSingleNodeSnapshotBuffer(ctx, 80, "/drill_valid", "valid");

    auto disk = ctx->getSnapshotDisk();
    /// Retained NON-latest index with one valid and one corrupt copy.
    constexpr auto valid_retained_file = "snapshot_80_valid000.bin.zstd";
    writeSnapshotBufferToFile(disk, valid_retained_file, buf_80);
    {
        auto out = disk->writeFile("snapshot_80_corrupt0.bin.zstd");
        const std::string garbage = "corrupt retained copy";
        out->write(garbage.data(), garbage.size());
        out->finalize();
    }
    /// Corrupt LATEST snapshot: the documented operator drill (`test_invalid_snapshot`)
    /// removes it and restarts, which turns the retained idx 80 into the boot point.
    {
        auto out = disk->writeFile("snapshot_90_corrupt0.bin.zstd");
        const std::string garbage = "corrupt latest snapshot";
        out->write(garbage.data(), garbage.size());
        out->finalize();
    }

    {
        DB::KeeperSnapshotManager manager(3, ctx, true);
        EXPECT_EQ(manager.totalSnapshots(), 2);
        EXPECT_EQ(manager.getLatestSnapshotIndex(), 90);
        /// Both retained idx-80 copies must survive the first scan — in particular the
        /// valid one, whichever of the two got registered (scan order is unspecified).
        EXPECT_EQ(snapshotFilesForIdx("./snapshots", 80).size(), 2);
        EXPECT_TRUE(fs::exists(std::string("./snapshots/") + valid_retained_file));
    }

    /// Drill step 1: the latest snapshot is unreadable — the operator removes it and
    /// restarts; idx 80 becomes the latest.
    disk->removeFileIfExists("snapshot_90_corrupt0.bin.zstd");

    DB::KeeperSnapshotManager manager(3, ctx, true);
    EXPECT_EQ(manager.getLatestSnapshotIndex(), 80);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 80).size(), 2);

    /// Drill step 2 (scan-order-robust): whichever idx-80 copy got registered, the
    /// data is recoverable — directly when the valid copy was registered, or by
    /// removing the corrupt copy and restarting otherwise.
    bool registered_copy_is_valid = true;
    try
    {
        auto restored = manager.deserializeSnapshotFromBuffer(manager.deserializeSnapshotBufferFromDisk(80));
        ASSERT_NE(restored.storage, nullptr);
        EXPECT_TRUE(restored.storage->container.contains("/drill_valid"));
    }
    catch (...) // Ok: exception means the registered copy is corrupt; we use the boolean to drive the recovery path below
    {
        registered_copy_is_valid = false;
    }

    if (!registered_copy_is_valid)
    {
        disk->removeFileIfExists("snapshot_80_corrupt0.bin.zstd");
        DB::KeeperSnapshotManager recovered_manager(3, ctx, true);
        EXPECT_EQ(recovered_manager.getLatestSnapshotIndex(), 80);
        auto restored = recovered_manager.deserializeSnapshotFromBuffer(recovered_manager.deserializeSnapshotBufferFromDisk(80));
        ASSERT_NE(restored.storage, nullptr);
        EXPECT_TRUE(restored.storage->container.contains("/drill_valid"));
    }
}

TEST(KeeperSnapshotManagerCleanupTest, RetainedDuplicateAgesOutWithoutRestart)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    auto buf_80 = makeSingleNodeSnapshotBuffer(ctx, 80, "/kept80", "from_80");
    auto buf_90 = makeSingleNodeSnapshotBuffer(ctx, 90, "/kept90", "from_90");

    auto disk = ctx->getSnapshotDisk();
    /// Retained non-latest index (2 indexes, keep = 3): the startup scan keeps both same-index
    /// copies — one registered, one redundant recovery copy.
    writeSnapshotBufferToFile(disk, "snapshot_80_aaaaaaaa.bin.zstd", buf_80);
    writeSnapshotBufferToFile(disk, "snapshot_80_bbbbbbbb.bin.zstd", buf_80);
    writeSnapshotBufferToFile(disk, "snapshot_90_cccccccc.bin.zstd", buf_90);

    DB::KeeperSnapshotManager manager(3, ctx, true);
    EXPECT_EQ(manager.totalSnapshots(), 2);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 80).size(), 2);

    /// Age idx 80 out WITHOUT a restart. Both the registered copy and the tracked duplicate
    /// must be unlinked together. Before the fix the unregistered duplicate was invisible to
    /// retention and leaked until the next restart, leaving size() == 1 here.
    manager.removeSnapshot(80);
    EXPECT_EQ(manager.totalSnapshots(), 1);
    EXPECT_TRUE(snapshotFilesForIdx("./snapshots", 80, /*include_tmp_markers=*/true).empty());
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 90).size(), 1);
}

TEST(KeeperSnapshotManagerCleanupTest, QueuePushFailureCleansSnapshotAndCallsWhenDone)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    ctx->setServerState(DB::KeeperContext::Phase::RUNNING);

    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    auto request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
    request->path = "/node";
    auto entry = getLogEntryFromZKRequest(0, 1, state_machine->getNextZxid(), request);
    state_machine->pre_commit(1, entry->get_buf());
    state_machine->commit(1, entry->get_buf());

    snapshots_queue.finish();

    int callback_calls = 0;
    bool callback_result = true;
    nuraft::async_result<bool>::handler_type when_done
        = [&](bool & ret, nuraft::ptr<std::exception> &)
    {
        ++callback_calls;
        callback_result = ret;
    };

    nuraft::snapshot snapshot(1, 0, std::make_shared<nuraft::cluster_config>());
    state_machine->create_snapshot(snapshot, when_done);

    EXPECT_EQ(callback_calls, 1);
    EXPECT_FALSE(callback_result);
    EXPECT_EQ(state_machine->last_snapshot(), nullptr);
    EXPECT_TRUE(snapshotFilesForIdx("./snapshots", 1, /*include_tmp_markers=*/true).empty());
}

TEST(KeeperSnapshotManagerCleanupTest, SameIndexReceiveDuringLocalCreate)
{
    using namespace std::chrono_literals;

    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    ctx->setServerState(DB::KeeperContext::Phase::RUNNING);
    auto block_state = std::make_shared<BlockingSnapshotWriteState>();
    ctx->setSnapshotDisk(std::make_shared<BlockingSnapshotDisk>(
        "SnapshotDisk", "./snapshots", "snapshot_1_", block_state));

    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    auto request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
    request->path = "/node";
    auto entry = getLogEntryFromZKRequest(0, 1, state_machine->getNextZxid(), request);
    state_machine->pre_commit(1, entry->get_buf());
    state_machine->commit(1, entry->get_buf());

    auto snapshot_buf = makeSingleNodeSnapshotBuffer(ctx, 1, "/from_receive", "from_receive");

    bool callback_called = false;
    bool callback_result = false;
    nuraft::async_result<bool>::handler_type when_done
        = [&](bool & ret, nuraft::ptr<std::exception> &)
    {
        callback_called = true;
        callback_result = ret;
    };

    nuraft::snapshot snapshot(1, 0, std::make_shared<nuraft::cluster_config>());
    state_machine->create_snapshot(snapshot, when_done);
    DB::CreateSnapshotTask snapshot_task;
    ASSERT_TRUE(snapshots_queue.pop(snapshot_task));

    auto create_future = std::async(
        std::launch::async,
        [task = std::move(snapshot_task)]() mutable
        {
            return task.create_snapshot(std::move(task.snapshot), /*execute_only_cleanup=*/false);
        });
    SCOPE_EXIT({ block_state->release(); });

    ASSERT_TRUE(block_state->waitUntilBlocked(5s));

    /// One-object receive of the same index completes immediately to its own unique
    /// file — no deferral protocol.
    uint64_t obj_id = 0;
    state_machine->save_logical_snp_obj(
        snapshot,
        obj_id,
        *snapshot_buf,
        /*is_first_obj=*/true,
        /*is_last_obj=*/true);
    EXPECT_EQ(obj_id, 1);
    EXPECT_EQ(block_state->getSyncCalls(), 1);
    /// The receive stamps `pending_snapshot_to_apply`, not the mark, so `last_snapshot` stays null
    /// until the still-blocked create publishes.
    EXPECT_EQ(state_machine->last_snapshot(), nullptr);

    block_state->release();
    auto snapshot_file_info = create_future.get();
    ASSERT_NE(snapshot_file_info, nullptr);

    /// The create adopted the receive's registered entry and advanced the mark to 1.
    ASSERT_NE(state_machine->last_snapshot(), nullptr);
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 1);

    EXPECT_TRUE(callback_called);
    EXPECT_TRUE(callback_result);
    EXPECT_EQ(block_state->getSyncCalls(), 1);

    /// The create adopted the receive's published entry and retired its own file:
    /// exactly one idx-1 data file remains and no idx-1 markers.
    auto files = snapshotFilesForIdx("./snapshots", 1);
    ASSERT_EQ(files.size(), 1);
    EXPECT_EQ(fs::path(snapshot_file_info->path).filename().string(), files[0]);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 1, /*include_tmp_markers=*/true).size(), 1);

    /// Published bytes were never rewritten — a fresh manager restores the receive's data.
    DB::KeeperSnapshotManager manager(3, ctx, true);
    auto restored = manager.deserializeSnapshotFromBuffer(manager.deserializeSnapshotBufferFromDisk(1));
    ASSERT_NE(restored.storage, nullptr);
    EXPECT_TRUE(restored.storage->container.contains("/from_receive"));
}

TEST(KeeperSnapshotManagerCleanupTest, ChunkedSameIndexReceiveDoesNotRewritePublishedSnapshot)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    ctx->setServerState(DB::KeeperContext::Phase::RUNNING);

    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    DB::KeeperStorage original_storage(500, "", ctx);
    addNode(original_storage, "/original", "original");
    auto original_buf = makeSnapshotBufferFromStorage(original_storage, 10, ctx);
    nuraft::snapshot snapshot(10, 0, std::make_shared<nuraft::cluster_config>());
    saveSingleObjectSnapshot(*state_machine, snapshot, original_buf);

    auto published_files = snapshotFilesForIdx("./snapshots", 10);
    ASSERT_EQ(published_files.size(), 1);
    const fs::path published_snapshot_path = fs::path("./snapshots") / published_files[0];
    const auto published_snapshot_size = fs::file_size(published_snapshot_path);

    DB::KeeperStorage duplicate_storage(500, "", ctx);
    addNode(duplicate_storage, "/duplicate", "duplicate");
    auto duplicate_buf = makeSnapshotBufferFromStorage(duplicate_storage, 10, ctx);
    ASSERT_GT(duplicate_buf->size(), 1);
    const size_t first_chunk_size = duplicate_buf->size() / 2;
    auto first_chunk = sliceBuffer(duplicate_buf, 0, first_chunk_size);
    auto second_chunk = sliceBuffer(duplicate_buf, first_chunk_size, duplicate_buf->size() - first_chunk_size);

    uint64_t obj_id = 0;
    state_machine->save_logical_snp_obj(
        snapshot,
        obj_id,
        *first_chunk,
        /*is_first_obj=*/true,
        /*is_last_obj=*/false);
    EXPECT_EQ(obj_id, 1);
    EXPECT_TRUE(fs::exists(published_snapshot_path));
    EXPECT_EQ(fs::file_size(published_snapshot_path), published_snapshot_size);

    nuraft::snapshot other_snapshot(11, 0, std::make_shared<nuraft::cluster_config>());
    DB::KeeperStorage other_storage(500, "", ctx);
    /// Serialize over an isolated disk: a throwaway manager over `./snapshots` would run its
    /// ctor incomplete-pair scan and delete the idx-10 receive that is mid-flight here.
    auto other_buf = makeInstallBuffer(other_storage, 11, ctx);
    auto other_first_chunk = sliceBuffer(other_buf, 0, other_buf->size() / 2);
    obj_id = 0;
    state_machine->save_logical_snp_obj(
        other_snapshot,
        obj_id,
        *other_first_chunk,
        /*is_first_obj=*/true,
        /*is_last_obj=*/false);
    EXPECT_EQ(obj_id, 1);
    EXPECT_TRUE(fs::exists(published_snapshot_path));
    EXPECT_EQ(fs::file_size(published_snapshot_path), published_snapshot_size);

    obj_id = 0;
    state_machine->save_logical_snp_obj(
        snapshot,
        obj_id,
        *first_chunk,
        /*is_first_obj=*/true,
        /*is_last_obj=*/false);
    EXPECT_EQ(obj_id, 1);

    state_machine->save_logical_snp_obj(
        snapshot,
        obj_id,
        *second_chunk,
        /*is_first_obj=*/false,
        /*is_last_obj=*/true);
    EXPECT_EQ(obj_id, 2);

    /// The duplicate's unique file lost the publish race and was retired+unlinked by
    /// `finalizeSnapshotReceiveToDisk`; the published file is byte-identical.
    EXPECT_TRUE(fs::exists(published_snapshot_path));
    EXPECT_EQ(fs::file_size(published_snapshot_path), published_snapshot_size);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 10).size(), 1);

    DB::KeeperSnapshotManager manager(3, ctx, true);
    auto restored_buf = manager.deserializeSnapshotBufferFromDisk(10);
    auto restored = manager.deserializeSnapshotFromBuffer(restored_buf);
    ASSERT_NE(restored.storage, nullptr);
    EXPECT_TRUE(restored.storage->container.contains("/original"));
    EXPECT_FALSE(restored.storage->container.contains("/duplicate"));
}

TEST(KeeperSnapshotManagerCleanupTest, ApplySnapshotDoesNotWaitForLocalCreate)
{
    using namespace std::chrono_literals;

    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    ctx->setServerState(DB::KeeperContext::Phase::RUNNING);
    auto block_state = std::make_shared<BlockingSnapshotWriteState>();
    ctx->setSnapshotDisk(std::make_shared<BlockingSnapshotDisk>(
        "SnapshotDisk", "./snapshots", "snapshot_1_", block_state));

    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    auto request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
    request->path = "/old_node";
    auto entry = getLogEntryFromZKRequest(0, 1, state_machine->getNextZxid(), request);
    state_machine->pre_commit(1, entry->get_buf());
    state_machine->commit(1, entry->get_buf());

    auto snapshot_buf = makeSingleNodeSnapshotBuffer(ctx, 2, "/replacement", "from_receive");

    bool callback_called = false;
    bool callback_result = false;
    nuraft::async_result<bool>::handler_type when_done
        = [&](bool & ret, nuraft::ptr<std::exception> &)
    {
        callback_called = true;
        callback_result = ret;
    };

    nuraft::snapshot local_snapshot(1, 0, std::make_shared<nuraft::cluster_config>());
    state_machine->create_snapshot(local_snapshot, when_done);
    DB::CreateSnapshotTask snapshot_task;
    ASSERT_TRUE(snapshots_queue.pop(snapshot_task));

    auto create_future = std::async(
        std::launch::async,
        [task = std::move(snapshot_task)]() mutable
        {
            return task.create_snapshot(std::move(task.snapshot), /*execute_only_cleanup=*/false);
        });
    SCOPE_EXIT({ block_state->release(); });

    ASSERT_TRUE(block_state->waitUntilBlocked(5s));

    nuraft::snapshot received_snapshot(2, 0, std::make_shared<nuraft::cluster_config>());
    uint64_t obj_id = 0;
    state_machine->save_logical_snp_obj(
        received_snapshot,
        obj_id,
        *snapshot_buf,
        /*is_first_obj=*/true,
        /*is_last_obj=*/true);
    EXPECT_EQ(obj_id, 1);

    /// Completion-while-blocked IS the assertion; the failure mode is a deterministic
    /// timeout. ASSERT so a regression fails instead of hanging in get(); the writer is
    /// released before the early return (apply_future dies before the SCOPE_EXIT fires).
    auto apply_future = std::async(
        std::launch::async,
        [&]
        {
            return state_machine->apply_snapshot(received_snapshot);
        });
    const auto apply_status = apply_future.wait_for(10s);
    if (apply_status != std::future_status::ready)
        block_state->release();
    ASSERT_EQ(apply_status, std::future_status::ready);
    EXPECT_TRUE(apply_future.get());
    EXPECT_TRUE(state_machine->getStorageUnsafe().container.contains("/replacement"));
    EXPECT_FALSE(state_machine->getStorageUnsafe().container.contains("/old_node"));

    /// The old storage stayed alive via the task's captured reference; the task's
    /// cleanup runs against it and the task adopts the newer published snapshot.
    block_state->release();
    auto snapshot_file_info = create_future.get();
    ASSERT_NE(snapshot_file_info, nullptr);
    EXPECT_EQ(DB::getLogIdxFromSnapshotPath(snapshot_file_info->path), 2);
    EXPECT_TRUE(callback_called);
    EXPECT_TRUE(callback_result);

    EXPECT_TRUE(snapshotFilesForIdx("./snapshots", 1, /*include_tmp_markers=*/true).empty());
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 2).size(), 1);
}

TEST(KeeperSnapshotManagerCleanupTest, CreateLosesRaceToNewerSnapshotRetiresWrittenFile)
{
    using namespace std::chrono_literals;

    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    ctx->setServerState(DB::KeeperContext::Phase::RUNNING);
    auto block_state = std::make_shared<BlockingSnapshotWriteState>();
    ctx->setSnapshotDisk(std::make_shared<BlockingSnapshotDisk>(
        "SnapshotDisk", "./snapshots", "snapshot_1_", block_state));

    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    auto request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
    request->path = "/node";
    auto entry = getLogEntryFromZKRequest(0, 1, state_machine->getNextZxid(), request);
    state_machine->pre_commit(1, entry->get_buf());
    state_machine->commit(1, entry->get_buf());

    auto snapshot_buf = makeSingleNodeSnapshotBuffer(ctx, 2, "/newer", "from_receive");

    bool callback_called = false;
    bool callback_result = false;
    nuraft::async_result<bool>::handler_type when_done
        = [&](bool & ret, nuraft::ptr<std::exception> &)
    {
        callback_called = true;
        callback_result = ret;
    };

    nuraft::snapshot local_snapshot(1, 0, std::make_shared<nuraft::cluster_config>());
    state_machine->create_snapshot(local_snapshot, when_done);
    DB::CreateSnapshotTask snapshot_task;
    ASSERT_TRUE(snapshots_queue.pop(snapshot_task));

    auto create_future = std::async(
        std::launch::async,
        [task = std::move(snapshot_task)]() mutable
        {
            return task.create_snapshot(std::move(task.snapshot), /*execute_only_cleanup=*/false);
        });
    SCOPE_EXIT({ block_state->release(); });

    ASSERT_TRUE(block_state->waitUntilBlocked(5s));

    /// `snapshot_2_*` never matches the already-consumed blocking matcher.
    nuraft::snapshot received_snapshot(2, 0, std::make_shared<nuraft::cluster_config>());
    saveSingleObjectSnapshot(*state_machine, received_snapshot, snapshot_buf);
    /// The receive stamps pending, not the mark; apply it so the mark advances to 2 and the
    /// blocked create at 1 loses the race (otherwise it would legitimately win at 1).
    EXPECT_EQ(state_machine->last_snapshot(), nullptr);
    EXPECT_TRUE(state_machine->apply_snapshot(received_snapshot));
    ASSERT_NE(state_machine->last_snapshot(), nullptr);
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 2);

    block_state->release();
    auto snapshot_file_info = create_future.get();
    ASSERT_NE(snapshot_file_info, nullptr);
    /// The create adopted the newer published snapshot and retired its own idx-1 file.
    EXPECT_EQ(DB::getLogIdxFromSnapshotPath(snapshot_file_info->path), 2);
    EXPECT_TRUE(callback_called);
    EXPECT_TRUE(callback_result);

    EXPECT_TRUE(snapshotFilesForIdx("./snapshots", 1, /*include_tmp_markers=*/true).empty());
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 2).size(), 1);
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 2);
}

TEST(KeeperSnapshotManagerCleanupTest, CreateForOlderRetainedIndexAdoptsLatestSnapshot)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    ctx->setServerState(DB::KeeperContext::Phase::RUNNING);

    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    auto buf_idx_3 = makeSingleNodeSnapshotBuffer(ctx, 3, "/from_receive_3", "three");
    auto buf_idx_5 = makeSingleNodeSnapshotBuffer(ctx, 5, "/from_receive_5", "five");

    nuraft::snapshot s3(3, 0, std::make_shared<nuraft::cluster_config>());
    saveSingleObjectSnapshot(*state_machine, s3, buf_idx_3);
    EXPECT_TRUE(state_machine->apply_snapshot(s3));
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 3);
    nuraft::snapshot s5(5, 0, std::make_shared<nuraft::cluster_config>());
    saveSingleObjectSnapshot(*state_machine, s5, buf_idx_5);
    EXPECT_TRUE(state_machine->apply_snapshot(s5));
    ASSERT_NE(state_machine->last_snapshot(), nullptr);
    ASSERT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 5);

    bool callback_called = false;
    bool callback_result = false;
    nuraft::async_result<bool>::handler_type when_done
        = [&](bool & ret, nuraft::ptr<std::exception> &)
    {
        callback_called = true;
        callback_result = ret;
    };

    /// Both indexes are retained (`snapshots_to_keep` defaults to 3). A create for the
    /// RETAINED OLDER index must adopt the latest snapshot, never the idx-3 entry —
    /// the returned entry is what gets uploaded to S3.
    nuraft::snapshot create_s3(3, 0, std::make_shared<nuraft::cluster_config>());
    state_machine->create_snapshot(create_s3, when_done);
    DB::CreateSnapshotTask snapshot_task;
    ASSERT_TRUE(snapshots_queue.pop(snapshot_task));
    auto snapshot_file_info = snapshot_task.create_snapshot(std::move(snapshot_task.snapshot), /*execute_only_cleanup=*/false);

    EXPECT_TRUE(callback_called);
    EXPECT_TRUE(callback_result);
    ASSERT_NE(snapshot_file_info, nullptr);
    EXPECT_EQ(DB::getLogIdxFromSnapshotPath(snapshot_file_info->path), 5);
    /// No new idx-3 file was written; the metadata stays at 5.
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 3).size(), 1);
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 5);
}

TEST(KeeperSnapshotManagerCleanupTest, CreateRacingNewerReceiveWithRetainedSameIndexAdoptsLatest)
{
    using namespace std::chrono_literals;

    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    ctx->setServerState(DB::KeeperContext::Phase::RUNNING);
    auto block_state = std::make_shared<BlockingSnapshotWriteState>();
    ctx->setSnapshotDisk(std::make_shared<BlockingSnapshotDisk>(
        "SnapshotDisk", "./snapshots", "snapshot_3_", block_state));

    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    auto buf_idx_3 = makeSingleNodeSnapshotBuffer(ctx, 3, "/from_receive_3", "three");
    auto buf_idx_5 = makeSingleNodeSnapshotBuffer(ctx, 5, "/from_receive_5", "five");

    bool callback_called = false;
    bool callback_result = false;
    nuraft::async_result<bool>::handler_type when_done
        = [&](bool & ret, nuraft::ptr<std::exception> &)
    {
        callback_called = true;
        callback_result = ret;
    };

    nuraft::snapshot local_snapshot(3, 0, std::make_shared<nuraft::cluster_config>());
    state_machine->create_snapshot(local_snapshot, when_done);
    DB::CreateSnapshotTask snapshot_task;
    ASSERT_TRUE(snapshots_queue.pop(snapshot_task));

    auto create_future = std::async(
        std::launch::async,
        [task = std::move(snapshot_task)]() mutable
        {
            return task.create_snapshot(std::move(task.snapshot), /*execute_only_cleanup=*/false);
        });
    SCOPE_EXIT({ block_state->release(); });

    ASSERT_TRUE(block_state->waitUntilBlocked(5s));

    nuraft::snapshot s3(3, 0, std::make_shared<nuraft::cluster_config>());
    saveSingleObjectSnapshot(*state_machine, s3, buf_idx_3);
    nuraft::snapshot s5(5, 0, std::make_shared<nuraft::cluster_config>());
    saveSingleObjectSnapshot(*state_machine, s5, buf_idx_5);
    /// Saves stamp pending (3 then 5), not the mark; apply 5 so the mark reaches 5 and the
    /// blocked create at 3 adopts the mark's idx-5 pin via branch (a).
    EXPECT_EQ(state_machine->last_snapshot(), nullptr);
    EXPECT_TRUE(state_machine->apply_snapshot(s5));
    ASSERT_NE(state_machine->last_snapshot(), nullptr);
    ASSERT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 5);

    block_state->release();
    auto snapshot_file_info = create_future.get();
    ASSERT_NE(snapshot_file_info, nullptr);
    EXPECT_TRUE(callback_called);
    EXPECT_TRUE(callback_result);

    /// Phase-3 latest-covering adoption fires (not the same-index branch): the create's
    /// own idx-3 file is retired and removed and the idx-5 entry is returned.
    EXPECT_EQ(DB::getLogIdxFromSnapshotPath(snapshot_file_info->path), 5);
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 5);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 3).size(), 1);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 3, /*include_tmp_markers=*/true).size(), 1);
}

TEST(KeeperSnapshotManagerCleanupTest, PublishSnapshotFileReusesExistingAndRetiredLoserIsRemoved)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    auto buf_a = makeSingleNodeSnapshotBuffer(ctx, 100, "/from_a", "a");
    auto buf_b = makeSingleNodeSnapshotBuffer(ctx, 100, "/from_b", "b");

    DB::KeeperSnapshotManager manager(3, ctx, true);

    auto first_file_info = manager.writeSnapshotBufferToFile(*buf_a, 100);
    EXPECT_EQ(manager.publishSnapshotFile(100, first_file_info), first_file_info);

    auto second_file_info = manager.writeSnapshotBufferToFile(*buf_b, 100);
    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 100).size(), 2);

    /// Publishing a second file for the same index returns the registered entry.
    EXPECT_EQ(manager.publishSnapshotFile(100, second_file_info), first_file_info);

    /// The loser is retired; dropping the last reference unlinks it.
    manager.retireUnpublishedSnapshotFile(second_file_info);
    second_file_info.reset();

    EXPECT_EQ(snapshotFilesForIdx("./snapshots", 100).size(), 1);
    EXPECT_EQ(manager.totalSnapshots(), 1);

    auto restored = manager.deserializeSnapshotFromBuffer(manager.deserializeSnapshotBufferFromDisk(100));
    ASSERT_NE(restored.storage, nullptr);
    EXPECT_TRUE(restored.storage->container.contains("/from_a"));
    EXPECT_FALSE(restored.storage->container.contains("/from_b"));
}

TEST(KeeperSnapshotManagerCleanupTest, MovePublicationRejectionBranches)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");
    ChangelogDirTest snapshots_other("./snapshots_other");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    auto buf = makeSingleNodeSnapshotBuffer(ctx, 10, "/move_me", "data");

    DB::KeeperSnapshotManager manager(3, ctx, true);
    manager.serializeSnapshotBufferToDisk(*buf, 10);

    DB::SnapshotMoveCandidate candidate;
    {
        auto pin = manager.getSnapshotPin(10);
        ASSERT_NE(pin, nullptr);
        candidate = DB::SnapshotMoveCandidate{
            .log_idx = 10,
            .file_info = pin,
            .source_disk = pin->disk,
            .source_path = pin->path,
            .target_disk = pin->disk,
            .target_path = pin->path};
    } /// pin released -> use_count == 2 (map + candidate)

    /// (a) Valid baseline.
    EXPECT_TRUE(manager.publishMovedSnapshotIfValid(candidate));

    /// (b) Pinned: a deliberate extra reference during the call rejects the move.
    {
        auto extra_pin = candidate.file_info;
        EXPECT_FALSE(manager.publishMovedSnapshotIfValid(candidate));
    }

    /// (c) Retired entries are rejected.
    candidate.file_info->retired_for_removal.store(true, std::memory_order_release);
    EXPECT_FALSE(manager.publishMovedSnapshotIfValid(candidate));
    candidate.file_info->retired_for_removal.store(false, std::memory_order_release);

    /// (d2) Source metadata changed since the candidate was built; mutate in place to
    /// keep use_count at 2.
    {
        const auto original_source_path = candidate.source_path;
        candidate.source_path += "_changed";
        EXPECT_FALSE(manager.publishMovedSnapshotIfValid(candidate));
        candidate.source_path = original_source_path;
    }

    /// (e) Target disk role changed; mutate in place to keep use_count at 2.
    auto other_disk = std::make_shared<DB::DiskLocal>("OtherDisk", "./snapshots_other");
    auto original_target_disk = candidate.target_disk;
    candidate.target_disk = other_disk;
    EXPECT_FALSE(manager.publishMovedSnapshotIfValid(candidate));
    candidate.target_disk = original_target_disk;

    /// After every rejection the registered entry is untouched and readable.
    EXPECT_EQ(candidate.file_info->disk, candidate.source_disk);
    EXPECT_EQ(candidate.file_info->path, candidate.source_path);
    {
        auto restored = manager.deserializeSnapshotFromBuffer(manager.deserializeSnapshotBufferFromDisk(10));
        ASSERT_NE(restored.storage, nullptr);
        EXPECT_TRUE(restored.storage->container.contains("/move_me"));
    }

    /// (d) Metadata replaced: remove + republish a fresh file for the same index;
    /// the candidate still holds the old pointer.
    manager.removeSnapshot(10);
    manager.serializeSnapshotBufferToDisk(*buf, 10);
    EXPECT_FALSE(manager.publishMovedSnapshotIfValid(candidate));

    /// (d variant) Metadata absent.
    manager.removeSnapshot(10);
    EXPECT_FALSE(manager.publishMovedSnapshotIfValid(candidate));
}

TEST(KeeperSnapshotManagerCleanupTest, MoveSnapshotCandidateHonorsBoolPublishCallback)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");
    ChangelogDirTest move_target("./snapshots_move_target");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    auto buf = makeSingleNodeSnapshotBuffer(ctx, 10, "/move_me", "data");

    DB::KeeperSnapshotManager manager(3, ctx, true);
    manager.serializeSnapshotBufferToDisk(*buf, 10);

    auto target_disk = std::make_shared<DB::DiskLocal>("MoveTargetDisk", "./snapshots_move_target");

    DB::SnapshotMoveCandidate candidate;
    {
        auto pin = manager.getSnapshotPin(10);
        ASSERT_NE(pin, nullptr);
        candidate = DB::SnapshotMoveCandidate{
            .log_idx = 10,
            .file_info = pin,
            .source_disk = pin->disk,
            .source_path = pin->path,
            .target_disk = target_disk,
            .target_path = pin->path};
    }

    /// Rejection: the copied target is cleaned up and the source is kept.
    EXPECT_FALSE(manager.moveSnapshotCandidate(candidate, [](const DB::SnapshotMoveCandidate &) { return false; }));
    EXPECT_TRUE(fs::exists("./snapshots/" + candidate.source_path));
    EXPECT_FALSE(fs::exists("./snapshots_move_target/" + candidate.target_path));
    EXPECT_FALSE(fs::exists("./snapshots_move_target/tmp_" + candidate.target_path));

    /// Acceptance: the callback performs the production metadata update before
    /// returning true; the source is removed afterwards.
    EXPECT_TRUE(manager.moveSnapshotCandidate(
        candidate,
        [](const DB::SnapshotMoveCandidate & c)
        {
            c.file_info->disk = c.target_disk;
            c.file_info->path = c.target_path;
            return true;
        }));
    EXPECT_FALSE(fs::exists("./snapshots/" + candidate.source_path));
    EXPECT_TRUE(fs::exists("./snapshots_move_target/" + candidate.target_path));

    /// The manager reads the moved snapshot from its new location.
    auto restored = manager.deserializeSnapshotFromBuffer(manager.deserializeSnapshotBufferFromDisk(10));
    ASSERT_NE(restored.storage, nullptr);
    EXPECT_TRUE(restored.storage->container.contains("/move_me"));
}

TEST(KeeperSnapshotFileNameTest, CanonicalSnapshotS3Name)
{
    EXPECT_EQ(DB::getCanonicalSnapshotS3Name("snapshot_100_ab12cd34.bin.zstd"), "snapshot_100.bin.zstd");
    EXPECT_EQ(DB::getCanonicalSnapshotS3Name("snapshot_100_ab12cd34.bin"), "snapshot_100.bin");
    EXPECT_EQ(
        DB::getCanonicalSnapshotS3Name("snapshot_100_11111111-2222-4333-8444-555555555555.bin.zstd"), "snapshot_100.bin.zstd");
    EXPECT_EQ(DB::getCanonicalSnapshotS3Name("snapshot_100.bin.zstd"), "snapshot_100.bin.zstd");
    EXPECT_EQ(DB::getCanonicalSnapshotS3Name("snapshot_100.bin"), "snapshot_100.bin");
    EXPECT_EQ(DB::getCanonicalSnapshotS3Name("/var/lib/x/snapshot_5_deadbeef.bin.zstd"), "snapshot_5.bin.zstd");
}

// Chunked snapshot header/footer pack/unpack + bounds validation tests.

namespace DB
{
namespace ErrorCodes
{
extern const int UNKNOWN_FORMAT_VERSION;
}

static DB::KeeperContextPtr makeKeeperContext(const std::string & snapshot_path)
{
    auto settings = std::make_shared<DB::CoordinationSettings>();
    auto ctx = std::make_shared<DB::KeeperContext>(true, settings);
    ctx->setLocalLogsPreprocessed();
    ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk", snapshot_path));
    return ctx;
}

/// Source context for chunked snapshot serialization: chunk_size=2, digest enabled.
static DB::KeeperContextPtr makeChunkedSourceCtx(const std::string & snapshot_path)
{
    auto settings = std::make_shared<DB::CoordinationSettings>();
    (*settings)[DB::CoordinationSetting::snapshot_chunk_size] = 2;
    auto ctx = std::make_shared<DB::KeeperContext>(true, settings);
    ctx->setLocalLogsPreprocessed();
    ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SrcSnapDisk", snapshot_path));
    ctx->setDigestEnabled(true);
    return ctx;
}

/// State machine for chunked snapshot apply tests: compress+zstd, deser_threads=2, digest enabled.
static std::shared_ptr<DB::KeeperStateMachine>
makeChunkedApplyStateMachine(DB::SnapshotsQueue & queue, const std::string & snapshot_path)
{
    auto settings = std::make_shared<DB::CoordinationSettings>();
    (*settings)[DB::CoordinationSetting::compress_snapshots_with_zstd_format] = true;
    (*settings)[DB::CoordinationSetting::snapshot_deser_threads] = 2;
    auto ctx = std::make_shared<DB::KeeperContext>(true, settings);
    ctx->setLocalLogsPreprocessed();
    ctx->setDigestEnabled(true);
    ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SmSnapDisk", snapshot_path));
    auto sm = std::make_shared<DB::KeeperStateMachine>(nullptr, queue, ctx, nullptr);
    sm->init();
    return sm;
}

/// Build a test buffer from descriptors: [front header][synthetic zeros][footer].
static std::string buildChunkedBufferFromDescriptors(const std::vector<SnapshotChunkDescriptor> & frames)
{
    uint64_t footer_offset = chunkedSnapshotHeaderSize(); // starts at 13 when no frames
    for (const auto & f : frames)
        footer_offset = std::max(footer_offset, f.compressed_offset + f.compressed_size);

    WriteBufferFromOwnString out;
    packChunkedSnapshotHeader(static_cast<uint64_t>(frames.size()), out);

    // Chunk region (synthetic zeros — the actual frames are not written in this test helper)
    const size_t chunk_region_size = static_cast<size_t>(footer_offset) - chunkedSnapshotHeaderSize();
    std::string zeros(chunk_region_size, '\0');
    out.write(zeros.data(), zeros.size());

    packChunkedSnapshotFooter(frames, out);
    out.finalize();
    return out.str();
}

/// Round-trip a 2-chunk (METADATA, NODES) descriptor through parseAndValidateChunkedSnapshot.
TEST(CoordinationChunkedSnapshotTest, RoundTripMinimal)
{
    // Frames start at offset 13 (just past the front header); contiguous, no gaps.
    std::vector<SnapshotChunkDescriptor> frames = {
        {SnapshotChunkType::METADATA, 13, 100},
        {SnapshotChunkType::NODES, 113, 200},
    };
    auto buf = buildChunkedBufferFromDescriptors(frames);

    DB::ReadBufferFromString in38(buf);
    auto parsed = parseAndValidateChunkedSnapshot(in38);

    ASSERT_EQ(parsed.size(), 2u);
    EXPECT_EQ(parsed[0].type, SnapshotChunkType::METADATA);
    EXPECT_EQ(parsed[0].compressed_offset, 13u);
    EXPECT_EQ(parsed[0].compressed_size, 100u);
    EXPECT_EQ(parsed[1].type, SnapshotChunkType::NODES);
    EXPECT_EQ(parsed[1].compressed_offset, 113u);
    EXPECT_EQ(parsed[1].compressed_size, 200u);
}

/// Multiple NODES frames round-trip correctly (K > 1 per the write path).
TEST(CoordinationChunkedSnapshotTest, RoundTripMultipleNodesFrames)
{
    // Frames start at offset 13 (just past front header); METADATA + 3 NODES.
    std::vector<SnapshotChunkDescriptor> frames;
    frames.push_back({SnapshotChunkType::METADATA, 13, 80});
    frames.push_back({SnapshotChunkType::NODES, 93, 300});
    frames.push_back({SnapshotChunkType::NODES, 393, 250});
    frames.push_back({SnapshotChunkType::NODES, 643, 150});

    auto buf = buildChunkedBufferFromDescriptors(frames);

    DB::ReadBufferFromString in37(buf);
    auto parsed = parseAndValidateChunkedSnapshot(in37);

    ASSERT_EQ(parsed.size(), 4u);
    EXPECT_EQ(parsed[0].type, SnapshotChunkType::METADATA);
    EXPECT_EQ(parsed[1].type, SnapshotChunkType::NODES);
    EXPECT_EQ(parsed[2].type, SnapshotChunkType::NODES);
    EXPECT_EQ(parsed[3].type, SnapshotChunkType::NODES);
    EXPECT_EQ(parsed[3].compressed_size, 150u);
}

/// Frames with gaps between them (non-contiguous) are still valid (but trailing gap before footer is not).
TEST(CoordinationChunkedSnapshotTest, RoundTripWithGapsBetweenFrames)
{
    // Frames start at offset 13 (just past front header); gaps between chunks are permitted.
    const uint64_t gap = 128;
    std::vector<SnapshotChunkDescriptor> frames = {
        {SnapshotChunkType::METADATA, 13, 100},
        {SnapshotChunkType::NODES, 13 + 100 + gap, 200},
    };

    auto buf = buildChunkedBufferFromDescriptors(frames);

    DB::ReadBufferFromString in36(buf);
    auto parsed = parseAndValidateChunkedSnapshot(in36);
    ASSERT_EQ(parsed.size(), 2u);
    EXPECT_EQ(parsed[1].compressed_offset, 13u + 100u + gap);
}

// --- Rejection tests ---------------------------------------------------------

TEST(CoordinationChunkedSnapshotTest, RejectsBufTooSmall)
{
    // A buffer smaller than chunkedSnapshotHeaderSize() (13) must be rejected immediately.
    const size_t small_size = chunkedSnapshotHeaderSize() - 1;
    std::string buf(small_size, '\0');
    {
        DB::ReadBufferFromString in35(buf);
        EXPECT_THROW(parseAndValidateChunkedSnapshot(in35), DB::Exception);
    }
}

TEST(CoordinationChunkedSnapshotTest, RejectsWrongMagic)
{
    std::vector<SnapshotChunkDescriptor> frames = {
        {SnapshotChunkType::METADATA, 13, 10},
        {SnapshotChunkType::NODES, 23, 20},
    };
    auto buf = buildChunkedBufferFromDescriptors(frames);

    // Corrupt the first byte of the FRONT header magic.
    buf[0] ^= 0xFF;
    DB::ReadBufferFromString in34(buf);
    EXPECT_THROW(parseAndValidateChunkedSnapshot(in34), DB::Exception);
}

TEST(CoordinationChunkedSnapshotTest, RejectsChunkCountZeroAndOne)
{
    for (uint64_t cc : {uint64_t{0}, uint64_t{1}})
    {
        WriteBufferFromOwnString out;
        packChunkedSnapshotHeader(cc, out);
        out.write(std::string(100, '\0').data(), 100); // synthetic chunk region
        const std::vector<SnapshotChunkDescriptor> descs(static_cast<size_t>(cc));
        packChunkedSnapshotFooter(descs, out);
        out.finalize();
        auto buf = out.str();
        {
            DB::ReadBufferFromString in32(buf);
            EXPECT_THROW(parseAndValidateChunkedSnapshot(in32), DB::Exception);
        }
    }
}

TEST(CoordinationChunkedSnapshotTest, RejectsChunkExtendingIntoFooter)
{
    // A chunk whose [offset, offset+size) extends past footer_offset.
    // Build a valid 2-chunk buffer, then hand-corrupt the last chunk's compressed_size
    // in the footer descriptor so the chunk appears to extend into the footer region.
    std::vector<SnapshotChunkDescriptor> frames = {
        {SnapshotChunkType::METADATA, 13, 100},
        {SnapshotChunkType::NODES, 113, 200}, // last byte at 313 = footer_offset
    };
    auto buf = buildChunkedBufferFromDescriptors(frames);

    // Locate the NODES descriptor in the footer and inflate its compressed_size by 1.
    // footer_offset = 313; footer starts at buf[313]; NODES is the 2nd descriptor (index 1).
    const uint64_t footer_offset = 313;
    char * footer = buf.data() + footer_offset;
    // descriptor[1]: type(1) + offset(8) + size field at offset 9 into this descriptor
    uint64_t bad_size = 201; // one byte past footer_offset
    memcpy(footer + KEEPER_CHUNKED_SNAPSHOT_DESCRIPTOR_SIZE * 1 + 1 + 8, &bad_size, 8);
    {
        DB::ReadBufferFromString in31(buf);
        EXPECT_THROW(parseAndValidateChunkedSnapshot(in31), DB::Exception);
    }
}

TEST(CoordinationChunkedSnapshotTest, RejectsOverlappingFrames)
{
    // Frame 1 starts before frame 0 ends (overlapping by 1 byte).
    std::vector<SnapshotChunkDescriptor> frames = {
        {SnapshotChunkType::METADATA, 13, 100},
        {SnapshotChunkType::NODES, 112, 200}, // starts 1 byte before frame 0 ends (13+100=113)
    };
    auto buf = buildChunkedBufferFromDescriptors(frames);
    {
        DB::ReadBufferFromString in30(buf);
        EXPECT_THROW(parseAndValidateChunkedSnapshot(in30), DB::Exception);
    }
}

TEST(CoordinationChunkedSnapshotTest, RejectsFrameExceedingBufferSize)
{
    // Build a valid buffer, then corrupt the last descriptor's compressed_size so the
    // chunk appears to extend past footer_offset.
    std::vector<SnapshotChunkDescriptor> frames = {
        {SnapshotChunkType::METADATA, 13, 100},
        {SnapshotChunkType::NODES, 113, 200},
    };
    auto buf = buildChunkedBufferFromDescriptors(frames);

    // footer_offset = 313; NODES descriptor (index 1) has compressed_size field
    // at footer_offset + DESCRIPTOR_SIZE*1 + 1 (type) + 8 (offset) = 313 + 25 + 9 = 347
    const uint64_t footer_offset = 313;
    char * footer = buf.data() + footer_offset;
    uint64_t bad_size = 9999; // way too large — 113 + 9999 > footer_offset=313
    memcpy(footer + KEEPER_CHUNKED_SNAPSHOT_DESCRIPTOR_SIZE * 1 + 1 + 8, &bad_size, 8);
    {
        DB::ReadBufferFromString in29(buf);
        EXPECT_THROW(parseAndValidateChunkedSnapshot(in29), DB::Exception);
    }
}

TEST(CoordinationChunkedSnapshotTest, RejectsNonMonotonicOffsets)
{
    // First frame at a higher offset than the second — non-monotonic (NODES starts before METADATA ends).
    std::vector<SnapshotChunkDescriptor> frames = {
        {SnapshotChunkType::METADATA, 300, 10},
        {SnapshotChunkType::NODES, 100, 200}, // starts before frame 0 ends (300+10=310 > 100)
    };
    auto buf = buildChunkedBufferFromDescriptors(frames);
    {
        DB::ReadBufferFromString in26(buf);
        EXPECT_THROW(parseAndValidateChunkedSnapshot(in26), DB::Exception);
    }
}

TEST(CoordinationChunkedSnapshotTest, ChunkCountExceedsBufferCapacity)
{
    // chunk_count so large that chunkedSnapshotFooterSize(chunk_count) would not fit in the buffer.
    // Total buffer = 32 bytes; header occupies first 13, leaving only 19 bytes after it.
    // chunk_count=100 → footer_size=2500, which far exceeds the 19 available bytes.
    WriteBufferFromOwnString out;
    packChunkedSnapshotHeader(100, out);
    // Pad to 32 bytes total (32 - 13 header bytes = 19 zeros)
    out.write(std::string(32 - chunkedSnapshotHeaderSize(), '\0').data(), 32 - chunkedSnapshotHeaderSize());
    out.finalize();
    auto buf = out.str();
    {
        DB::ReadBufferFromString in25(buf);
        EXPECT_THROW(parseAndValidateChunkedSnapshot(in25), DB::Exception);
    }
}

/// chunk_count = (SIZE_MAX/25)+1 would wrap footer_size; the division-based guard catches it.
TEST(CoordinationChunkedSnapshotTest, ChunkCountSizeTOverflowRejected)
{
    const uint64_t chunk_count
        = static_cast<uint64_t>(std::numeric_limits<size_t>::max() / KEEPER_CHUNKED_SNAPSHOT_DESCRIPTOR_SIZE) + 1;

    WriteBufferFromOwnString out;
    packChunkedSnapshotHeader(chunk_count, out);
    out.write(std::string(32, '\0').data(), 32);
    out.finalize();
    auto buf = out.str();

    {
        DB::ReadBufferFromString in_parse(buf);
        EXPECT_THROW(parseAndValidateChunkedSnapshot(in_parse), DB::Exception);
    }
    {
        DB::ReadBufferFromString in_detect(buf);
        EXPECT_FALSE(isChunkedSnapshot(in_detect));
    }
}

/// Verify serializeSnapshotToBuffer produces a structurally valid V9 chunked snapshot.
TEST(CoordinationChunkedSnapshotTest, InspectBytes)
{
    ChangelogDirTest snap_dir("./chunked_write_test_snap");

    // Set up a minimal KeeperContext with default settings.
    auto keeper_context = makeKeeperContext(snap_dir.path);

    // Create a small storage with a few nodes.
    DB::KeeperStorage storage(500, "", keeper_context);
    addNode(storage, "/alpha", "hello");
    addNode(storage, "/beta", "world");
    addNode(storage, "/gamma", "data");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 10;

    // Build a chunked snapshot (explicit version passed to the snapshot constructor).
    DB::KeeperStorageSnapshot snapshot(
        &storage, /*up_to_log_idx=*/10, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);

    DB::KeeperSnapshotManager manager(3, keeper_context, /*compress_snapshots_zstd_=*/true);
    auto buf = manager.serializeSnapshotToBuffer(snapshot);

    ASSERT_NE(buf, nullptr);
    ASSERT_GT(buf->size(), 0u);

    const char * data = reinterpret_cast<const char *>(buf->data_begin());
    const size_t data_size = buf->size();

    // Parse and validate the chunked snapshot; throws on any structural violation.
    DB::ReadBufferFromNuraftBuffer in24(buf);
    auto frames = parseAndValidateChunkedSnapshot(in24);

    // Must have METADATA + at least 1 NODES.
    ASSERT_GE(frames.size(), DB::KEEPER_CHUNKED_SNAPSHOT_MIN_CHUNK_COUNT);

    // Chunk ordering: METADATA first, all subsequent chunks are NODES.
    EXPECT_EQ(frames.front().type, SnapshotChunkType::METADATA);
    EXPECT_EQ(frames.back().type, SnapshotChunkType::NODES);
    for (size_t i = 1; i < frames.size(); ++i)
        EXPECT_EQ(frames[i].type, SnapshotChunkType::NODES) << "Chunk " << i << " should be NODES";

    // Front-header layout: CKFS magic is at the front (first 4 bytes), version at byte 4,
    // and chunk_count at bytes 5-12.
    EXPECT_EQ(memcmp(data, "CKFS", 4), 0) << "CKFS magic must be at the front (header)";
    EXPECT_EQ(static_cast<uint8_t>(data[4]), 8u) << "Header version byte must be 8";
    {
        uint64_t hdr_count = 0;
        memcpy(&hdr_count, data + 5, 8);
        EXPECT_EQ(hdr_count, frames.size()) << "Header chunk_count must match parsed frame count";
    }
    // First chunk must start at offset 13 (just past the 13-byte front header).
    EXPECT_EQ(frames.front().compressed_offset, 13u) << "First chunk must start at offset 13 in the front-header layout";
    // Last 4 bytes must NOT be CKFS (no trailer).
    EXPECT_NE(memcmp(data + data_size - 4, "CKFS", 4), 0) << "Last bytes must NOT be CKFS (no trailer in front-header layout)";

    // Every frame must begin with the ZSTD magic bytes (0x28 0xB5 0x2F 0xFD).
    static constexpr unsigned char kZstdMagic[4] = {0x28, 0xB5, 0x2F, 0xFD};
    for (size_t i = 0; i < frames.size(); ++i)
    {
        const auto & f = frames[i];
        ASSERT_GE(data_size, f.compressed_offset + 4u) << "Frame " << i << " too short for magic check";
        EXPECT_EQ(memcmp(data + f.compressed_offset, kZstdMagic, 4), 0) << "Frame " << i << " does not start with ZSTD magic";
    }

    // Verify that the first NODES frame descriptor carries a non-zero node_count.
    // node_count lives in the footer descriptor (not the frame body).
    // Storage has 4 non-system nodes: /, /alpha, /beta, /gamma (system nodes excluded).
    // With the default chunk_size_limit (100000), all 4 fit in one NODES frame.
    {
        const auto & nf = frames[1]; // first NODES frame
        EXPECT_GE(nf.node_count, 1u) << "NODES frame descriptor must carry at least the root node count";
    }
}

/// Verify serializeSnapshotToDisk: chunk ordering, ZSTD magic, and KeeperSnapshotWrittenBytes.
TEST(CoordinationChunkedSnapshotTest, DiskInspectBytes)
{
    ChangelogDirTest snap_dir("./chunked_disk_test_snap");

    auto settings = std::make_shared<DB::CoordinationSettings>();
    auto keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_context->setLocalLogsPreprocessed();
    auto snap_disk = std::make_shared<DB::DiskLocal>("SnapDiskDisk", snap_dir.path);
    keeper_context->setSnapshotDisk(snap_disk);

    DB::KeeperStorage storage(500, "", keeper_context);
    addNode(storage, "/p", "v1");
    addNode(storage, "/q", "v2");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 5;

    // V9 is the maximum supported version; SnapshotVersion::V9 is passed directly for clarity.
    DB::KeeperStorageSnapshot snapshot(
        &storage, /*up_to_log_idx=*/5, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);

    DB::KeeperSnapshotManager manager(3, keeper_context, /*compress_snapshots_zstd_=*/false);

    // Capture the global KeeperSnapshotWrittenBytes counter before writing.
    const auto written_before = ProfileEvents::global_counters[ProfileEvents::KeeperSnapshotWrittenBytes].load();

    auto file_info = manager.serializeSnapshotToDisk(snapshot);
    ASSERT_NE(file_info, nullptr);

    const auto written_after = ProfileEvents::global_counters[ProfileEvents::KeeperSnapshotWrittenBytes].load();
    const uint64_t reported_bytes = written_after - written_before;

    // Read the snapshot file as raw bytes.
    const std::string abs_path = snap_dir.path + "/" + file_info->path;
    std::ifstream raw_file(abs_path, std::ios::binary | std::ios::ate);
    ASSERT_TRUE(raw_file.is_open()) << "Cannot open chunked snapshot on disk: " << abs_path;
    const size_t file_size = static_cast<size_t>(raw_file.tellg());
    raw_file.seekg(0, std::ios::beg);
    std::string raw(file_size, '\0');
    raw_file.read(raw.data(), static_cast<std::streamsize>(file_size));
    raw_file.close();

    // Reported bytes must equal actual file size (append-only — count() is the final file size).
    EXPECT_EQ(reported_bytes, file_size) << "KeeperSnapshotWrittenBytes mismatch — count() must equal the final file size";

    ASSERT_GT(file_size, 0u);

    // Parse and validate the chunked snapshot.
    DB::ReadBufferFromString in23(raw);
    auto frames = parseAndValidateChunkedSnapshot(in23);

    ASSERT_GE(frames.size(), DB::KEEPER_CHUNKED_SNAPSHOT_MIN_CHUNK_COUNT);
    EXPECT_EQ(frames.front().type, SnapshotChunkType::METADATA);
    EXPECT_EQ(frames.back().type, SnapshotChunkType::NODES);
    for (size_t i = 1; i < frames.size(); ++i)
        EXPECT_EQ(frames[i].type, SnapshotChunkType::NODES) << "Chunk " << i << " should be NODES";

    // Front-header layout: CKFS magic at front, first chunk at offset 13, no tail magic.
    EXPECT_EQ(memcmp(raw.data(), "CKFS", 4), 0) << "CKFS magic must be at front (header)";
    EXPECT_EQ(frames.front().compressed_offset, 13u) << "First chunk must start at offset 13";
    EXPECT_NE(memcmp(raw.data() + file_size - 4, "CKFS", 4), 0) << "Last bytes must NOT be CKFS (no trailer)";

    static constexpr unsigned char kZstdMagic[4] = {0x28, 0xB5, 0x2F, 0xFD};
    for (size_t i = 0; i < frames.size(); ++i)
    {
        ASSERT_GE(file_size, frames[i].compressed_offset + 4u) << "Chunk " << i << " too short";
        EXPECT_EQ(memcmp(raw.data() + frames[i].compressed_offset, kZstdMagic, 4), 0) << "Chunk " << i << " does not start with ZSTD magic";
    }
}

// ─── Chunked snapshot read-path tests (sequential deserialization + validating load API) ──────────

/// Round-trip a chunked snapshot: verify nodes, ACLs, sessions, and ephemerals are faithfully restored.
TEST(CoordinationChunkedSnapshotTest, RoundTripBasic)
{
    ChangelogDirTest snap_dir("./chunked_read_roundtrip");

    auto keeper_context = makeKeeperContext(snap_dir.path);

    // Build a storage with a few nodes (some ephemeral) and sessions.
    DB::KeeperStorage storage(500, "", keeper_context);

    // Add ACL mapping id=1 → one ACL entry so the ACL map round-trip is covered.
    Coordination::ACL acl1;
    acl1.permissions = 0x1f; // all
    acl1.scheme = "auth";
    acl1.id = "";
    storage.acl_map.addMapping(1, {acl1});

    addNode(storage, "/persistent", "hello", /*ephemeral_owner=*/0, /*acl_id=*/1);
    addNode(storage, "/ephnode", "tmp", /*ephemeral_owner=*/42);
    addNode(storage, "/subdir", "dir");
    addNode(storage, "/subdir/child", "child_data");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 7;
    storage.session_id_counter = 100;

    // Add a session (id=42) so ephemeral ownership is tracked.
    storage.committed_ephemerals[42].insert("/ephnode");
    ++storage.committed_ephemeral_nodes;
    storage.addSessionID(42, 30000);
    storage.committed_session_and_auth[42] = {{"digest", "user:pass"}};

    // Serialize as chunked format.
    DB::KeeperStorageSnapshot snap(
        &storage, /*up_to_log_idx=*/7, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);
    DB::KeeperSnapshotManager mgr(3, keeper_context, /*compress_snapshots_zstd_=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // Verify front-header layout: CKFS magic at offset 0, version byte 8 at offset 4, NO tail magic.
    ASSERT_GE(buf->size(), static_cast<size_t>(chunkedSnapshotHeaderSize()));
    const char * data_ptr = reinterpret_cast<const char *>(buf->data_begin());
    EXPECT_EQ(memcmp(data_ptr, "CKFS", 4), 0) << "Chunked snapshot must start with CKFS magic (front-header layout)";
    EXPECT_EQ(static_cast<uint8_t>(data_ptr[4]), 8u) << "Front header version byte must be 8";
    EXPECT_NE(memcmp(data_ptr + buf->size() - 4, "CKFS", 4), 0)
        << "Chunked snapshot must NOT end with CKFS (no trailer in front-header layout)";

    // Deserialize via the public API — must route to deserializeChunkedSnapshotFromBuffer.
    auto result = mgr.deserializeSnapshotFromBuffer(buf, /*load_full_storage=*/true);
    ASSERT_NE(result.storage, nullptr);
    ASSERT_NE(result.snapshot_meta, nullptr);

    const auto & s = *result.storage;

    // Root must exist.
    EXPECT_NE(s.container.find("/"), s.container.end());

    // /persistent node with correct data and acl_id.
    auto it_persistent = s.container.find("/persistent");
    ASSERT_NE(it_persistent, s.container.end());
    EXPECT_EQ(it_persistent->value.getData(), "hello");
    EXPECT_EQ(it_persistent->value.acl_id, 1u);

    // /ephnode must exist and be ephemeral.
    auto it_eph = s.container.find("/ephnode");
    ASSERT_NE(it_eph, s.container.end());
    EXPECT_TRUE(it_eph->value.stats.isEphemeral());
    EXPECT_EQ(it_eph->value.stats.ephemeralOwner(), 42);

    // /subdir/child must exist.
    EXPECT_NE(s.container.find("/subdir/child"), s.container.end());

    // Ephemeral tracking restored.
    EXPECT_EQ(s.committed_ephemeral_nodes, 1u);
    auto eph_it = s.committed_ephemerals.find(42);
    ASSERT_NE(eph_it, s.committed_ephemerals.end());
    EXPECT_TRUE(eph_it->second.count("/ephnode"));

    // Session restored.
    EXPECT_EQ(s.session_id_counter, storage.session_id_counter);
    auto auth_it = s.committed_session_and_auth.find(42);
    ASSERT_NE(auth_it, s.committed_session_and_auth.end());
    ASSERT_EQ(auth_it->second.size(), 1u);
    EXPECT_EQ(auth_it->second[0].scheme, "digest");
    EXPECT_EQ(auth_it->second[0].id, "user:pass");

    // ACL map restored — addMapping for id=1 must have been called.
    auto restored_acls = s.acl_map.convertNumber(1);
    EXPECT_EQ(restored_acls.size(), 1u);
    EXPECT_EQ(restored_acls[0].scheme, "auth");
}

/// Verify that `deserializeSnapshotMetadataFromBuffer` correctly extracts the log index
/// from a chunked snapshot without loading the full storage.
TEST(CoordinationChunkedSnapshotTest, MetadataOnlyExtraction)
{
    ChangelogDirTest snap_dir("./chunked_read_meta");

    auto keeper_context = makeKeeperContext(snap_dir.path);

    DB::KeeperStorage storage(500, "", keeper_context);
    addNode(storage, "/a", "data");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 42;

    DB::KeeperStorageSnapshot snap(
        &storage, /*up_to_log_idx=*/42, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);
    DB::KeeperSnapshotManager mgr(3, keeper_context, /*compress_snapshots_zstd_=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // Must extract snapshot_meta without loading nodes.
    auto meta = mgr.deserializeSnapshotMetadataFromBuffer(buf);
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->get_last_log_idx(), 42u);
}

/// Verify paths-only mode: `load_full_storage=false` collects paths without building the storage.
TEST(CoordinationChunkedSnapshotTest, PathsOnlyMode)
{
    ChangelogDirTest snap_dir("./chunked_read_paths");

    auto keeper_context = makeKeeperContext(snap_dir.path);

    DB::KeeperStorage storage(500, "", keeper_context);
    addNode(storage, "/x", "x_data");
    addNode(storage, "/y", "y_data");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 5;

    DB::KeeperStorageSnapshot snap(
        &storage, /*up_to_log_idx=*/5, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);
    DB::KeeperSnapshotManager mgr(3, keeper_context, /*compress_snapshots_zstd_=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // load_full_storage=false: paths collected, storage is null (or empty — implementation may vary).
    auto result = mgr.deserializeSnapshotFromBuffer(buf, /*load_full_storage=*/false);
    ASSERT_FALSE(result.paths.empty()) << "paths must be collected in path-only mode";

    // At minimum /, /x, /y should be present.
    const auto & paths = result.paths;
    EXPECT_NE(std::find(paths.begin(), paths.end(), "/"), paths.end()) << "root path missing";
    EXPECT_NE(std::find(paths.begin(), paths.end(), "/x"), paths.end()) << "/x missing";
    EXPECT_NE(std::find(paths.begin(), paths.end(), "/y"), paths.end()) << "/y missing";
}

/// V7 snapshots still load correctly when the writer supports V9.
TEST(CoordinationChunkedSnapshotTest, LegacyV7SnapshotStillLoads)
{
    ChangelogDirTest snap_dir("./chunked_read_legacy");

    auto keeper_context = makeKeeperContext(snap_dir.path);

    DB::KeeperStorage storage(500, "", keeper_context);
    addNode(storage, "/legacy", "old_data");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 3;

    // Write as V7 (legacy ZSTD).
    DB::KeeperStorageSnapshot snap(
        &storage, /*up_to_log_idx=*/3, /*cluster_config=*/nullptr, DB::SnapshotVersion::V7);
    DB::KeeperSnapshotManager mgr(3, keeper_context, /*compress_snapshots_zstd_=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // Must NOT end with CKFS magic — it's a ZSTD-compressed legacy snapshot without a chunked trailer.
    ASSERT_GE(buf->size(), 4u);
    const char * v7_data = reinterpret_cast<const char *>(buf->data_begin());
    EXPECT_NE(memcmp(v7_data + buf->size() - 4, "CKFS", 4), 0) << "V7 snapshot must not have CKFS magic at tail";

    // 3-way detection should route to legacy ZSTD path.
    auto result = mgr.deserializeSnapshotFromBuffer(buf, /*load_full_storage=*/true);
    ASSERT_NE(result.storage, nullptr);
    ASSERT_NE(result.storage->container.find("/legacy"), result.storage->container.end());
}

// ─── Chunked snapshot validation / corruption tests ──────────────────────────────────────────────

/// Metadata-only deserialization reads only the METADATA chunk; corrupt NODES do not affect it,
/// but the full deserializer detects the same corruption.
TEST(CoordinationChunkedSnapshotTest, MetadataOnlyFastPath)
{
    ChangelogDirTest snap_dir("./chunked_val_metafast");

    auto keeper_context = makeKeeperContext(snap_dir.path);

    DB::KeeperStorage storage(500, "", keeper_context);
    addNode(storage, "/node1", "v1");
    addNode(storage, "/node2", "v2");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 99;

    DB::KeeperStorageSnapshot snap(
        &storage, /*up_to_log_idx=*/99, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);
    DB::KeeperSnapshotManager mgr(3, keeper_context, /*compress_snapshots_zstd_=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // Locate the first NODES chunk and corrupt its middle bytes.
    DB::ReadBufferFromNuraftBuffer in22(buf);
    auto frames = parseAndValidateChunkedSnapshot(in22);
    ASSERT_GE(frames.size(), DB::KEEPER_CHUNKED_SNAPSHOT_MIN_CHUNK_COUNT);

    // Find the first NODES chunk.
    const SnapshotChunkDescriptor * nodes_fd = nullptr;
    for (const auto & f : frames)
    {
        if (f.type == SnapshotChunkType::NODES)
        {
            nodes_fd = &f;
            break;
        }
    }
    ASSERT_NE(nodes_fd, nullptr) << "No NODES chunk found";

    // Corrupt the middle of the NODES chunk (ZSTD checksum will catch this).
    auto * raw_mut = reinterpret_cast<char *>(buf->data_begin());
    const size_t corrupt_pos = nodes_fd->compressed_offset + nodes_fd->compressed_size / 2;
    raw_mut[corrupt_pos] ^= static_cast<char>(0xFF);

    // Metadata-only path MUST succeed — it never touches the corrupt NODES chunk.
    auto meta = mgr.deserializeSnapshotMetadataFromBuffer(buf);
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->get_last_log_idx(), 99u) << "Metadata-only path must return correct log index despite corrupt NODES chunk";

    // Full deserializer MUST detect the corruption.
    EXPECT_THROW(mgr.deserializeSnapshotFromBuffer(buf, /*load_full_storage=*/true), DB::Exception)
        << "Full deserializer must throw on NODES chunk corruption";
}

/// Front-header version != 8 throws UNKNOWN_FORMAT_VERSION before any chunk data is read.
TEST(CoordinationChunkedSnapshotTest, WrongHeaderVersionRejected)
{
    ChangelogDirTest snap_dir("./chunked_val_hdrver");

    auto keeper_context = makeKeeperContext(snap_dir.path);

    DB::KeeperStorage storage(500, "", keeper_context);
    addNode(storage, "/z", "z_data");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 1;

    DB::KeeperStorageSnapshot snap(
        &storage, /*up_to_log_idx=*/1, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);
    DB::KeeperSnapshotManager mgr(3, keeper_context, /*compress_snapshots_zstd_=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);
    ASSERT_GE(buf->size(), chunkedSnapshotHeaderSize());

    // The version byte is at offset 4 from the start of the buffer (front header: magic[4]+version[1]).
    auto * raw_mut = reinterpret_cast<char *>(buf->data_begin());
    const size_t ver_idx = 4; // front header version field
    ASSERT_EQ(static_cast<uint8_t>(raw_mut[ver_idx]), 8u) << "Expected front header version byte 8";
    raw_mut[ver_idx] = 9; // simulate an unknown future version

    // isChunkedSnapshot passes version-independently (structural sniff succeeds, version not checked)
    // so both public APIs route to the parser, which throws UNKNOWN_FORMAT_VERSION.
    auto expect_unknown_version = [](auto && fn, const char * what)
    {
        try
        {
            fn();
            FAIL() << what << " must throw on unknown chunked header version";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::UNKNOWN_FORMAT_VERSION) << what << " expected UNKNOWN_FORMAT_VERSION, got: " << e.message();
        }
    };
    expect_unknown_version([&] { mgr.deserializeSnapshotFromBuffer(buf, true); }, "deserializeSnapshotFromBuffer");
    expect_unknown_version([&] { mgr.deserializeSnapshotMetadataFromBuffer(buf); }, "deserializeSnapshotMetadataFromBuffer");
    DB::ReadBufferFromNuraftBuffer in21(buf);
    expect_unknown_version([&] { parseAndValidateChunkedSnapshot(in21); }, "parseAndValidateChunkedSnapshot");
}

/// Corrupt NODES chunk payload is detected by the ZSTD checksum during decompression.
TEST(CoordinationChunkedSnapshotTest, CorruptNodeFrameRejected)
{
    ChangelogDirTest snap_dir("./chunked_val_corrupt");

    auto keeper_context = makeKeeperContext(snap_dir.path);

    DB::KeeperStorage storage(500, "", keeper_context);
    addNode(storage, "/n1", "v1");
    addNode(storage, "/n2", "v2");
    addNode(storage, "/n3", "v3");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 5;

    DB::KeeperStorageSnapshot snap(
        &storage, /*up_to_log_idx=*/5, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);
    DB::KeeperSnapshotManager mgr(3, keeper_context, /*compress_snapshots_zstd_=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    DB::ReadBufferFromNuraftBuffer in20(buf);
    auto frames = parseAndValidateChunkedSnapshot(in20);

    // Find the first NODES chunk.
    const SnapshotChunkDescriptor * nodes_fd = nullptr;
    for (const auto & f : frames)
    {
        if (f.type == SnapshotChunkType::NODES)
        {
            nodes_fd = &f;
            break;
        }
    }
    ASSERT_NE(nodes_fd, nullptr);

    // Flip 8 bits in the middle of the compressed NODES chunk payload
    // (well past the ZSTD frame header, so it hits payload bytes).
    auto * raw_mut = reinterpret_cast<char *>(buf->data_begin());
    const size_t corrupt_offset = nodes_fd->compressed_offset + nodes_fd->compressed_size / 2;
    raw_mut[corrupt_offset] ^= static_cast<char>(0xFF);

    EXPECT_THROW(mgr.deserializeSnapshotFromBuffer(buf, /*load_full_storage=*/true), DB::Exception)
        << "Corrupt NODES chunk must throw during full deserialization";
}

/// Invalid footer descriptor type is rejected by the parser.
TEST(CoordinationChunkedSnapshotTest, CorruptFooterDescriptorRejected)
{
    ChangelogDirTest snap_dir("./chunked_val_corrupt_footer");

    auto keeper_context = makeKeeperContext(snap_dir.path);

    DB::KeeperStorage storage(500, "", keeper_context);
    addNode(storage, "/cfd1", "v1");
    addNode(storage, "/cfd2", "v2");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 3;

    DB::KeeperStorageSnapshot snap(
        &storage, /*up_to_log_idx=*/3, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);
    DB::KeeperSnapshotManager mgr(3, keeper_context, /*compress_snapshots_zstd_=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    const size_t size = buf->size();

    // Find footer_offset from the front header (derived from buf_size and chunk_count).
    DB::ReadBufferFromNuraftBuffer in19(buf);
    auto frames = parseAndValidateChunkedSnapshot(in19);
    const size_t footer_offset = size - chunkedSnapshotFooterSize(frames.size());

    // Corrupt descriptor[0].type to an out-of-range value (0x7F > NODES=1).
    auto * raw_mut = reinterpret_cast<char *>(buf->data_begin());
    raw_mut[footer_offset] = static_cast<char>(0x7F);

    // The trailer arithmetic is still valid (footer_offset unchanged), so detection routes to the
    // chunked parser; the parser must reject the invalid chunk type.
    EXPECT_THROW(mgr.deserializeSnapshotFromBuffer(buf, /*load_full_storage=*/true), DB::Exception);
    EXPECT_THROW(mgr.deserializeSnapshotMetadataFromBuffer(buf), DB::Exception);
}

/// Paths-only mode (load_full_storage=false) collects paths without building the storage.
TEST(CoordinationChunkedSnapshotTest, PathsOnlyDoesNotFinalize)
{
    ChangelogDirTest snap_dir("./chunked_val_pathsonly");

    auto keeper_context = makeKeeperContext(snap_dir.path);

    DB::KeeperStorage storage(500, "", keeper_context);
    addNode(storage, "/dir", "d");
    addNode(storage, "/dir/child1", "c1");
    addNode(storage, "/dir/child2", "c2");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 3;

    DB::KeeperStorageSnapshot snap(
        &storage, /*up_to_log_idx=*/3, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);
    DB::KeeperSnapshotManager mgr(3, keeper_context, /*compress_snapshots_zstd_=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    auto result = mgr.deserializeSnapshotFromBuffer(buf, /*load_full_storage=*/false);

    // storage must be null or empty — analyzer mode must not build the map.
    EXPECT_TRUE(result.storage == nullptr || result.storage->container.size() == 0) << "Paths-only mode must not build the full storage";

    // All snapshot paths (including root) must appear in result.paths.
    const auto & paths = result.paths;
    EXPECT_NE(std::find(paths.begin(), paths.end(), "/"), paths.end()) << "/ missing";
    EXPECT_NE(std::find(paths.begin(), paths.end(), "/dir"), paths.end()) << "/dir missing";
    EXPECT_NE(std::find(paths.begin(), paths.end(), "/dir/child1"), paths.end()) << "/dir/child1 missing";
    EXPECT_NE(std::find(paths.begin(), paths.end(), "/dir/child2"), paths.end()) << "/dir/child2 missing";
}

// ── Chunked snapshot reader-level rejection tests ────────────────────────────

/// Synthetic node entry for tampered NODES chunks; num_children is int32_t to allow -1.
struct FakeNodeEntry
{
    std::string path = {};
    std::string data = {};
    uint32_t acl_id = 0;
    int64_t czxid = 0;
    int64_t mzxid = 0;
    int64_t ctime = 0;
    int64_t mtime = 0;
    int32_t version = 0;
    int32_t cversion = 0;
    int32_t aversion = 0;
    int64_t ephemeral_owner = 0;
    int32_t num_children = 0; ///< may be -1 for test 7c
    int64_t pzxid = 0;
    int64_t seq_num = 0;
};

/// Build uncompressed bytes for a NODES chunk body matching the V7/chunked node encoding.
static std::string buildNodesChunkBytes(const std::vector<FakeNodeEntry> & entries)
{
    WriteBufferFromOwnString w;

    for (const auto & e : entries)
    {
        writeBinary(e.path, w); // VarUInt(len) + bytes

        // V7/chunked node encoding (matches writeNode(V7) and readChunkedSnapshotNode):
        writeVarUInt(e.data.size(), w); // VarUInt data_size
        if (!e.data.empty())
            w.write(e.data.data(), e.data.size());

        writeBinary(e.acl_id, w); // uint32_t (V7+ layout)
        writeBinary(e.czxid, w);
        writeBinary(e.mzxid, w);
        writeBinary(e.ctime, w);
        writeBinary(e.mtime, w);
        writeBinary(e.version, w);
        writeBinary(e.cversion, w);
        writeBinary(e.aversion, w);
        writeBinary(e.ephemeral_owner, w);
        writeBinary(e.num_children, w); // int32_t (can be -1)
        writeBinary(e.pzxid, w);
        writeBinary(e.seq_num, w); // int64_t (V7+ layout)
    }

    return w.str();
}

/// Decompress and return the uncompressed content of a single chunk from a chunked snapshot buffer.
static std::string decompressSnapshotChunk(nuraft::ptr<nuraft::buffer> buf, size_t chunk_idx)
{
    DB::ReadBufferFromNuraftBuffer header_in(buf);
    auto frames = parseAndValidateChunkedSnapshot(header_in);
    if (chunk_idx >= frames.size())
        throw std::runtime_error("decompressSnapshotChunk: chunk_idx out of range");
    const SnapshotChunkDescriptor & fd = frames[chunk_idx];

    DB::ZstdInflatingReadBuffer inflating(header_in.getView(fd.compressed_offset, fd.compressed_size));
    WriteBufferFromOwnString out;
    copyData(inflating, out);
    out.finalize();
    return out.str();
}

/// Replace the first chunk of target_type in a chunked snapshot buffer with new uncompressed
/// content.  All other chunks are kept verbatim.  Returns a freshly-allocated nuraft::buffer.
/// node_count_for_replaced is written into the descriptor for the replaced chunk (0 for non-NODES chunks).
static nuraft::ptr<nuraft::buffer> replaceFirstChunkOfType(
    nuraft::ptr<nuraft::buffer> orig_buf,
    SnapshotChunkType target_type,
    const std::string & new_frame_bytes,
    uint64_t node_count_for_replaced = 0)
{
    const char * raw = reinterpret_cast<const char *>(orig_buf->data_begin());

    DB::ReadBufferFromNuraftBuffer in17(orig_buf);
    auto frames = parseAndValidateChunkedSnapshot(in17);

    // Find the first chunk of the requested type.
    size_t target_idx = frames.size();
    for (size_t i = 0; i < frames.size(); ++i)
    {
        if (frames[i].type == target_type)
        {
            target_idx = i;
            break;
        }
    }
    if (target_idx == frames.size())
        throw std::runtime_error("replaceFirstChunkOfType: target chunk type not found");

    // Compress the replacement using the same ZSTD parameters as the writer.
    std::string new_compressed;
    {
        auto compressed_buf = std::make_unique<WriteBufferFromOwnString>();
        auto * compressed_buf_ptr = compressed_buf.get();
        DB::ZstdDeflatingWriteBuffer deflating(std::move(compressed_buf), /*compression_level=*/3);
        deflating.write(new_frame_bytes.data(), new_frame_bytes.size());
        deflating.finalize();
        new_compressed = compressed_buf_ptr->str();
    }
    const size_t new_cs = new_compressed.size();

    // Compute new frame descriptors with updated offsets (chunks start at offset 13, past the front header).
    std::vector<SnapshotChunkDescriptor> new_descs;
    new_descs.reserve(frames.size());
    uint64_t pos = chunkedSnapshotHeaderSize(); // start at 13, just past the front header
    for (size_t i = 0; i < frames.size(); ++i)
    {
        const uint64_t payload_sz = (i == target_idx) ? new_cs : frames[i].compressed_size;
        const uint64_t nc = (i == target_idx) ? node_count_for_replaced : frames[i].node_count;
        new_descs.push_back(SnapshotChunkDescriptor{frames[i].type, pos, payload_sz, nc});
        pos += payload_sz;
    }
    // Build the new buffer: [front header][chunk data][footer]
    DB::WriteBufferFromNuraftBuffer out;
    packChunkedSnapshotHeader(static_cast<uint64_t>(frames.size()), out);
    for (size_t i = 0; i < frames.size(); ++i)
    {
        if (i == target_idx)
            out.write(new_compressed.data(), new_cs);
        else
            out.write(raw + frames[i].compressed_offset, frames[i].compressed_size);
    }
    packChunkedSnapshotFooter(new_descs, out);
    out.finalize();
    return out.getBuffer();
}

/// Convenience wrapper — replace the first NODES chunk (most common test operation).
/// entries is passed to carry the node_count for the descriptor; new_nodes_bytes is the uncompressed body.
static nuraft::ptr<nuraft::buffer> replaceFirstNodesChunk(
    nuraft::ptr<nuraft::buffer> orig_buf, const std::string & new_nodes_bytes, const std::vector<FakeNodeEntry> & entries)
{
    return replaceFirstChunkOfType(orig_buf, SnapshotChunkType::NODES, new_nodes_bytes, static_cast<uint64_t>(entries.size()));
}

// ── Rejection tests ──────────────────────────────────────────────────────────

/// NODES chunk with node_count == 0 → no root → CORRUPTED_DATA
/// ("Chunked snapshot has no root '/' node").
TEST(CoordinationChunkedSnapshotTest, EmptyNodesFrameNoRoot)
{
    ChangelogDirTest snap_dir("./chunked_val_emptyroot");

    auto keeper_ctx = makeKeeperContext(snap_dir.path);

    DB::KeeperStorage storage(500, "", keeper_ctx);
    addNode(storage, "/n1", "v1");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 1;

    DB::KeeperStorageSnapshot snap(
        &storage, /*up_to_log_idx=*/1, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);
    DB::KeeperSnapshotManager mgr(3, keeper_ctx, /*compress_snapshots_zstd_=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // Replace NODES with an empty frame (node_count=0, no nodes at all).
    // Deserialization must reject: no '/' root in container.
    const std::vector<FakeNodeEntry> empty_entries;
    auto tampered = replaceFirstNodesChunk(buf, buildNodesChunkBytes(empty_entries), empty_entries);

    EXPECT_THROW(mgr.deserializeSnapshotFromBuffer(tampered, /*load_full_storage=*/true), DB::Exception)
        << "Empty NODES frame must be rejected (no '/' root)";
}

/// A node whose declared numChildren is 0 but that has one actual child.
/// Loading must reject because children.size() > numChildren().
TEST(CoordinationChunkedSnapshotTest, NumChildrenOvercount)
{
    ChangelogDirTest snap_dir("./chunked_val_overcount");

    auto keeper_ctx = makeKeeperContext(snap_dir.path);

    DB::KeeperStorage storage(500, "", keeper_ctx);
    addNode(storage, "/a", "v");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 1;

    DB::KeeperStorageSnapshot snap(
        &storage, /*up_to_log_idx=*/1, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);
    DB::KeeperSnapshotManager mgr(3, keeper_ctx, /*compress_snapshots_zstd_=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // '/' declares numChildren=0 but '/a' is a real child.
    // addChild("a") → children.size()==1 > numChildren()==0 → CORRUPTED_DATA.
    FakeNodeEntry root;
    root.path = "/";
    root.num_children = 0; // under-declares: actual child '/a' will exceed this

    FakeNodeEntry child_a;
    child_a.path = "/a";
    child_a.num_children = 0;

    const std::vector<FakeNodeEntry> entries_over{root, child_a};
    auto tampered = replaceFirstNodesChunk(buf, buildNodesChunkBytes(entries_over), entries_over);

    EXPECT_THROW(mgr.deserializeSnapshotFromBuffer(tampered, /*load_full_storage=*/true), DB::Exception)
        << "numChildren=0 with one actual child must be rejected (children.size() > numChildren())";
}

/// A node whose declared numChildren is 5 but that has only one actual child.
/// The post-load equality check must reject: out_non_root (1) != out_total_children (5).
TEST(CoordinationChunkedSnapshotTest, NumChildrenUndercount)
{
    ChangelogDirTest snap_dir("./chunked_val_undercount");

    auto keeper_ctx = makeKeeperContext(snap_dir.path);

    DB::KeeperStorage storage(500, "", keeper_ctx);
    addNode(storage, "/a", "v");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 1;

    DB::KeeperStorageSnapshot snap(
        &storage, /*up_to_log_idx=*/1, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);
    DB::KeeperSnapshotManager mgr(3, keeper_ctx, /*compress_snapshots_zstd_=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // '/' over-declares numChildren=5 but only '/a' exists (1 non-root node).
    // The per-child check passes (1 ≤ 5); the post-load equality check rejects: out_non_root(1) != out_total_children(5).
    FakeNodeEntry root;
    root.path = "/";
    root.num_children = 5; // over-declares: only one actual child

    FakeNodeEntry child_a;
    child_a.path = "/a";
    child_a.num_children = 0;

    const std::vector<FakeNodeEntry> entries_under{root, child_a};
    auto tampered = replaceFirstNodesChunk(buf, buildNodesChunkBytes(entries_under), entries_under);

    EXPECT_THROW(mgr.deserializeSnapshotFromBuffer(tampered, /*load_full_storage=*/true), DB::Exception)
        << "numChildren=5 with only 1 actual child must be rejected (out_non_root != out_total_children)";
}

/// (7c) A node whose raw num_children field is -1.
/// readChunkedSnapshotNode must reject before finalizeMemorySnapshotLoad is ever reached.
TEST(CoordinationChunkedSnapshotTest, NegativeNumChildrenRejected)
{
    ChangelogDirTest snap_dir("./chunked_val_negchildren");

    auto keeper_ctx = makeKeeperContext(snap_dir.path);

    DB::KeeperStorage storage(500, "", keeper_ctx);
    addNode(storage, "/n1", "v1");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 1;

    DB::KeeperStorageSnapshot snap(
        &storage, /*up_to_log_idx=*/1, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);
    DB::KeeperSnapshotManager mgr(3, keeper_ctx, /*compress_snapshots_zstd_=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // Root node has num_children = -1.
    // readChunkedSnapshotNode must throw CORRUPTED_DATA before finalize is reached.
    FakeNodeEntry root;
    root.path = "/";
    root.num_children = -1; // invalid — must be rejected by readChunkedSnapshotNode

    const std::vector<FakeNodeEntry> entries_neg{root};
    auto tampered = replaceFirstNodesChunk(buf, buildNodesChunkBytes(entries_neg), entries_neg);

    EXPECT_THROW(mgr.deserializeSnapshotFromBuffer(tampered, /*load_full_storage=*/true), DB::Exception)
        << "Negative num_children must be rejected by readChunkedSnapshotNode";
}

/// Trailing bytes after the cluster config in the METADATA chunk must throw CORRUPTED_DATA
/// (caught by the EOF drain check, not a readVarUInt error).
TEST(CoordinationChunkedSnapshotTest, MetadataTrailingBytesRejected)
{
    ChangelogDirTest snap_dir("./chunked_val_metaeof");

    auto keeper_context = makeKeeperContext(snap_dir.path);

    DB::KeeperStorage storage(500, "", keeper_context);
    addNode(storage, "/node1", "v1");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 7;

    // Serialize WITH a cluster config so the METADATA chunk ends with a length-prefixed
    // cluster-config block.  The trailing byte is appended AFTER the complete cluster-config
    // so only the drain `if (!rbuf.eof())` fires — not readVarUInt.
    auto cluster_cfg = std::make_shared<nuraft::cluster_config>();
    DB::KeeperStorageSnapshot snap(&storage, /*up_to_log_idx=*/7, cluster_cfg, DB::SnapshotVersion::V9);
    DB::KeeperSnapshotManager mgr(3, keeper_context, /*compress_snapshots_zstd_=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // Decompress the METADATA chunk (chunks[0]) and append a single trailing garbage byte
    // AFTER the last valid byte of the cluster-config block.
    std::string meta_bytes = decompressSnapshotChunk(buf, 0);
    meta_bytes += '\xAB';

    // Replace the METADATA chunk with the tampered version.
    auto tampered = replaceFirstChunkOfType(buf, SnapshotChunkType::METADATA, meta_bytes);

    // Must throw CORRUPTED_DATA from the drain check (not a generic read error).
    try
    {
        mgr.deserializeSnapshotFromBuffer(tampered, /*load_full_storage=*/true);
        FAIL() << "deserializeSnapshotFromBuffer must throw on trailing bytes in METADATA chunk";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA) << "Expected CORRUPTED_DATA from METADATA-chunk drain, got: " << e.message();
    }
}

/// Shrinking a chunk's compressed_size to drop the 4-byte ZSTD checksum epilogue must be detected.
/// Covers serial, parallel (deser_threads=8), and METADATA chunk paths.
TEST(CoordinationChunkedSnapshotTest, F1ZstdFrameTrailerDropped)
{
    ChangelogDirTest snap_dir("./chunked_val_f1_trailer");

    // chunk_size=1: each node gets its own NODES chunk.
    // With nodes /, /a, /b that gives K=3 NODES chunks → parallel path (K > 1) is reachable.
    auto settings_src = std::make_shared<DB::CoordinationSettings>();
    (*settings_src)[DB::CoordinationSetting::snapshot_chunk_size] = 1;
    auto ctx_src = std::make_shared<DB::KeeperContext>(true, settings_src);
    ctx_src->setLocalLogsPreprocessed();
    ctx_src->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDiskF1Src", snap_dir.path));
    ctx_src->setDigestEnabled(false);

    DB::KeeperStorage storage_src(500, "", ctx_src);
    addNode(storage_src, "/a", "aaa");
    addNode(storage_src, "/b", "bbb");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage_src.zxid) = 10;
    storage_src.session_id_counter = 50;

    nuraft::ptr<nuraft::buffer> good_buf;
    {
        DB::KeeperStorageSnapshot snap(
            &storage_src, /*up_to_log_idx=*/10, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);
        DB::KeeperSnapshotManager mgr_src(3, ctx_src, /*compress_snapshots_zstd_=*/true);
        good_buf = mgr_src.serializeSnapshotToBuffer(snap);
    }
    ASSERT_NE(good_buf, nullptr);

    // Parse and verify K ≥ 2 NODES chunks (needed for the parallel path test).
    std::vector<DB::SnapshotChunkDescriptor> frames;
    {
        DB::ReadBufferFromNuraftBuffer in16(good_buf);
        frames = DB::parseAndValidateChunkedSnapshot(in16);
    }
    size_t nodes_count = 0;
    size_t first_nodes_frame_idx = 0; // index in `frames[]` of the first NODES chunk
    for (size_t i = 0; i < frames.size(); ++i)
    {
        if (frames[i].type == DB::SnapshotChunkType::NODES)
        {
            if (nodes_count == 0)
                first_nodes_frame_idx = i;
            ++nodes_count;
        }
    }
    ASSERT_GE(nodes_count, 2u) << "Need ≥2 NODES chunks (chunk=1 with 3 nodes) for parallel path";

    // Return a copy of good_buf with chunks[frame_idx].compressed_size shrunk by 4.
    // The 4 bytes are the ZSTD content-checksum epilogue (ZSTD_c_checksumFlag=1).
    // They become an inter-chunk gap — legal under the non-overlap-only footer check —
    // so parseAndValidateChunkedSnapshotFooter still passes on the tampered buffer.
    // Footer layout: compressed_size of descriptor i is at
    //   footer_offset + DESCRIPTOR_SIZE * i + 1 (type) + 8 (offset)   [= +9 into descriptor]
    // Each descriptor is 25 bytes (type=1 + compressed_offset=8 + compressed_size=8 + node_count=8).
    const size_t footer_offset = good_buf->size() - chunkedSnapshotFooterSize(frames.size());
    auto make_trailer_droppped = [&](size_t frame_idx) -> nuraft::ptr<nuraft::buffer>
    {
        // descriptor: type[1] + compressed_offset[8] + compressed_size[8] + node_count[8]
        const size_t cs_offset = footer_offset + KEEPER_CHUNKED_SNAPSHOT_DESCRIPTOR_SIZE * frame_idx + 1 + 8;
        DB::ReadBufferFromNuraftBuffer good_in(good_buf);
        DB::WriteBufferFromNuraftBuffer out;
        copyData(good_in, out, cs_offset);
        uint64_t cs = 0;
        readBinary(cs, good_in);
        cs -= 4; // drop the 4-byte ZSTD content-checksum epilogue
        writeBinary(cs, out);
        copyData(good_in, out);
        out.finalize();
        return out.getBuffer();
    };

    // ── NODES chunk, serial path (deser_threads=1) ──────────────────────────────────────────
    {
        auto bad = make_trailer_droppped(first_nodes_frame_idx);
        auto s = std::make_shared<DB::CoordinationSettings>();
        (*s)[DB::CoordinationSetting::snapshot_deser_threads] = 1;
        auto ctx = std::make_shared<DB::KeeperContext>(true, s);
        ctx->setLocalLogsPreprocessed();
        ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDiskF1T1", snap_dir.path));
        DB::KeeperSnapshotManager mgr(3, ctx, /*compress_snapshots_zstd_=*/true);
        EXPECT_THROW(mgr.deserializeSnapshotFromBuffer(bad, /*load_full_storage=*/true), DB::Exception)
            << "NODES chunk: dropped ZSTD trailer must throw (serial, deser_threads=1)";
    }

    // ── NODES chunk, parallel path (deser_threads=8, K=3 → pool dispatch) ──────────────────
    {
        auto bad = make_trailer_droppped(first_nodes_frame_idx);
        auto s = std::make_shared<DB::CoordinationSettings>();
        (*s)[DB::CoordinationSetting::snapshot_deser_threads] = 8;
        auto ctx = std::make_shared<DB::KeeperContext>(true, s);
        ctx->setLocalLogsPreprocessed();
        ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDiskF1T8", snap_dir.path));
        DB::KeeperSnapshotManager mgr(3, ctx, /*compress_snapshots_zstd_=*/true);
        EXPECT_THROW(mgr.deserializeSnapshotFromBuffer(bad, /*load_full_storage=*/true), DB::Exception)
            << "NODES chunk: dropped ZSTD trailer must throw (parallel, deser_threads=8)";
    }

    // ── METADATA chunk (always read serially regardless of deser_threads) ───────────────────
    {
        auto bad = make_trailer_droppped(0); // chunks[0] is always METADATA
        auto s = std::make_shared<DB::CoordinationSettings>();
        (*s)[DB::CoordinationSetting::snapshot_deser_threads] = 1;
        auto ctx = std::make_shared<DB::KeeperContext>(true, s);
        ctx->setLocalLogsPreprocessed();
        ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDiskF1Meta", snap_dir.path));
        DB::KeeperSnapshotManager mgr(3, ctx, /*compress_snapshots_zstd_=*/true);
        EXPECT_THROW(mgr.deserializeSnapshotFromBuffer(bad, /*load_full_storage=*/true), DB::Exception)
            << "METADATA chunk: dropped ZSTD trailer must throw";
    }
}

/// ── Digest recalculation and parallel load determinism ────────────────────────────────────────

/// Loading a NO_DIGEST snapshot under a digest-enabled context recalculates nodes_digest.
TEST(CoordinationChunkedSnapshotTest, DigestRecalculationOnLoad)
{
    ChangelogDirTest snap_dir("./chunked_digest_recalc");

    // 1. Serialize with digest DISABLED → NO_DIGEST in METADATA chunk.
    auto src_settings = std::make_shared<DB::CoordinationSettings>();
    auto src_ctx = std::make_shared<DB::KeeperContext>(true, src_settings);
    src_ctx->setLocalLogsPreprocessed();
    src_ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDiskND", snap_dir.path));
    src_ctx->setDigestEnabled(false);

    DB::KeeperStorage storage_src(500, "", src_ctx);
    addNode(storage_src, "/alpha", "val_alpha");
    addNode(storage_src, "/beta", "val_beta");
    addNode(storage_src, "/alpha/child", "val_child");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage_src.zxid) = 42;
    storage_src.session_id_counter = 7;

    nuraft::ptr<nuraft::buffer> buf_no_digest;
    {
        DB::KeeperStorageSnapshot snap(
            &storage_src, /*up_to_log_idx=*/42, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);
        DB::KeeperSnapshotManager mgr_nd(3, src_ctx, /*compress_snapshots_zstd_=*/true);
        buf_no_digest = mgr_nd.serializeSnapshotToBuffer(snap);
    }
    ASSERT_NE(buf_no_digest, nullptr);

    // 2. Load with digest ENABLED → triggers recalculate_digest path.
    auto load_settings = std::make_shared<DB::CoordinationSettings>();
    auto load_ctx = std::make_shared<DB::KeeperContext>(true, load_settings);
    load_ctx->setLocalLogsPreprocessed();
    load_ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDiskD", snap_dir.path));
    load_ctx->setDigestEnabled(true);

    DB::KeeperSnapshotManager mgr_load(3, load_ctx, /*compress_snapshots_zstd_=*/true);
    auto res = mgr_load.deserializeSnapshotFromBuffer(buf_no_digest, /*load_full_storage=*/true);
    ASSERT_NE(res.storage, nullptr);

    // 3. Load a second time — must produce the same digest (recalculation is deterministic).
    auto res2 = mgr_load.deserializeSnapshotFromBuffer(buf_no_digest, /*load_full_storage=*/true);
    ASSERT_NE(res2.storage, nullptr);

    EXPECT_NE(res.storage->nodes_digest, 0u) << "Recalculated nodes_digest must be non-zero for a non-empty chunked snapshot";
    EXPECT_EQ(res.storage->nodes_digest, res2.storage->nodes_digest)
        << "Recalculated nodes_digest must be the same across two loads of the same snapshot";
    EXPECT_EQ(res.storage->container.size(), res2.storage->container.size());
}

/// Serial (threads=1) and parallel (threads=8) deserialization of the same snapshot produce identical results.
TEST(CoordinationChunkedSnapshotTest, ParallelVsSequential)
{
    ChangelogDirTest snap_dir("./chunked_parallel_det");

    // Small chunk size → multiple NODES chunks; digest disabled to also exercise recalculate_digest.
    auto src_settings = std::make_shared<DB::CoordinationSettings>();
    (*src_settings)[DB::CoordinationSetting::snapshot_chunk_size] = 4;
    auto src_ctx = std::make_shared<DB::KeeperContext>(true, src_settings);
    src_ctx->setLocalLogsPreprocessed();
    src_ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDiskSrc", snap_dir.path));
    src_ctx->setDigestEnabled(false);

    // Build a storage with enough nodes to span ≥ 2 NODES chunks (chunk=4 → ceil(N/4) chunks).
    DB::KeeperStorage storage_src(500, "", src_ctx);
    addNode(storage_src, "/n1", "data1");
    addNode(storage_src, "/n2", "data2");
    addNode(storage_src, "/n3", "data3");
    addNode(storage_src, "/n4", "data4");
    addNode(storage_src, "/n5", "data5");
    addNode(storage_src, "/n1/c1", "child1");
    addNode(storage_src, "/n2/c2", "child2");
    addNode(storage_src, "/eph", "eph_data", /*ephemeral_owner=*/100);
    storage_src.committed_ephemerals[100].insert("/eph");
    ++storage_src.committed_ephemeral_nodes;
    storage_src.addSessionID(100, 60000);
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage_src.zxid) = 50;
    storage_src.session_id_counter = 200;

    // Serialize once.
    nuraft::ptr<nuraft::buffer> source_buf;
    {
        DB::KeeperStorageSnapshot snap(
            &storage_src, /*up_to_log_idx=*/50, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);
        DB::KeeperSnapshotManager mgr_src(3, src_ctx, /*compress_snapshots_zstd_=*/true);
        source_buf = mgr_src.serializeSnapshotToBuffer(snap);
    }
    ASSERT_NE(source_buf, nullptr);

    // Verify we actually got multiple NODES chunks (otherwise the parallel path is not tested).
    {
        DB::ReadBufferFromNuraftBuffer in15(source_buf);
        auto frames = parseAndValidateChunkedSnapshot(in15);
        size_t nodes_frame_count = 0;
        for (const auto & fd : frames)
            if (fd.type == SnapshotChunkType::NODES)
                ++nodes_frame_count;
        ASSERT_GT(nodes_frame_count, 1u) << "Test requires multiple NODES chunks for parallelism";
    }

    // Load with serial path (threads=1).
    auto settings1 = std::make_shared<DB::CoordinationSettings>();
    (*settings1)[DB::CoordinationSetting::snapshot_deser_threads] = 1;
    auto ctx1 = std::make_shared<DB::KeeperContext>(true, settings1);
    ctx1->setLocalLogsPreprocessed();
    ctx1->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk1", snap_dir.path));
    ctx1->setDigestEnabled(true); // recalculate_digest=true since snapshot has NO_DIGEST
    DB::KeeperSnapshotManager mgr1(3, ctx1, /*compress_snapshots_zstd_=*/true);
    auto res1 = mgr1.deserializeSnapshotFromBuffer(source_buf, /*load_full_storage=*/true);
    ASSERT_NE(res1.storage, nullptr);

    // Load with parallel path (threads=8).
    auto settings8 = std::make_shared<DB::CoordinationSettings>();
    (*settings8)[DB::CoordinationSetting::snapshot_deser_threads] = 8;
    auto ctx8 = std::make_shared<DB::KeeperContext>(true, settings8);
    ctx8->setLocalLogsPreprocessed();
    ctx8->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk8", snap_dir.path));
    ctx8->setDigestEnabled(true); // recalculate_digest=true since snapshot has NO_DIGEST
    DB::KeeperSnapshotManager mgr8(3, ctx8, /*compress_snapshots_zstd_=*/true);
    auto res8 = mgr8.deserializeSnapshotFromBuffer(source_buf, /*load_full_storage=*/true);
    ASSERT_NE(res8.storage, nullptr);

    // Node counts must match.
    EXPECT_EQ(res1.storage->container.size(), res8.storage->container.size())
        << "Serial and parallel loads must produce the same number of nodes";

    // Spot-check key paths for data equality.
    for (const char * path : {"/", "/n1", "/n2", "/n3", "/n4", "/n5", "/n1/c1", "/n2/c2", "/eph"})
    {
        auto it1 = res1.storage->container.find(path);
        auto it8 = res8.storage->container.find(path);
        ASSERT_NE(it1, res1.storage->container.end()) << "Serial missing " << path;
        ASSERT_NE(it8, res8.storage->container.end()) << "Parallel missing " << path;
        EXPECT_EQ(it1->value.getData(), it8->value.getData()) << "Data mismatch at " << path;
    }

    // Digest must be non-zero (recalculated from nodes) and equal across both loads.
    EXPECT_NE(res1.storage->nodes_digest, 0u) << "Recalculated digest must be non-zero";
    EXPECT_EQ(res1.storage->nodes_digest, res8.storage->nodes_digest) << "Serial and parallel digests must match";

    // Ephemeral ownership must be preserved identically.
    EXPECT_EQ(res1.storage->committed_ephemeral_nodes, res8.storage->committed_ephemeral_nodes);
    {
        auto eph1 = res1.storage->committed_ephemerals.find(100);
        auto eph8 = res8.storage->committed_ephemerals.find(100);
        ASSERT_NE(eph1, res1.storage->committed_ephemerals.end()) << "Owner 100 missing in serial";
        ASSERT_NE(eph8, res8.storage->committed_ephemerals.end()) << "Owner 100 missing in parallel";
        EXPECT_EQ(eph1->second, eph8->second) << "Ephemeral path sets must match";
    }
}

/// apply_snapshot with a multi-chunk V9 snapshot replaces committed state and restores
/// ephemerals, ACLs, and digest; uses chunk_size=2 and deser_threads=2 to exercise the parallel path.
TEST(CoordinationChunkedSnapshotTest, ApplyChunkedSnapshotReplacesCommittedState)
{
    ChangelogDirTest snapshots("./chunked_apply_snapshots");
    ChangelogDirTest rocks("./chunked_apply_rocksdb");

    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = makeChunkedApplyStateMachine(snapshots_queue, "./chunked_apply_snapshots");

    auto old_entry = makeCreateEntry(*state_machine, "/old_before_chunked", "stale_data");
    state_machine->pre_commit(1, old_entry->get_buf());
    state_machine->commit(1, old_entry->get_buf());
    ASSERT_TRUE(state_machine->getStorageUnsafe().container.contains("/old_before_chunked"));

    // chunk_size=2 → 6 nodes → 3 NODES chunks; /b_acl exercises the ACL restoration path.
    auto src_ctx = makeChunkedSourceCtx("./chunked_apply_snapshots");

    DB::KeeperStorage snap_storage(500, "", src_ctx);

    // Register an ACL in the source storage so it is serialized in the METADATA chunk.
    // acl_id is deterministically 1 (first registration).
    const DB::ACLId test_acl_id = snap_storage.acl_map.convertACLs({{31, "world", "anyone"}});
    snap_storage.acl_map.addUsage(test_acl_id);
    const Coordination::ACLs expected_acl = {{31, "world", "anyone"}};

    addNode(snap_storage, "/a", "data_a");
    addNode(snap_storage, "/b", "data_b");
    addNode(snap_storage, "/a/c", "data_ac");
    addNode(snap_storage, "/eph", "eph_val", /*ephemeral_owner=*/42);
    addNode(snap_storage, "/b_acl", "acl_data", /*ephemeral_owner=*/0, test_acl_id);
    TSA_SUPPRESS_WARNING_FOR_WRITE(snap_storage.zxid) = 10;
    snap_storage.session_id_counter = 100;
    snap_storage.committed_ephemerals[42].insert("/eph");
    ++snap_storage.committed_ephemeral_nodes;
    snap_storage.addSessionID(42, 30000);

    // Serialize as chunked format using a KeeperSnapshotManager backed by the source context.
    DB::KeeperSnapshotManager src_mgr(3, src_ctx, /*compress_snapshots_zstd_=*/true);
    nuraft::ptr<nuraft::buffer> chunked_buf;
    {
        DB::KeeperStorageSnapshot snap(
            &snap_storage, /*up_to_log_idx=*/10, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);
        chunked_buf = src_mgr.serializeSnapshotToBuffer(snap);
    }
    ASSERT_NE(chunked_buf, nullptr);
    // Verify the buffer is actually a chunked snapshot (front-header layout: CKFS at offset 0).
    ASSERT_GE(chunked_buf->size(), static_cast<size_t>(chunkedSnapshotHeaderSize()));
    const char * cdata = reinterpret_cast<const char *>(chunked_buf->data_begin());
    EXPECT_EQ(std::memcmp(cdata, "CKFS", 4), 0) << "Serialized buffer must start with chunked snapshot magic 'CKFS' (front-header layout)";
    EXPECT_EQ(static_cast<uint8_t>(cdata[4]), 8u) << "Front header version byte must be 8";
    EXPECT_NE(std::memcmp(cdata + chunked_buf->size() - 4, "CKFS", 4), 0)
        << "Serialized buffer must NOT end with CKFS (no trailer in front-header layout)";

    // Install the chunked snapshot into the state machine via the follower path.
    nuraft::snapshot snapshot_meta(10, 0, std::make_shared<nuraft::cluster_config>());
    saveSingleObjectSnapshot(*state_machine, snapshot_meta, chunked_buf);
    EXPECT_TRUE(state_machine->apply_snapshot(snapshot_meta));

    // ── Post-apply assertions ────────────────────────────────────────────────────────────────
    const auto & storage = state_machine->getStorageUnsafe();

    // Committed state replaced: old node gone, new nodes present.
    EXPECT_FALSE(storage.container.contains("/old_before_chunked")) << "Pre-snapshot node must be removed after apply_snapshot";
    ASSERT_TRUE(storage.container.contains("/a"));
    ASSERT_TRUE(storage.container.contains("/b"));
    ASSERT_TRUE(storage.container.contains("/a/c"));
    ASSERT_TRUE(storage.container.contains("/eph"));
    ASSERT_TRUE(storage.container.contains("/b_acl"));
    EXPECT_EQ(std::string(storage.container.getValue("/a").getData()), "data_a");
    EXPECT_EQ(std::string(storage.container.getValue("/b").getData()), "data_b");
    EXPECT_EQ(std::string(storage.container.getValue("/a/c").getData()), "data_ac");
    EXPECT_EQ(std::string(storage.container.getValue("/eph").getData()), "eph_val");
    EXPECT_EQ(std::string(storage.container.getValue("/b_acl").getData()), "acl_data");

    // Children rebuilt: /a has exactly one child (/a/c).
    EXPECT_EQ(storage.container.getValue("/a").getChildren().size(), 1u) << "/a must have exactly one child after chunked apply_snapshot";

    // Ephemeral bookkeeping correct.
    EXPECT_EQ(storage.committed_ephemeral_nodes, 1u) << "committed_ephemeral_nodes must be 1 after chunked apply";
    {
        auto eit = storage.committed_ephemerals.find(42);
        ASSERT_NE(eit, storage.committed_ephemerals.end()) << "Owner 42 must be present in committed_ephemerals";
        EXPECT_EQ(eit->second.count("/eph"), 1u) << "/eph must be tracked as an ephemeral owned by session 42";
    }

    // ACL usage correctly restored: the ACL mapping for test_acl_id must be present
    // in the loaded storage's acl_map (serialized in the METADATA chunk, deserialized
    // by the parallel NODES-chunk path that batches acl_usage into acl_map.addUsageBatch).
    {
        const auto mapping = storage.acl_map.getMapping();
        auto acl_it = std::find_if(
            mapping.begin(), mapping.end(), [&](const auto & entry) { return entry.first == test_acl_id; });
        ASSERT_NE(acl_it, mapping.end()) << "ACL id " << test_acl_id << " must be present in acl_map after chunked apply_snapshot";
        EXPECT_EQ(acl_it->second, expected_acl)
            << "ACL for id " << test_acl_id << " must match the original ACL after chunked apply_snapshot";
        EXPECT_EQ(storage.container.getValue("/b_acl").acl_id, test_acl_id) << "/b_acl must retain its acl_id after chunked apply_snapshot";
    }

    // Digest must be non-zero (recalculated from node data since digestEnabled=true on load).
    EXPECT_NE(storage.nodes_digest, 0u) << "nodes_digest must be non-zero after chunked apply_snapshot with digest enabled";

    // last_commit_index must reflect the snapshot index.
    EXPECT_EQ(state_machine->last_commit_index(), 10u);
}

/// Unreferenced ACLs (usage=0) must be dropped after chunked load, matching the legacy path.
TEST(CoordinationChunkedSnapshotTest, ChunkedLoadDropsUnreferencedACLs)
{
    ChangelogDirTest snapshots("./chunked_drop_acl_snapshots");
    ChangelogDirTest rocks("./chunked_drop_acl_rocksdb");

    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = makeChunkedApplyStateMachine(snapshots_queue, "./chunked_drop_acl_snapshots");

    auto src_ctx = makeChunkedSourceCtx("./chunked_drop_acl_snapshots");

    DB::KeeperStorage source(500, "", src_ctx);

    // Referenced ACL: convertACLs bumps usage → survives removeUnusedACLs.
    const Coordination::ACLs acls_referenced = {{31, "world", "anyone"}};
    const DB::ACLId referenced_id = source.acl_map.convertACLs(acls_referenced);
    addNode(source, "/acl_node", "data", /*ephemeral_owner=*/0, referenced_id);

    // Orphan ACL: injected via addMapping with usage=0 → must be dropped.
    const DB::ACLId orphan_id = 999;
    source.acl_map.addMapping(orphan_id, {{1, "digest", "orphan:pwd"}});

    TSA_SUPPRESS_WARNING_FOR_WRITE(source.zxid) = 5;

    DB::KeeperSnapshotManager src_mgr(3, src_ctx, /*compress_snapshots_zstd_=*/true);
    nuraft::ptr<nuraft::buffer> chunked_buf;
    {
        DB::KeeperStorageSnapshot snap(
            &source, /*up_to_log_idx=*/5, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);
        chunked_buf = src_mgr.serializeSnapshotToBuffer(snap);
    }
    ASSERT_NE(chunked_buf, nullptr);

    nuraft::snapshot snapshot_meta(5, 0, std::make_shared<nuraft::cluster_config>());
    saveSingleObjectSnapshot(*state_machine, snapshot_meta, chunked_buf);
    EXPECT_TRUE(state_machine->apply_snapshot(snapshot_meta));

    const auto & storage = state_machine->getStorageUnsafe();
    const auto mapping = storage.acl_map.getMapping();

    auto ref_it = std::find_if(mapping.begin(), mapping.end(), [&](const auto & entry) { return entry.first == referenced_id; });
    ASSERT_NE(ref_it, mapping.end()) << "Referenced ACL must survive";
    EXPECT_EQ(ref_it->second, acls_referenced);
    EXPECT_EQ(storage.container.getValue("/acl_node").acl_id, referenced_id);

    EXPECT_TRUE(std::none_of(mapping.begin(), mapping.end(), [](const auto & entry) { return entry.first == 999; }))
        << "Orphan ACL (usage=0) must be dropped";
}

// ─── Chunked snapshot detection tests ───────────────────────────────────────────────────────────

/// A V9 snapshot is detected by its CKFS front magic and round-trips correctly.
TEST(CoordinationChunkedSnapshotTest, ChunkedDetectedViaFrontMagic)
{
    ChangelogDirTest snap_dir("./chunked_det_front");

    auto keeper_context = makeKeeperContext(snap_dir.path);

    DB::KeeperStorage storage(500, "", keeper_context);
    addNode(storage, "/det1", "v1");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 5;

    DB::KeeperStorageSnapshot snap(
        &storage, /*up_to_log_idx=*/5, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);
    DB::KeeperSnapshotManager mgr(3, keeper_context, /*compress_snapshots_zstd_=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    const char * data = reinterpret_cast<const char *>(buf->data_begin());
    const size_t size = buf->size();

    // First 4 bytes must be CKFS (front header magic); version byte at [4] must be 8.
    ASSERT_GE(size, static_cast<size_t>(chunkedSnapshotHeaderSize()));
    EXPECT_EQ(memcmp(data, "CKFS", 4), 0) << "CKFS magic must be at front (header)";
    EXPECT_EQ(static_cast<uint8_t>(data[4]), 8u) << "Front header version byte must be 8";
    // Last 4 bytes must NOT be CKFS (no trailer in front-header layout).
    EXPECT_NE(memcmp(data + size - 4, "CKFS", 4), 0) << "Last bytes must NOT be CKFS (no trailer)";

    // Full round-trip.
    auto result = mgr.deserializeSnapshotFromBuffer(buf, /*load_full_storage=*/true);
    ASSERT_NE(result.storage, nullptr);
    EXPECT_NE(result.storage->container.find("/det1"), result.storage->container.end());

    // Metadata-only fast path (O(1) in node count).
    auto meta = mgr.deserializeSnapshotMetadataFromBuffer(buf);
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->get_last_log_idx(), 5u);
}

/// A V7 ZSTD snapshot is not misdetected as chunked.
TEST(CoordinationChunkedSnapshotTest, LegacyZstdNotMisdetectedAsChunked)
{
    ChangelogDirTest snap_dir("./chunked_det_legv7");

    auto keeper_context = makeKeeperContext(snap_dir.path);

    DB::KeeperStorage storage(500, "", keeper_context);
    addNode(storage, "/legacy", "old_data");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 3;

    DB::KeeperStorageSnapshot snap(
        &storage, /*up_to_log_idx=*/3, /*cluster_config=*/nullptr, DB::SnapshotVersion::V7);
    DB::KeeperSnapshotManager mgr(3, keeper_context, /*compress_snapshots_zstd_=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    const char * data = reinterpret_cast<const char *>(buf->data_begin());
    const size_t size = buf->size();

    // A V7 snapshot must not have CKFS as its front magic (it's a ZSTD frame).
    ASSERT_GE(size, 4u);
    EXPECT_NE(memcmp(data, "CKFS", 4), 0) << "V7 snapshot must NOT have CKFS at front";
    DB::ReadBufferFromNuraftBuffer in14(buf);
    EXPECT_FALSE(isChunkedSnapshot(in14)) << "V7 snapshot must not be detected as chunked";

    // Must still load successfully via the legacy path.
    auto result = mgr.deserializeSnapshotFromBuffer(buf, /*load_full_storage=*/true);
    ASSERT_NE(result.storage, nullptr);
    EXPECT_NE(result.storage->container.find("/legacy"), result.storage->container.end());
}

/// FrontMagicWithBadStructureNotChunked: prove the predicate is NOT magic-only and NOT magic+count-only.
/// A buffer starting with CKFS but with an invalid descriptor table must not be detected as chunked.
TEST(CoordinationChunkedSnapshotTest, FrontMagicWithBadStructureNotChunked)
{
    // (a) plausible magic but chunk_count < MIN (2 < 3)
    {
        WriteBufferFromOwnString out;
        packChunkedSnapshotHeader(2, out);
        out.write(std::string(128 - chunkedSnapshotHeaderSize(), '\0').data(), 128 - chunkedSnapshotHeaderSize());
        out.finalize();
        auto b = out.str();
        DB::ReadBufferFromString in13(b);
        EXPECT_FALSE(isChunkedSnapshot(in13)) << "(a) chunk_count=2 < MIN must return false";
    }
    // (b) chunk_count so large the footer cannot fit
    {
        WriteBufferFromOwnString out;
        packChunkedSnapshotHeader(1ull << 40, out);
        out.write(std::string(128 - chunkedSnapshotHeaderSize(), '\0').data(), 128 - chunkedSnapshotHeaderSize());
        out.finalize();
        auto b = out.str();
        DB::ReadBufferFromString in12(b);
        EXPECT_FALSE(isChunkedSnapshot(in12)) << "(b) huge chunk_count footer-doesn't-fit must return false";
    }
    // (c) THE KEY CASE: plausible chunk_count (3) + footer fits, but the descriptor table is invalid.
    //     buf_size = 13 + 3*25 = 88 → footer_offset = 13; the all-zero "descriptors" have
    //     offset 0 < 13 (out of bounds) and a wrong chunk order → must be rejected.
    {
        const uint64_t cc3 = 3;
        WriteBufferFromOwnString out;
        packChunkedSnapshotHeader(cc3, out);
        // No chunk region (footer_offset = 13); all-zero descriptors have compressed_offset=0 < 13 → ChunkOutOfBounds
        const std::vector<SnapshotChunkDescriptor> zero_descs(static_cast<size_t>(cc3));
        packChunkedSnapshotFooter(zero_descs, out);
        out.finalize();
        auto c = out.str();
        DB::ReadBufferFromString in11(c);
        EXPECT_FALSE(isChunkedSnapshot(in11))
            << "(c) all-zero descriptors with out-of-bounds offsets must return false (full structural sniff)";
    }
}

/// UnsupportedVersionRoutedToParser: a structurally valid front-header buffer with version=9
/// is detected (version-independent sniff) and routed to the parser which throws UNKNOWN_FORMAT_VERSION.
TEST(CoordinationChunkedSnapshotTest, UnsupportedVersionRoutedToParser)
{
    ChangelogDirTest snap_dir("./chunked_det_v9router");

    auto keeper_context = makeKeeperContext(snap_dir.path);
    DB::KeeperSnapshotManager mgr(3, keeper_context, /*compress_snapshots_zstd_=*/true);

    // Build a structurally valid front-header buffer, then flip only the version byte to 9.
    std::vector<SnapshotChunkDescriptor> frames = {
        {SnapshotChunkType::METADATA, 13, 10},
        {SnapshotChunkType::NODES, 23, 10},
    };
    auto raw = buildChunkedBufferFromDescriptors(frames);
    raw[4] = 9; // unsupported version; magic + footer structure intact

    // isChunkedSnapshot is version-independent → structural sniff passes.
    DB::ReadBufferFromString in10(raw);
    EXPECT_TRUE(isChunkedSnapshot(in10)) << "Version-9 buffer with valid structure must return true from isChunkedSnapshot";

    // Wrap in nuraft buffer.
    DB::WriteBufferFromNuraftBuffer buf_out;
    buf_out.write(raw.data(), raw.size());
    buf_out.finalize();
    auto buf = buf_out.getBuffer();

    auto expect_unknown_version = [](auto && fn, const char * what)
    {
        try
        {
            fn();
            FAIL() << what << " must throw on unsupported chunked version";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::UNKNOWN_FORMAT_VERSION) << what << " expected UNKNOWN_FORMAT_VERSION, got: " << e.message();
        }
    };
    expect_unknown_version([&] { mgr.deserializeSnapshotFromBuffer(buf, true); }, "deserializeSnapshotFromBuffer");
    expect_unknown_version([&] { mgr.deserializeSnapshotMetadataFromBuffer(buf); }, "deserializeSnapshotMetadataFromBuffer");
}

/// TooSmallBufferNotChunked: buffers smaller than the header size must return false from
/// isChunkedSnapshot, and parseAndValidateChunkedSnapshot must throw.
TEST(CoordinationChunkedSnapshotTest, TooSmallBufferNotChunked)
{
    for (size_t n = 0; n < chunkedSnapshotHeaderSize(); ++n)
    {
        std::string buf(n, '\0');
        DB::ReadBufferFromString in9(buf);
        EXPECT_FALSE(isChunkedSnapshot(in9)) << "Buffer of size " << n << " must not look like chunked snapshot";
    }
    // parseAndValidateChunkedSnapshot must also throw on too-small input.
    std::string small(chunkedSnapshotHeaderSize() - 1, '\0');
    {
        DB::ReadBufferFromString in8(small);
        EXPECT_THROW(parseAndValidateChunkedSnapshot(in8), DB::Exception);
    }
}

/// CorruptHeaderRejectedByParser: valid V9 buffer with corrupt front magic → parser throws;
/// separately, huge chunk_count (footer doesn't fit) → parser throws.
TEST(CoordinationChunkedSnapshotTest, CorruptHeaderRejectedByParser)
{
    ChangelogDirTest snap_dir("./chunked_det_corr_hdr");

    auto keeper_context = makeKeeperContext(snap_dir.path);

    DB::KeeperStorage storage(500, "", keeper_context);
    addNode(storage, "/ctr", "v1");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 2;

    DB::KeeperStorageSnapshot snap(
        &storage, /*up_to_log_idx=*/2, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);
    DB::KeeperSnapshotManager mgr(3, keeper_context, /*compress_snapshots_zstd_=*/true);
    auto good_buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(good_buf, nullptr);

    // Variant A: corrupt front magic byte → parser throws CORRUPTED_DATA.
    {
        DB::WriteBufferFromNuraftBuffer out;
        DB::ReadBufferFromNuraftBuffer good_in(good_buf);
        copyData(good_in, out);
        out.finalize();
        auto bad = out.getBuffer();
        reinterpret_cast<char *>(bad->data_begin())[0] ^= 0xFF; // corrupt magic[0]
        {
            DB::ReadBufferFromNuraftBuffer in7(bad);
            EXPECT_THROW(parseAndValidateChunkedSnapshot(in7), DB::Exception);
        }
    }

    // Variant B: corrupt chunk_count to a huge value (footer doesn't fit) → parser throws.
    {
        DB::WriteBufferFromNuraftBuffer out;
        DB::ReadBufferFromNuraftBuffer good_in(good_buf);
        copyData(good_in, out);
        out.finalize();
        auto bad = out.getBuffer();
        uint64_t huge_count = 0xFFFFFFFFFFFFFFFFULL;
        std::memcpy(reinterpret_cast<char *>(bad->data_begin()) + 5, &huge_count, 8);
        {
            DB::ReadBufferFromNuraftBuffer in6(bad);
            EXPECT_THROW(parseAndValidateChunkedSnapshot(in6), DB::Exception);
        }
    }
}

/// CorruptOrTruncatedChunkedRejectedByPublicApi: a chunked snapshot with a corrupted front magic
/// or truncated footer must be rejected through both public APIs.
TEST(CoordinationChunkedSnapshotTest, CorruptOrTruncatedChunkedRejectedByPublicApi)
{
    ChangelogDirTest snap_dir("./chunked_det_corr_or_trunc");

    auto keeper_context = makeKeeperContext(snap_dir.path);

    DB::KeeperStorage storage(500, "", keeper_context);
    addNode(storage, "/ctm1", "v1");
    addNode(storage, "/ctm2", "v2");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 7;

    DB::KeeperStorageSnapshot snap(
        &storage, /*up_to_log_idx=*/7, /*cluster_config=*/nullptr, DB::SnapshotVersion::V9);
    DB::KeeperSnapshotManager mgr(3, keeper_context, /*compress_snapshots_zstd_=*/true);
    auto good_buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(good_buf, nullptr);

    // Get the frame count so we can compute the footer size.
    DB::ReadBufferFromNuraftBuffer in5(good_buf);
    auto frames = parseAndValidateChunkedSnapshot(in5);
    const size_t footer_sz = chunkedSnapshotFooterSize(frames.size());

    // Variant A: corrupt the front magic byte — isChunkedSnapshot=false → legacy reader.
    // The first bytes are now corrupt, so the legacy reader (ZSTD/LZ4) cannot decode them either.
    // Both APIs must throw DB::Exception.
    {
        DB::WriteBufferFromNuraftBuffer out;
        DB::ReadBufferFromNuraftBuffer good_in(good_buf);
        copyData(good_in, out);
        out.finalize();
        auto bad = out.getBuffer();
        reinterpret_cast<char *>(bad->data_begin())[0] ^= 0xFF;
        EXPECT_THROW(mgr.deserializeSnapshotFromBuffer(bad, true), DB::Exception)
            << "Corrupt-magic must throw through deserializeSnapshotFromBuffer";
        EXPECT_THROW(mgr.deserializeSnapshotMetadataFromBuffer(bad), DB::Exception)
            << "Corrupt-magic must throw through deserializeSnapshotMetadataFromBuffer";
    }

    // Variant B: truncate the buffer by dropping the trailing footer.
    // Front magic + chunk_count are intact, but the footer can no longer be located at
    // buf_size - footer_size (it's now in the middle of chunk data). The structural sniff
    // finds invalid descriptors there → isChunkedSnapshot=false → legacy reader rejects.
    {
        const size_t truncated_size = good_buf->size() - footer_sz;
        DB::WriteBufferFromNuraftBuffer out;
        DB::ReadBufferFromNuraftBuffer good_in(good_buf);
        copyData(good_in, out, truncated_size);
        out.finalize();
        auto bad = out.getBuffer();
        EXPECT_THROW(mgr.deserializeSnapshotFromBuffer(bad, true), DB::Exception)
            << "Footer-truncated must throw through deserializeSnapshotFromBuffer";
        EXPECT_THROW(mgr.deserializeSnapshotMetadataFromBuffer(bad), DB::Exception)
            << "Footer-truncated must throw through deserializeSnapshotMetadataFromBuffer";
    }
}

/// RejectsTrailingGapBeforeFooter: a buffer where the last chunk ends before footer_offset
/// (trailing gap) must be rejected by both the parser and the detector.
TEST(CoordinationChunkedSnapshotTest, RejectsTrailingGapBeforeFooter)
{
    // Build a valid 2-chunk structure with a deliberate 7-byte gap between the last chunk and footer.
    // last chunk ends at 33, but force the footer to start at 40 (7-byte trailing gap).
    std::vector<SnapshotChunkDescriptor> frames = {
        {SnapshotChunkType::METADATA, 13, 10},
        {SnapshotChunkType::NODES, 23, 10},
    };
    // Manual construction: total size = 40 + 50 (footer for 2 chunks) = 90
    const uint64_t footer_offset = 40; // 7-byte gap after last chunk (ends at 33)
    WriteBufferFromOwnString out;
    packChunkedSnapshotHeader(static_cast<uint64_t>(frames.size()), out);
    // Chunk data region + trailing gap: footer_offset - header_size = 27 bytes of zeros
    out.write(std::string(static_cast<size_t>(footer_offset) - chunkedSnapshotHeaderSize(), '\0').data(),
              static_cast<size_t>(footer_offset) - chunkedSnapshotHeaderSize());
    packChunkedSnapshotFooter(frames, out);
    out.finalize();
    auto buf = out.str();
    // The front header says chunk_count=2, footer_offset derived as total-50=40.
    // The last chunk ends at 33 != 40 → TrailingGap.
    {
        DB::ReadBufferFromString in4(buf);
        EXPECT_THROW(parseAndValidateChunkedSnapshot(in4), DB::Exception)
            << "Trailing gap before footer must throw CORRUPTED_DATA from parseAndValidateChunkedSnapshot";
    }

    DB::ReadBufferFromString in3(buf);
    EXPECT_FALSE(isChunkedSnapshot(in3)) << "Trailing gap before footer must return false from isChunkedSnapshot (detector also rejects)";
}

/// LegacyNonZstdSnapshotStillLoads: a legacy snapshot serialized with compress_snapshots_zstd_=false
/// (CompressedWriteBuffer/CompressedReadBuffer path, CityHash front) round-trips correctly under
/// the new front-header detection. Confirms the non-ZSTD legacy path is unaffected.
TEST(CoordinationChunkedSnapshotTest, LegacyNonZstdSnapshotStillLoads)
{
    ChangelogDirTest snap_dir("./chunked_det_legacy_nonzstd");

    auto keeper_context = makeKeeperContext(snap_dir.path);

    DB::KeeperStorage storage(500, "", keeper_context);
    addNode(storage, "/lnz1", "data1");
    addNode(storage, "/lnz2", "data2");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 4;

    // V7 with compress_snapshots_zstd_=false → CompressedWriteBuffer → CityHash128 front bytes.
    DB::KeeperStorageSnapshot snap(
        &storage, /*up_to_log_idx=*/4, /*cluster_config=*/nullptr, DB::SnapshotVersion::V7);
    DB::KeeperSnapshotManager mgr(3, keeper_context, /*compress_snapshots_zstd_=*/false);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // The legacy non-ZSTD front is a CityHash128 checksum — must NOT start with CKFS.
    DB::ReadBufferFromNuraftBuffer in2(buf);
    EXPECT_FALSE(isChunkedSnapshot(in2)) << "Non-ZSTD legacy snapshot must not be detected as chunked";

    // Must round-trip successfully via the legacy LZ4/CompressedReadBuffer path.
    auto result = mgr.deserializeSnapshotFromBuffer(buf, /*load_full_storage=*/true);
    ASSERT_NE(result.storage, nullptr);
    EXPECT_NE(result.storage->container.find("/lnz1"), result.storage->container.end());
    EXPECT_NE(result.storage->container.find("/lnz2"), result.storage->container.end());
}

/// A legacy-framed stream with inner version byte 8 is rejected with UNKNOWN_FORMAT_VERSION
/// by both public APIs.
TEST(CoordinationChunkedSnapshotTest, LegacyInnerVersion8RejectedByLegacyReader)
{
    ChangelogDirTest snap_dir("./chunked_det_leg_v8guard");

    auto keeper_context = makeKeeperContext(snap_dir.path);

    // Build a legacy CompressedWriteBuffer-framed stream whose decompressed first byte is 8.
    // This simulates a corrupt/crafted stream that happens to decompress to version byte 8
    // but was NOT detected as a chunked snapshot (front bytes != CKFS or structural sniff fails).
    auto writer = std::make_unique<DB::WriteBufferFromNuraftBuffer>();
    auto * raw_ptr = writer.get();
    {
        DB::CompressedWriteBuffer compressed(*writer); // legacy framing; front = CityHash checksum
        DB::writeBinary(static_cast<uint8_t>(8), compressed); // inner version byte = 8
        DB::writeBinary(static_cast<uint64_t>(0), compressed); // a little extra body
        compressed.finalize();
    }
    writer->finalize();
    auto buf = raw_ptr->getBuffer();
    ASSERT_NE(buf, nullptr);

    // Must NOT be detected as a chunked snapshot (CityHash front != CKFS or structural sniff fails).
    DB::ReadBufferFromNuraftBuffer in1(buf);
    EXPECT_FALSE(isChunkedSnapshot(in1)) << "Legacy CompressedWriteBuffer stream must not be detected as chunked";

    DB::KeeperSnapshotManager mgr(3, keeper_context, /*compress_snapshots_zstd_=*/false);

    auto expect_unknown_version = [](auto && fn, const char * what)
    {
        try
        {
            fn();
            FAIL() << what << " must throw UNKNOWN_FORMAT_VERSION for inner version 8 in legacy reader";
        }
        catch (const DB::Exception & e)
        {
            EXPECT_EQ(e.code(), DB::ErrorCodes::UNKNOWN_FORMAT_VERSION) << what << " expected UNKNOWN_FORMAT_VERSION, got: " << e.message();
        }
    };
    expect_unknown_version([&] { mgr.deserializeSnapshotFromBuffer(buf, true); }, "deserializeSnapshotFromBuffer");
    expect_unknown_version([&] { mgr.deserializeSnapshotMetadataFromBuffer(buf); }, "deserializeSnapshotMetadataFromBuffer");
}

} // namespace DB

#endif
