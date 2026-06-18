#include "config.h"

#if USE_NURAFT
#include <Coordination/tests/gtest_coordination_common.h>

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

#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileDecorator.h>

#include <Poco/Util/MapConfiguration.h>

#include <base/scope_guard.h>

#include <zstd.h>

#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <fstream>
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

class ThrowingSnapshotDisk : public DB::DiskLocal
{
public:
    ThrowingSnapshotDisk(
        const std::string & disk_name,
        const std::string & disk_path,
        std::string fail_path_,
        SnapshotDiskFailureMode failure_mode_)
        : DB::DiskLocal(disk_name, disk_path)
        , fail_path(std::move(fail_path_))
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

    std::unique_ptr<DB::WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size,
        DB::WriteMode mode,
        const DB::WriteSettings & settings) override
    {
        auto inner = DB::DiskLocal::writeFile(path, buf_size, mode, settings);

        if (failure_enabled && path == fail_path && failure_mode == SnapshotDiskFailureMode::OpenFileAfterCreate)
            throw std::runtime_error("Injected snapshot open failure");

        if (failure_enabled
            && path == fail_path
            && (failure_mode == SnapshotDiskFailureMode::SyncFile
                || failure_mode == SnapshotDiskFailureMode::SyncFileAndCleanupDataFileRemoveFailure))
            return std::make_unique<ThrowingSnapshotWriteBuffer>(std::move(inner));

        return inner;
    }

    void removeFile(const String & path) override
    {
        if (failure_enabled && path == fail_path && failure_mode == SnapshotDiskFailureMode::RemoveFileOnce && !remove_failed)
        {
            remove_failed = true;
            throw std::runtime_error("Injected snapshot remove failure");
        }

        DB::DiskLocal::removeFile(path);
    }

    void removeFileIfExists(const String & path) override
    {
        if (failure_enabled
            && path == fail_path
            && failure_mode == SnapshotDiskFailureMode::SyncFileAndCleanupDataFileRemoveFailure
            && !remove_if_exists_failed)
        {
            remove_if_exists_failed = true;
            throw std::runtime_error("Injected snapshot remove-if-exists failure");
        }

        DB::DiskLocal::removeFileIfExists(path);
    }

private:
    std::string fail_path;
    SnapshotDiskFailureMode failure_mode;
    bool failure_enabled = true;
    bool remove_failed = false;
    bool remove_if_exists_failed = false;
};

template <typename Manager>
void assertNoSnapshotArtifactsAndNoRegistration(
    Manager & manager,
    const std::string & snapshot_path,
    const std::string & tmp_snapshot_path)
{
    EXPECT_FALSE(fs::exists(snapshot_path));
    EXPECT_FALSE(fs::exists(tmp_snapshot_path));
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
    using Node = DB::KeeperMemoryStorage::Node;
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

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    DB::KeeperSnapshotManager<Storage> manager(3, this->keeper_context, this->enable_compression);

    Storage storage(500, "", this->keeper_context);

    /// Set ACLs on nodes to verify acl_id round-trips through V7 snapshots
    auto acl_id1 = storage.acl_map.convertACLs({{31, "world", "anyone"}});
    auto acl_id2 = storage.acl_map.convertACLs({{1, "digest", "user1:pwd"}});
    storage.acl_map.addUsage(acl_id1);
    storage.acl_map.addUsage(acl_id2);

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

    DB::KeeperStorageSnapshot<Storage> snapshot(&storage, 2, nullptr, DB::SnapshotVersion::V7);

    EXPECT_EQ(snapshot.snapshot_meta->get_last_log_idx(), 2);
    EXPECT_EQ(snapshot.session_id, 7);
    EXPECT_EQ(snapshot.snapshot_container_size, 6);
    EXPECT_EQ(snapshot.session_and_timeout.size(), 2);

    auto buf = manager.serializeSnapshotToBuffer(snapshot);
    manager.serializeSnapshotBufferToDisk(*buf, 2);
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_2.bin" + this->extension));

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
    if constexpr (!TestFixture::Storage::use_rocksdb)
        EXPECT_EQ(restored_storage->container.find("/")->value.stats.seqNum(), large_seq_num);
}

TYPED_TEST(CoordinationTest, TestStorageSnapshotMoreWrites)
{

    ChangelogDirTest test("./snapshots");
    this->setSnapshotDirectory("./snapshots");

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    DB::KeeperSnapshotManager<Storage> manager(3, this->keeper_context, this->enable_compression);

    Storage storage(500, "", this->keeper_context);
    storage.getSessionID(130);

    for (size_t i = 0; i < 50; ++i)
    {
        addNode(storage, "/hello_" + std::to_string(i), "world_" + std::to_string(i));
    }

    DB::KeeperStorageSnapshot<Storage> snapshot(&storage, 50, nullptr, this->keeper_context->getWriteSnapshotVersion());
    EXPECT_EQ(snapshot.snapshot_meta->get_last_log_idx(), 50);
    EXPECT_EQ(snapshot.snapshot_container_size, 54);

    for (size_t i = 50; i < 100; ++i)
    {
        addNode(storage, "/hello_" + std::to_string(i), "world_" + std::to_string(i));
    }

    EXPECT_EQ(storage.container.size(), 104);

    auto buf = manager.serializeSnapshotToBuffer(snapshot);
    manager.serializeSnapshotBufferToDisk(*buf, 50);
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_50.bin" + this->extension));

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

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    DB::KeeperSnapshotManager<Storage> manager(3, this->keeper_context, this->enable_compression);

    Storage storage(500, "", this->keeper_context);
    storage.getSessionID(130);

    for (size_t j = 1; j <= 5; ++j)
    {
        for (size_t i = (j - 1) * 50; i < j * 50; ++i)
        {
            addNode(storage, "/hello_" + std::to_string(i), "world_" + std::to_string(i));
        }

        DB::KeeperStorageSnapshot<Storage> snapshot(&storage, j * 50, nullptr, this->keeper_context->getWriteSnapshotVersion());
        auto buf = manager.serializeSnapshotToBuffer(snapshot);
        manager.serializeSnapshotBufferToDisk(*buf, j * 50);
        EXPECT_TRUE(fs::exists(std::string{"./snapshots/snapshot_"} + std::to_string(j * 50) + ".bin" + this->extension));
    }

    EXPECT_FALSE(fs::exists("./snapshots/snapshot_50.bin" + this->extension));
    EXPECT_FALSE(fs::exists("./snapshots/snapshot_100.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_150.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_200.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_250.bin" + this->extension));


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

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    DB::KeeperSnapshotManager<Storage> manager(3, this->keeper_context, this->enable_compression);
    Storage storage(500, "", this->keeper_context);

    for (size_t i = 0; i < 50; ++i)
    {
        addNode(storage, fmt::format("/hello_{}", i), fmt::format("world_{}", i));
    }
    {
        DB::KeeperStorageSnapshot<Storage> snapshot(&storage, 50, nullptr, this->keeper_context->getWriteSnapshotVersion());
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
        if constexpr (Storage::use_rocksdb)
            EXPECT_EQ(storage.container.snapshotSizeWithVersion().first, 54);
        else
            EXPECT_EQ(storage.container.snapshotSizeWithVersion().first, 104);
        EXPECT_EQ(storage.container.snapshotSizeWithVersion().second, 1);
        auto buf = manager.serializeSnapshotToBuffer(snapshot);
        manager.serializeSnapshotBufferToDisk(*buf, 50);
    }
    EXPECT_TRUE(fs::exists(fmt::format("./snapshots/snapshot_50.bin{}", this->extension)));
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

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    DB::KeeperSnapshotManager<Storage> manager(3, this->keeper_context, this->enable_compression);
    Storage storage(500, "", this->keeper_context);
    for (size_t i = 0; i < 50; ++i)
    {
        addNode(storage, "/hello_" + std::to_string(i), "world_" + std::to_string(i));
    }
    {
        DB::KeeperStorageSnapshot<Storage> snapshot(&storage, 50, nullptr, this->keeper_context->getWriteSnapshotVersion());
        auto buf = manager.serializeSnapshotToBuffer(snapshot);
        manager.serializeSnapshotBufferToDisk(*buf, 50);
    }
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_50.bin" + this->extension));

    /// Let's corrupt file
    DB::WriteBufferFromFile plain_buf(
        "./snapshots/snapshot_50.bin" + this->extension, DB::DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY);
    plain_buf.truncate(34);
    plain_buf.finalize();

    EXPECT_THROW(manager.restoreFromLatestSnapshot(), DB::Exception);
}

TYPED_TEST(CoordinationTest, TestStorageSnapshotDifferentCompressions)
{
    ChangelogDirTest test("./snapshots");
    this->setSnapshotDirectory("./snapshots");

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    DB::KeeperSnapshotManager<Storage> manager(3, this->keeper_context, this->enable_compression);

    Storage storage(500, "", this->keeper_context);
    addNode(storage, "/hello1", "world", 1);
    addNode(storage, "/hello2", "somedata", 3);
    storage.session_id_counter = 5;
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 2;
    storage.committed_ephemerals[3] = {"/hello2"};
    storage.committed_ephemerals[1] = {"/hello1"};
    storage.getSessionID(130);
    storage.getSessionID(130);

    DB::KeeperStorageSnapshot<Storage> snapshot(&storage, 2, nullptr, this->keeper_context->getWriteSnapshotVersion());

    auto buf = manager.serializeSnapshotToBuffer(snapshot);
    manager.serializeSnapshotBufferToDisk(*buf, 2);
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_2.bin" + this->extension));

    DB::KeeperSnapshotManager<Storage> new_manager(3, this->keeper_context, !this->enable_compression);

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

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    std::optional<UInt128> snapshot_hash;
    for (size_t i = 0; i < 15; ++i)
    {
        DB::KeeperSnapshotManager<Storage> manager(3, this->keeper_context, this->enable_compression);

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

        DB::KeeperStorageSnapshot<Storage> snapshot(&storage, storage.getZXID(), nullptr, this->keeper_context->getWriteSnapshotVersion());

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

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    DB::KeeperSnapshotManager<Storage> manager(3, this->keeper_context, this->enable_compression);

    Storage storage(500, "", this->keeper_context);
    static constexpr std::string_view path = "/hello";
    static constexpr DB::ACLId acl_id = 42;
    addNode(storage, std::string{path}, "world", /*ephemeral_owner=*/0, acl_id);
    DB::KeeperStorageSnapshot<Storage> snapshot(&storage, 50, nullptr, this->keeper_context->getWriteSnapshotVersion());
    auto buf = manager.serializeSnapshotToBuffer(snapshot);
    manager.serializeSnapshotBufferToDisk(*buf, 50);

    EXPECT_TRUE(fs::exists("./snapshots/snapshot_50.bin" + this->extension));
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

template <typename Storage>
static DB::KeeperContextPtr makeFollowerContext(int idx)
{
    auto settings = std::make_shared<DB::CoordinationSettings>();
#if USE_ROCKSDB
    (*settings)[DB::CoordinationSetting::experimental_use_rocksdb] = std::is_same_v<Storage, DB::KeeperRocksStorage>;
#else
    (*settings)[DB::CoordinationSetting::experimental_use_rocksdb] = 0;
#endif
    auto ctx = std::make_shared<DB::KeeperContext>(true, settings);
    ctx->setLocalLogsPreprocessed();
    ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>(
        fmt::format("SnapshotDisk_{}", idx), fmt::format("./snapshots_{}", idx)));
    ctx->setRocksDBDisk(std::make_shared<DB::DiskLocal>(
        fmt::format("RocksDisk_{}", idx), fmt::format("./rocksdb_{}", idx)));
    ctx->setRocksDBOptions();
    return ctx;
}

template <typename Storage>
static std::string runFollower(int idx, DB::IKeeperStateMachine & leader, nuraft::snapshot & s)
{
    fs::create_directory(fmt::format("./snapshots_{}", idx));
    fs::create_directory(fmt::format("./rocksdb_{}", idx));
    SCOPE_EXIT({
        fs::remove_all(fmt::format("./snapshots_{}", idx));
        fs::remove_all(fmt::format("./rocksdb_{}", idx));
    });

    auto ctx = makeFollowerContext<Storage>(idx);
    DB::SnapshotsQueue snapshots_queue{1};
    auto follower = std::make_shared<DB::KeeperStateMachine<Storage>>(nullptr, snapshots_queue, ctx, nullptr);
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

static DB::KeeperContextPtr makeMemoryContextForSnapshotApply(const std::string & snapshots_path, const std::string & rocksdb_path)
{
    auto settings = std::make_shared<DB::CoordinationSettings>();
#if USE_ROCKSDB
    (*settings)[DB::CoordinationSetting::experimental_use_rocksdb] = false;
#endif
    (*settings)[DB::CoordinationSetting::compress_snapshots_with_zstd_format] = true;
    auto ctx = std::make_shared<DB::KeeperContext>(true, settings);
    ctx->setLocalLogsPreprocessed();
    ctx->setDigestEnabled(true);
    ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapshotDisk", snapshots_path));
    ctx->setRocksDBDisk(std::make_shared<DB::DiskLocal>("RocksDisk", rocksdb_path));
    ctx->setRocksDBOptions();
    return ctx;
}

static LogEntryPtr makeCreateEntry(
    DB::KeeperStateMachine<DB::KeeperMemoryStorage> & state_machine,
    const std::string & path,
    const std::string & data)
{
    auto request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
    request->path = path;
    request->data = data;
    return getLogEntryFromZKRequest(0, 1, state_machine.getNextZxid(), request);
}

static LogEntryPtr makeSetEntry(
    DB::KeeperStateMachine<DB::KeeperMemoryStorage> & state_machine,
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
    DB::KeeperStateMachine<DB::KeeperMemoryStorage> & state_machine,
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

static LogEntryPtr makeCloseEntry(DB::KeeperStateMachine<DB::KeeperMemoryStorage> & state_machine, int64_t session_id)
{
    auto request = std::make_shared<Coordination::ZooKeeperCloseRequest>();
    return getLogEntryFromZKRequest(0, session_id, state_machine.getNextZxid(), request);
}

static nuraft::ptr<nuraft::buffer> makeSnapshotBufferFromStorage(
    DB::KeeperMemoryStorage & storage,
    uint64_t last_log_idx,
    const DB::KeeperContextPtr & keeper_context)
{
    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> manager(3, keeper_context, true);
    DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snapshot(
        &storage, last_log_idx, nullptr, keeper_context->getWriteSnapshotVersion());
    return manager.serializeSnapshotToBuffer(snapshot);
}

static void saveSingleObjectSnapshot(
    DB::KeeperStateMachine<DB::KeeperMemoryStorage> & state_machine,
    nuraft::snapshot & snapshot,
    nuraft::ptr<nuraft::buffer> snapshot_buf)
{
    uint64_t obj_id = 0;
    state_machine.save_logical_snp_obj(snapshot, obj_id, *snapshot_buf, /*is_first_obj=*/true, /*is_last_obj=*/true);
    ASSERT_EQ(obj_id, 1);
}

/// Drain a queued create_snapshot task synchronously (the snapshot thread's job in production)
/// and return the resulting file info, mirroring the pattern used by the cleanup tests.
static DB::SnapshotFileInfoPtr executeCreateSnapshotTask(
    DB::KeeperStateMachine<DB::KeeperMemoryStorage> & state_machine,
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
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine<DB::KeeperMemoryStorage>>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    auto old_entry = makeCreateEntry(*state_machine, "/old", "old");
    state_machine->pre_commit(1, old_entry->get_buf());
    state_machine->commit(1, old_entry->get_buf());

    DB::KeeperMemoryStorage snapshot_storage(500, "", ctx);
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
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine<DB::KeeperMemoryStorage>>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    auto base_entry = makeCreateEntry(*state_machine, "/committed", "base");
    state_machine->pre_commit(1, base_entry->get_buf());
    state_machine->commit(1, base_entry->get_buf());

    auto set_entry = makeSetEntry(*state_machine, "/committed", "tail_update");
    state_machine->pre_commit(2, set_entry->get_buf());

    auto create_tail_entry = makeCreateEntry(*state_machine, "/tail", "tail_create");
    state_machine->pre_commit(3, create_tail_entry->get_buf());

    DB::KeeperMemoryStorage snapshot_storage(500, "", ctx);
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
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine<DB::KeeperMemoryStorage>>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    auto base_entry = makeCreateEntry(*state_machine, "/base", "base");
    state_machine->pre_commit(1, base_entry->get_buf());
    state_machine->commit(1, base_entry->get_buf());

    static constexpr int64_t session_id = 7;
    auto ephemeral_entry = makeEphemeralCreateEntry(*state_machine, session_id, "/ephemeral", "tail_ephemeral");
    state_machine->pre_commit(2, ephemeral_entry->get_buf());

    DB::KeeperMemoryStorage snapshot_storage(500, "", ctx);
    addNode(snapshot_storage, "/base", "base");
    TSA_SUPPRESS_WARNING_FOR_WRITE(snapshot_storage.zxid) = 1;

    nuraft::snapshot snapshot(1, 0, std::make_shared<nuraft::cluster_config>());
    auto snapshot_buf = makeSnapshotBufferFromStorage(snapshot_storage, 1, ctx);
    saveSingleObjectSnapshot(*state_machine, snapshot, snapshot_buf);

    EXPECT_TRUE(state_machine->apply_snapshot(snapshot));

    auto close_entry = makeCloseEntry(*state_machine, session_id);
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
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine<DB::KeeperMemoryStorage>>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    auto old_entry = makeCreateEntry(*state_machine, "/old", "old");
    state_machine->pre_commit(1, old_entry->get_buf());
    state_machine->commit(1, old_entry->get_buf());

    DB::KeeperMemoryStorage snapshot_storage(500, "", ctx);
    addNode(snapshot_storage, "/replacement", "replacement");
    TSA_SUPPRESS_WARNING_FOR_WRITE(snapshot_storage.zxid) = 1;

    nuraft::snapshot snapshot(1, 0, std::make_shared<nuraft::cluster_config>());
    auto snapshot_buf = makeSnapshotBufferFromStorage(snapshot_storage, 1, ctx);
    saveSingleObjectSnapshot(*state_machine, snapshot, snapshot_buf);

    DB::WriteBufferFromFile plain_buf(
        "./snapshots/snapshot_1.bin.zstd",
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
    DB::KeeperMemoryStorage & storage, uint64_t idx, const DB::KeeperContextPtr & version_ctx)
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

    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> manager(3, iso_ctx, true);
    DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snapshot(
        &storage, idx, nullptr, version_ctx->getWriteSnapshotVersion());
    return manager.serializeSnapshotToBuffer(snapshot);
}

/// Build a state-equivalent "install" snapshot file for `idx` (term 0) holding a single marker
/// node and save it through the receive path without applying it. Mirrors a fully received but
/// not-yet-applied snapshot install.
void saveInstallSnapshot(
    DB::KeeperStateMachine<DB::KeeperMemoryStorage> & state_machine,
    const DB::KeeperContextPtr & ctx,
    uint64_t idx,
    const std::string & marker)
{
    DB::KeeperMemoryStorage storage(500, "", ctx);
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
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine<DB::KeeperMemoryStorage>>(nullptr, snapshots_queue, ctx, nullptr);
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
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_5.bin.zstd"));

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
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine<DB::KeeperMemoryStorage>>(nullptr, snapshots_queue, ctx, nullptr);
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
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_3.bin.zstd"));
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 3);
}

/// After a restart, a snapshot that was saved but never applied is the newest disk snapshot and
/// `init` recovers from it (a fully saved snapshot is a valid committed prefix).
TEST(KeeperMemorySnapshotApplyTest, RestartAfterSavedButNotAppliedRecoversNewestDiskSnapshot)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    {
        auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
        DB::SnapshotsQueue snapshots_queue{1};
        auto sm1 = std::make_shared<DB::KeeperStateMachine<DB::KeeperMemoryStorage>>(nullptr, snapshots_queue, ctx, nullptr);
        sm1->init();

        auto e1 = makeCreateEntry(*sm1, "/old", "old");
        sm1->pre_commit(1, e1->get_buf());
        sm1->commit(1, e1->get_buf());

        saveInstallSnapshot(*sm1, ctx, 5, "/from_snap5");
        EXPECT_TRUE(fs::exists("./snapshots/snapshot_5.bin.zstd"));
        /// sm1 destroyed without applying snapshot 5.
    }

    {
        auto ctx2 = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
        DB::SnapshotsQueue snapshots_queue2{1};
        auto sm2 = std::make_shared<DB::KeeperStateMachine<DB::KeeperMemoryStorage>>(nullptr, snapshots_queue2, ctx2, nullptr);
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
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine<DB::KeeperMemoryStorage>>(nullptr, snapshots_queue, ctx, nullptr);
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
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_2.bin.zstd"));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_3.bin.zstd"));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_4.bin.zstd"));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_5.bin.zstd"));

    /// (4) save 6 -> remove 3 -> {2,4,5,6}; the mark's file (2) stays servable.
    saveInstallSnapshot(*state_machine, ctx, 6, "/snap6");
    EXPECT_FALSE(fs::exists("./snapshots/snapshot_3.bin.zstd"));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_2.bin.zstd"));
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 2);

    /// (5) create-skip servability: returns the mark's file (snapshot_2), not the map max.
    {
        nuraft::snapshot s2_again(2, 0, std::make_shared<nuraft::cluster_config>());
        auto info = executeCreateSnapshotTask(*state_machine, snapshots_queue, s2_again);
        ASSERT_NE(info, nullptr);
        EXPECT_EQ(fs::path(info->path).filename().string(), "snapshot_2.bin.zstd");
    }

    /// (6) apply snapshot 6 -> mark advances to 6.
    {
        nuraft::snapshot s6(6, 0, std::make_shared<nuraft::cluster_config>());
        EXPECT_TRUE(state_machine->apply_snapshot(s6));
        EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 6);
    }

    /// (7) protection moved to 6: save 7 -> remove 2 then 4 -> {5,6,7}; mark 6 + its file survive.
    saveInstallSnapshot(*state_machine, ctx, 7, "/snap7");
    EXPECT_FALSE(fs::exists("./snapshots/snapshot_2.bin.zstd"));
    EXPECT_FALSE(fs::exists("./snapshots/snapshot_4.bin.zstd"));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_6.bin.zstd"));
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 6);
}

/// A covered stale apply (s.idx < pending, but local commits already cover s.idx) skips without
/// divergence and leaves the pending intact for its own apply.
TEST(KeeperMemorySnapshotApplyTest, CoveredStaleApplySkipsWithoutDivergence)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine<DB::KeeperMemoryStorage>>(nullptr, snapshots_queue, ctx, nullptr);
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

/// A queued same-index create that drains after a stale duplicate install at the same index was
/// saved (but not applied) adopts the registered file instead of rewriting it in place. The armed
/// disk is the discriminator: a regression to rewrite behavior would writeFile and throw.
TEST(KeeperMemorySnapshotApplyTest, QueuedSameIndexCreateAdoptsRegisteredInstallSnapshot)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    {
        auto settings = std::make_shared<DB::CoordinationSettings>();
#if USE_ROCKSDB
        (*settings)[DB::CoordinationSetting::experimental_use_rocksdb] = false;
#endif
        (*settings)[DB::CoordinationSetting::compress_snapshots_with_zstd_format] = true;
        auto ctx = std::make_shared<DB::KeeperContext>(true, settings);
        ctx->setLocalLogsPreprocessed();
        ctx->setDigestEnabled(true);
        auto throwing_disk = std::make_shared<ThrowingSnapshotDisk>(
            "SnapshotDisk", "./snapshots", "snapshot_5.bin.zstd", SnapshotDiskFailureMode::OpenFileAfterCreate);
        ctx->setSnapshotDisk(throwing_disk);
        ctx->setRocksDBDisk(std::make_shared<DB::DiskLocal>("RocksDisk", "./rocksdb"));
        ctx->setRocksDBOptions();
        throwing_disk->disarm();

        DB::SnapshotsQueue snapshots_queue{1};
        auto sm1 = std::make_shared<DB::KeeperStateMachine<DB::KeeperMemoryStorage>>(nullptr, snapshots_queue, ctx, nullptr);
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
        DB::KeeperMemoryStorage install5(500, "", ctx);
        for (uint64_t idx = 1; idx <= 5; ++idx)
            addNode(install5, fmt::format("/n{}", idx), "v");
        TSA_SUPPRESS_WARNING_FOR_WRITE(install5.zxid) = 5;
        nuraft::snapshot s5_save(5, 0, std::make_shared<nuraft::cluster_config>());
        auto buf5 = makeInstallBuffer(install5, 5, ctx);
        saveSingleObjectSnapshot(*sm1, s5_save, buf5);
        EXPECT_EQ(sm1->last_snapshot()->get_last_log_idx(), 1); /// mark still 1, pending 5

        /// Arm the disk: any rewrite of snapshot_5 would now throw and return nullptr.
        throwing_disk->arm();
        nuraft::snapshot s5_create(5, 0, std::make_shared<nuraft::cluster_config>());
        auto info = executeCreateSnapshotTask(*sm1, snapshots_queue, s5_create);
        ASSERT_NE(info, nullptr); /// adopted, not rewritten
        EXPECT_EQ(fs::path(info->path).filename().string(), "snapshot_5.bin.zstd");
        ASSERT_NE(sm1->last_snapshot(), nullptr);
        EXPECT_EQ(sm1->last_snapshot()->get_last_log_idx(), 5);

        auto & storage = sm1->getStorageUnsafe();
        for (uint64_t idx = 1; idx <= 5; ++idx)
            EXPECT_TRUE(storage.container.contains(fmt::format("/n{}", idx)));
    }

    /// Restart with a plain disk: the adopted snapshot 5 is the newest and recovers cleanly.
    {
        auto ctx2 = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
        DB::SnapshotsQueue snapshots_queue2{1};
        auto sm2 = std::make_shared<DB::KeeperStateMachine<DB::KeeperMemoryStorage>>(nullptr, snapshots_queue2, ctx2, nullptr);
        sm2->init();
        ASSERT_NE(sm2->last_snapshot(), nullptr);
        EXPECT_EQ(sm2->last_snapshot()->get_last_log_idx(), 5);
        auto & storage = sm2->getStorageUnsafe();
        for (uint64_t idx = 1; idx <= 5; ++idx)
            EXPECT_TRUE(storage.container.contains(fmt::format("/n{}", idx)));
    }
}

/// A registered same-index entry whose file cannot be confirmed present makes the create fail
/// closed (nullptr) without advancing the mark, rewriting bytes, or recreating the file.
TEST(KeeperMemorySnapshotApplyTest, SameIndexCreateFailsClosedOnMissingRegisteredFile)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine<DB::KeeperMemoryStorage>>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    /// commit 1 /n1, create-win at 1 -> mark 1.
    auto e1 = makeCreateEntry(*state_machine, "/n1", "v1");
    state_machine->pre_commit(1, e1->get_buf());
    state_machine->commit(1, e1->get_buf());
    nuraft::snapshot s1(1, 0, std::make_shared<nuraft::cluster_config>());
    {
        auto info = executeCreateSnapshotTask(*state_machine, snapshots_queue, s1);
        ASSERT_NE(info, nullptr);
    }
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 1);

    /// commits 2-5 so a create at 5 is above the mark (takes the else branch).
    for (uint64_t idx = 2; idx <= 5; ++idx)
    {
        auto entry = makeCreateEntry(*state_machine, fmt::format("/n{}", idx), "v");
        state_machine->pre_commit(idx, entry->get_buf());
        state_machine->commit(idx, entry->get_buf());
    }

    /// Save a full install of 5 (registered, never applied) -> map {1,5}, pending 5, mark 1.
    saveInstallSnapshot(*state_machine, ctx, 5, "/from_snap5");
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_5.bin.zstd"));
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 1);

    /// External loss of the registered file (the registry entry remains).
    fs::remove("./snapshots/snapshot_5.bin.zstd");
    ASSERT_FALSE(fs::exists("./snapshots/snapshot_5.bin.zstd"));

    /// A queued create at index 5 must fail closed: existsFile=false -> CORRUPTED_DATA -> nullptr.
    {
        nuraft::snapshot s5_create(5, 0, std::make_shared<nuraft::cluster_config>());
        auto info = executeCreateSnapshotTask(*state_machine, snapshots_queue, s5_create);
        EXPECT_EQ(info, nullptr);
    }
    /// Mark not advanced; no file was rewritten or recreated.
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 1);
    EXPECT_FALSE(fs::exists("./snapshots/snapshot_5.bin.zstd"));

    /// The fail-closed verdict is stable across retries at the same index (registry entry intact).
    {
        nuraft::snapshot s5_create_again(5, 0, std::make_shared<nuraft::cluster_config>());
        auto info = executeCreateSnapshotTask(*state_machine, snapshots_queue, s5_create_again);
        EXPECT_EQ(info, nullptr);
    }
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 1);

    /// Recovery at the next boundary: commit 6, create-win at 6 succeeds and advances the mark.
    auto e6 = makeCreateEntry(*state_machine, "/n6", "v6");
    state_machine->pre_commit(6, e6->get_buf());
    state_machine->commit(6, e6->get_buf());
    nuraft::snapshot s6(6, 0, std::make_shared<nuraft::cluster_config>());
    {
        auto info = executeCreateSnapshotTask(*state_machine, snapshots_queue, s6);
        ASSERT_NE(info, nullptr);
    }
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_6.bin.zstd"));
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 6);
}

/// A create at or below the mark returns the file backing the high-water mark, not the manager's
/// map max (which may be a saved-but-not-applied install feeding S3/shutdown uploads).
TEST(KeeperMemorySnapshotApplyTest, CreateSkipReturnsHighWaterMarkFileNotMapMax)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine<DB::KeeperMemoryStorage>>(nullptr, snapshots_queue, ctx, nullptr);
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
    EXPECT_EQ(state_machine->getLatestSnapshotSize(), fs::file_size("./snapshots/snapshot_5.bin.zstd"));

    nuraft::snapshot s2_again(2, 0, std::make_shared<nuraft::cluster_config>());
    auto info = executeCreateSnapshotTask(*state_machine, snapshots_queue, s2_again);
    ASSERT_NE(info, nullptr);
    EXPECT_EQ(fs::path(info->path).filename().string(), "snapshot_2.bin.zstd"); /// not snapshot_5
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 2);
}

/// A local create below saved-but-not-applied installs survives its own retention pass (the
/// just-written pin) and does not clobber the size cache (map-max guard).
TEST(KeeperMemorySnapshotApplyTest, LocalCreateBelowSavedInstallsSurvivesRetention)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine<DB::KeeperMemoryStorage>>(nullptr, snapshots_queue, ctx, nullptr);
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
    EXPECT_EQ(size_12, fs::file_size("./snapshots/snapshot_12.bin.zstd"));
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 2);

    /// (3) commits 3-5.
    for (uint64_t idx = 3; idx <= 5; ++idx)
    {
        auto entry = makeCreateEntry(*state_machine, fmt::format("/n{}", idx), "v");
        state_machine->pre_commit(idx, entry->get_buf());
        state_machine->commit(idx, entry->get_buf());
    }

    /// (4) local create at 5: write path, jw=5 pins 2 (protected) and 5 (just written) -> nothing pruned.
    nuraft::snapshot s5(5, 0, std::make_shared<nuraft::cluster_config>());
    {
        auto info = executeCreateSnapshotTask(*state_machine, snapshots_queue, s5);
        ASSERT_NE(info, nullptr);
    }
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_5.bin.zstd"));
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 5);
    /// Size guard: the create at 5 is below the map max (12) -> must not clobber the cache.
    EXPECT_EQ(state_machine->getLatestSnapshotSize(), size_12);

    /// (5) servability: a create at 5 again hits the skip branch and returns a live pin.
    {
        nuraft::snapshot s5_again(5, 0, std::make_shared<nuraft::cluster_config>());
        auto info = executeCreateSnapshotTask(*state_machine, snapshots_queue, s5_again);
        ASSERT_NE(info, nullptr);
        EXPECT_EQ(fs::path(info->path).filename().string(), "snapshot_5.bin.zstd");
    }

    /// (6) convergence: save full 13 -> remove 2 (no longer protected) then 10 -> {5,11,12,13}.
    saveInstallSnapshot(*state_machine, ctx, 13, "/snap13");
    EXPECT_FALSE(fs::exists("./snapshots/snapshot_2.bin.zstd"));
    EXPECT_FALSE(fs::exists("./snapshots/snapshot_10.bin.zstd"));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_5.bin.zstd"));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_13.bin.zstd"));
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 5);
    EXPECT_EQ(state_machine->getLatestSnapshotSize(), fs::file_size("./snapshots/snapshot_13.bin.zstd"));
}

/// A same-index re-receive whose file lands on a different disk than the registered (moved) entry
/// repoints the registry at the just-written file and retires the stale one, so the matching apply
/// lands the just-written bytes.
TEST(KeeperMemorySnapshotApplyTest, SameIndexReReceiveReplacesStaleRegistryEntryAcrossDisks)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest snapshots_latest("./snapshots_latest");
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    /// setSnapshotDisk overwrites both storages, so set the distinct latest disk afterwards.
    ctx->setLatestSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapshotLatestDisk", "./snapshots_latest"));

    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine<DB::KeeperMemoryStorage>>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    /// (1) local create win at 1 (drop the returned pin so moves are not deferred).
    auto e1 = makeCreateEntry(*state_machine, "/n1", "v1");
    state_machine->pre_commit(1, e1->get_buf());
    state_machine->commit(1, e1->get_buf());
    nuraft::snapshot s1(1, 0, std::make_shared<nuraft::cluster_config>());
    {
        auto info = executeCreateSnapshotTask(*state_machine, snapshots_queue, s1);
        ASSERT_NE(info, nullptr);
    }

    /// (2) save snapshot 5 (marker /v1) -> file on the latest disk.
    saveInstallSnapshot(*state_machine, ctx, 5, "/v1");

    /// (3) save snapshot 6 -> the move pass relocates non-max 5 to the regular disk.
    saveInstallSnapshot(*state_machine, ctx, 6, "/n6");
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_5.bin.zstd"));

    /// (4) re-receive index 5 (marker /v2): collision with a different (disk, path) -> the registry
    /// is repointed at the just-written latest-disk file and the stale regular-disk file is unlinked.
    saveInstallSnapshot(*state_machine, ctx, 5, "/v2");
    EXPECT_FALSE(fs::exists("./snapshots/snapshot_5.bin.zstd"));
    EXPECT_TRUE(fs::exists("./snapshots_latest/snapshot_5.bin.zstd"));

    /// (5) the matching apply lands the just-written bytes (/v2), not the stale /v1.
    nuraft::snapshot s5(5, 0, std::make_shared<nuraft::cluster_config>());
    EXPECT_TRUE(state_machine->apply_snapshot(s5));
    EXPECT_TRUE(state_machine->getStorageUnsafe().container.contains("/v2"));
    EXPECT_FALSE(state_machine->getStorageUnsafe().container.contains("/v1"));
}

/// Verify that concurrent snapshot transfers from a leader with a remote snapshot disk work correctly.
/// A remote disk causes `RemoteSnapshotLoader` to be used, which loads the snapshot into memory once
/// and serves all concurrent followers from the same buffer. The test checks that all followers
/// receive correct data and that the snapshot file is read from disk exactly once.
TYPED_TEST(CoordinationTest, TestReadSnapshotParallelMultiChunk)
{
    getContext(); /// needed for DiskObjectStorage background threads

    ChangelogDirTest snap_meta("./snapshots");
    ChangelogDirTest snap_obj("./snapshots_obj");
    ChangelogDirTest rocks("./rocksdb");

    using Storage = typename TestFixture::Storage;

    auto leader_settings = std::make_shared<DB::CoordinationSettings>();
#if USE_ROCKSDB
    (*leader_settings)[DB::CoordinationSetting::experimental_use_rocksdb] = std::is_same_v<Storage, DB::KeeperRocksStorage>;
#else
    (*leader_settings)[DB::CoordinationSetting::experimental_use_rocksdb] = 0;
#endif
    (*leader_settings)[DB::CoordinationSetting::snapshot_transfer_chunk_size] = 10;
    auto leader_ctx = std::make_shared<DB::KeeperContext>(true, leader_settings);
    leader_ctx->setLocalLogsPreprocessed();
    leader_ctx->setRocksDBDisk(std::make_shared<DB::DiskLocal>("RocksDisk", "./rocksdb"));
    leader_ctx->setRocksDBOptions();

    auto [snap_disk, obj_storage] = createLocalObjectStorageDisk("./snapshots", "./snapshots_obj/");
    leader_ctx->setSnapshotDisk(snap_disk);

    DB::KeeperSnapshotManager<Storage> manager(3, leader_ctx, this->enable_compression);
    Storage storage(500, "", leader_ctx);
    addNode(storage, "/hello", "world");
    DB::KeeperStorageSnapshot<Storage> snap(&storage, 50, nullptr, leader_ctx->getWriteSnapshotVersion());
    auto snap_buf = manager.serializeSnapshotToBuffer(snap);
    manager.serializeSnapshotBufferToDisk(*snap_buf, 50);

    DB::SnapshotsQueue leader_snapshots_queue{1};
    auto leader = std::make_shared<DB::KeeperStateMachine<Storage>>(
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
            threads.emplace_back([&, i] { loaded_data[i] = runFollower<Storage>(i, *leader, s); });
        for (auto & t : threads)
            t.join();
    }
    for (int i = 0; i < num_threads; ++i)
        EXPECT_EQ(loaded_data[i], "world") << "thread " << i;

    EXPECT_EQ(obj_storage->read_count.load() - reads_after_init, 1);

    snap_disk->shutdown();
}

TYPED_TEST(CoordinationTest, SerializeSnapshotToDiskCleansPartialFilesOnOpenException)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    using Storage = typename TestFixture::Storage;

    const std::string snapshot_file_name = "snapshot_50.bin" + this->extension;
    this->keeper_context->setSnapshotDisk(std::make_shared<ThrowingSnapshotDisk>(
        "SnapshotDisk", "./snapshots", snapshot_file_name, SnapshotDiskFailureMode::OpenFileAfterCreate));

    DB::KeeperSnapshotManager<Storage> manager(3, this->keeper_context, this->enable_compression);
    Storage storage(500, "", this->keeper_context);
    addNode(storage, "/hello", "world");
    DB::KeeperStorageSnapshot<Storage> snapshot(&storage, 50, nullptr, this->keeper_context->getWriteSnapshotVersion());

    EXPECT_THROW(manager.serializeSnapshotToDisk(snapshot), std::exception);
    assertNoSnapshotArtifactsAndNoRegistration(
        manager, "./snapshots/" + snapshot_file_name, "./snapshots/tmp_" + snapshot_file_name);
}

TYPED_TEST(CoordinationTest, SerializeSnapshotBufferToDiskCleansPartialFilesOnSyncException)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    using Storage = typename TestFixture::Storage;

    const std::string snapshot_file_name = "snapshot_51.bin" + this->extension;
    this->keeper_context->setSnapshotDisk(std::make_shared<ThrowingSnapshotDisk>(
        "SnapshotDisk", "./snapshots", snapshot_file_name, SnapshotDiskFailureMode::SyncFile));

    DB::KeeperSnapshotManager<Storage> manager(3, this->keeper_context, this->enable_compression);
    Storage storage(500, "", this->keeper_context);
    addNode(storage, "/hello", "world");
    DB::KeeperStorageSnapshot<Storage> snapshot(&storage, 51, nullptr, this->keeper_context->getWriteSnapshotVersion());
    auto buf = manager.serializeSnapshotToBuffer(snapshot);

    EXPECT_THROW(manager.serializeSnapshotBufferToDisk(*buf, 51), std::exception);
    assertNoSnapshotArtifactsAndNoRegistration(
        manager, "./snapshots/" + snapshot_file_name, "./snapshots/tmp_" + snapshot_file_name);
}

TYPED_TEST(CoordinationTest, SerializeSnapshotBufferToDiskKeepsMarkerWhenCleanupCannotRemoveDataFile)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    using Storage = typename TestFixture::Storage;

    const std::string snapshot_file_name = "snapshot_56.bin" + this->extension;
    const std::string tmp_snapshot_file_name = "tmp_" + snapshot_file_name;
    this->keeper_context->setSnapshotDisk(std::make_shared<ThrowingSnapshotDisk>(
        "SnapshotDisk", "./snapshots", snapshot_file_name, SnapshotDiskFailureMode::SyncFileAndCleanupDataFileRemoveFailure));

    DB::KeeperSnapshotManager<Storage> manager(3, this->keeper_context, this->enable_compression);
    Storage storage(500, "", this->keeper_context);
    addNode(storage, "/hello", "world");
    DB::KeeperStorageSnapshot<Storage> snapshot(&storage, 56, nullptr, this->keeper_context->getWriteSnapshotVersion());
    auto buf = manager.serializeSnapshotToBuffer(snapshot);

    EXPECT_THROW(manager.serializeSnapshotBufferToDisk(*buf, 56), std::exception);
    EXPECT_TRUE(fs::exists("./snapshots/" + snapshot_file_name));
    EXPECT_TRUE(fs::exists("./snapshots/" + tmp_snapshot_file_name));
    EXPECT_EQ(manager.totalSnapshots(), 0);
    EXPECT_EQ(manager.getLatestSnapshotIndex(), 0);
    EXPECT_EQ(manager.getLatestSnapshotInfo(), nullptr);
}

TYPED_TEST(CoordinationTest, SerializeSnapshotBufferToDiskCleansMarkerWhenMarkerCreationFails)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    using Storage = typename TestFixture::Storage;

    const std::string snapshot_file_name = "snapshot_52.bin" + this->extension;
    const std::string tmp_snapshot_file_name = "tmp_" + snapshot_file_name;
    this->keeper_context->setSnapshotDisk(std::make_shared<ThrowingSnapshotDisk>(
        "SnapshotDisk", "./snapshots", tmp_snapshot_file_name, SnapshotDiskFailureMode::OpenFileAfterCreate));

    DB::KeeperSnapshotManager<Storage> manager(3, this->keeper_context, this->enable_compression);
    Storage storage(500, "", this->keeper_context);
    addNode(storage, "/hello", "world");
    DB::KeeperStorageSnapshot<Storage> snapshot(&storage, 52, nullptr, this->keeper_context->getWriteSnapshotVersion());
    auto buf = manager.serializeSnapshotToBuffer(snapshot);

    EXPECT_THROW(manager.serializeSnapshotBufferToDisk(*buf, 52), std::exception);
    assertNoSnapshotArtifactsAndNoRegistration(
        manager, "./snapshots/" + snapshot_file_name, "./snapshots/" + tmp_snapshot_file_name);
}

TYPED_TEST(CoordinationTest, SerializeSnapshotBufferToDiskRemovesDataFileWhenMarkerRemovalFails)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    using Storage = typename TestFixture::Storage;

    const std::string snapshot_file_name = "snapshot_53.bin" + this->extension;
    const std::string tmp_snapshot_file_name = "tmp_" + snapshot_file_name;
    this->keeper_context->setSnapshotDisk(std::make_shared<ThrowingSnapshotDisk>(
        "SnapshotDisk", "./snapshots", tmp_snapshot_file_name, SnapshotDiskFailureMode::RemoveFileOnce));

    DB::KeeperSnapshotManager<Storage> manager(3, this->keeper_context, this->enable_compression);
    Storage storage(500, "", this->keeper_context);
    addNode(storage, "/hello", "world");
    DB::KeeperStorageSnapshot<Storage> snapshot(&storage, 53, nullptr, this->keeper_context->getWriteSnapshotVersion());
    auto buf = manager.serializeSnapshotToBuffer(snapshot);

    EXPECT_THROW(manager.serializeSnapshotBufferToDisk(*buf, 53), std::exception);
    assertNoSnapshotArtifactsAndNoRegistration(
        manager, "./snapshots/" + snapshot_file_name, "./snapshots/" + tmp_snapshot_file_name);
}

TYPED_TEST(CoordinationTest, BeginSnapshotReceiveToDiskCleansPartialFilesOnOpenException)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    using Storage = typename TestFixture::Storage;

    const std::string snapshot_file_name = "snapshot_54.bin" + this->extension;
    this->keeper_context->setSnapshotDisk(std::make_shared<ThrowingSnapshotDisk>(
        "SnapshotDisk", "./snapshots", snapshot_file_name, SnapshotDiskFailureMode::OpenFileAfterCreate));

    DB::KeeperSnapshotManager<Storage> manager(3, this->keeper_context, this->enable_compression);

    EXPECT_THROW(manager.beginSnapshotReceiveToDisk(54), std::exception);
    assertNoSnapshotArtifactsAndNoRegistration(
        manager, "./snapshots/" + snapshot_file_name, "./snapshots/tmp_" + snapshot_file_name);
}

TYPED_TEST(CoordinationTest, FinalizeSnapshotReceiveToDiskCleansPartialFilesOnSyncException)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    using Storage = typename TestFixture::Storage;

    const std::string snapshot_file_name = "snapshot_55.bin" + this->extension;
    this->keeper_context->setSnapshotDisk(std::make_shared<ThrowingSnapshotDisk>(
        "SnapshotDisk", "./snapshots", snapshot_file_name, SnapshotDiskFailureMode::SyncFile));

    DB::KeeperSnapshotManager<Storage> manager(3, this->keeper_context, this->enable_compression);
    auto receive_ctx = manager.beginSnapshotReceiveToDisk(55);
    const std::string partial_snapshot_bytes = "partial snapshot bytes";
    receive_ctx->write_buf->write(partial_snapshot_bytes.data(), partial_snapshot_bytes.size());

    EXPECT_THROW(manager.finalizeSnapshotReceiveToDisk(*receive_ctx), std::exception);
    EXPECT_FALSE(receive_ctx->write_buf);
    assertNoSnapshotArtifactsAndNoRegistration(
        manager, "./snapshots/" + snapshot_file_name, "./snapshots/tmp_" + snapshot_file_name);
}

TEST(KeeperSnapshotManagerCleanupTest, CreateSnapshotKeepsPreviousMetadataAndAllowsRetryAfterFailedWrite)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    auto settings = std::make_shared<DB::CoordinationSettings>();
#if USE_ROCKSDB
    (*settings)[DB::CoordinationSetting::experimental_use_rocksdb] = false;
#endif
    auto ctx = std::make_shared<DB::KeeperContext>(true, settings);
    ctx->setLocalLogsPreprocessed();
    auto throwing_disk = std::make_shared<ThrowingSnapshotDisk>(
        "SnapshotDisk", "./snapshots", "snapshot_2.bin.zstd", SnapshotDiskFailureMode::OpenFileAfterCreate);
    ctx->setSnapshotDisk(throwing_disk);
    ctx->setRocksDBDisk(std::make_shared<DB::DiskLocal>("RocksDisk", "./rocksdb"));
    ctx->setRocksDBOptions();

    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine<DB::KeeperMemoryStorage>>(nullptr, snapshots_queue, ctx, nullptr);
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
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_1.bin.zstd"));
    EXPECT_FALSE(fs::exists("./snapshots/tmp_snapshot_1.bin.zstd"));

    auto request2 = std::make_shared<Coordination::ZooKeeperCreateRequest>();
    request2->path = "/node2";
    auto entry2 = getLogEntryFromZKRequest(0, 1, state_machine->getNextZxid(), request2);
    state_machine->pre_commit(2, entry2->get_buf());
    state_machine->commit(2, entry2->get_buf());

    nuraft::snapshot s2(2, 0, std::make_shared<nuraft::cluster_config>());
    bool callback_called_2 = false;
    bool callback_result_2 = true;
    execute_snapshot_task(s2, callback_called_2, callback_result_2);
    EXPECT_TRUE(callback_called_2);
    EXPECT_FALSE(callback_result_2);
    ASSERT_NE(state_machine->last_snapshot(), nullptr);
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 1);
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_1.bin.zstd"));
    EXPECT_FALSE(fs::exists("./snapshots/snapshot_2.bin.zstd"));
    EXPECT_FALSE(fs::exists("./snapshots/tmp_snapshot_2.bin.zstd"));
    EXPECT_EQ(state_machine->last_commit_index(), 2);

    throwing_disk->disarm();
    bool callback_called_3 = false;
    bool callback_result_3 = false;
    execute_snapshot_task(s2, callback_called_3, callback_result_3);
    EXPECT_TRUE(callback_called_3);
    EXPECT_TRUE(callback_result_3);
    ASSERT_NE(state_machine->last_snapshot(), nullptr);
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 2);
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_1.bin.zstd"));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_2.bin.zstd"));
    EXPECT_FALSE(fs::exists("./snapshots/tmp_snapshot_2.bin.zstd"));
}

// ---------------------------------------------------------------------------
// Chunked snapshot header pack/unpack + bounds validation tests.
// These tests exercise KeeperChunkedSnapshot.h / .cpp independently of the actual
// snapshot write or read paths.
// ---------------------------------------------------------------------------

namespace DB
{

/// Build a minimal valid 3-chunk header (METADATA, NODES, SESSIONS) and round-trip through
/// packChunkedSnapshotHeader + parseAndValidateChunkedSnapshotHeader.
TEST(KeeperChunkedSnapshotHeader, RoundTripMinimal)
{
    const size_t hdr_size = chunkedSnapshotHeaderSize(3);
    // Frames start right after the header; no gaps.
    std::vector<SnapshotChunkDescriptor> frames = {
        {SnapshotChunkType::METADATA, hdr_size,         100},
        {SnapshotChunkType::NODES,    hdr_size + 100,   200},
        {SnapshotChunkType::SESSIONS, hdr_size + 300,    50},
    };
    const size_t total_size = hdr_size + 350;

    std::vector<char> buf(total_size, '\0');
    packChunkedSnapshotHeader(frames, buf.data());

    auto parsed = parseAndValidateChunkedSnapshotHeader(buf.data(), total_size);

    ASSERT_EQ(parsed.size(), 3u);
    EXPECT_EQ(parsed[0].type, SnapshotChunkType::METADATA);
    EXPECT_EQ(parsed[0].compressed_offset, hdr_size);
    EXPECT_EQ(parsed[0].compressed_size, 100u);
    EXPECT_EQ(parsed[1].type, SnapshotChunkType::NODES);
    EXPECT_EQ(parsed[1].compressed_offset, hdr_size + 100);
    EXPECT_EQ(parsed[1].compressed_size, 200u);
    EXPECT_EQ(parsed[2].type, SnapshotChunkType::SESSIONS);
    EXPECT_EQ(parsed[2].compressed_offset, hdr_size + 300);
    EXPECT_EQ(parsed[2].compressed_size, 50u);
}

/// Multiple NODES frames round-trip correctly (K > 1 per the write path).
TEST(KeeperChunkedSnapshotHeader, RoundTripMultipleNodesFrames)
{
    const uint64_t chunk_count = 5; // METADATA + 3 NODES + SESSIONS
    const size_t hdr_size = chunkedSnapshotHeaderSize(chunk_count);

    std::vector<SnapshotChunkDescriptor> frames;
    frames.push_back({SnapshotChunkType::METADATA, hdr_size, 80});
    frames.push_back({SnapshotChunkType::NODES, hdr_size + 80, 300});
    frames.push_back({SnapshotChunkType::NODES, hdr_size + 380, 250});
    frames.push_back({SnapshotChunkType::NODES, hdr_size + 630, 150});
    frames.push_back({SnapshotChunkType::SESSIONS, hdr_size + 780, 40});
    const size_t total_size = hdr_size + 820;

    std::vector<char> buf(total_size, '\0');
    packChunkedSnapshotHeader(frames, buf.data());

    auto parsed = parseAndValidateChunkedSnapshotHeader(buf.data(), total_size);

    ASSERT_EQ(parsed.size(), 5u);
    EXPECT_EQ(parsed[0].type, SnapshotChunkType::METADATA);
    EXPECT_EQ(parsed[1].type, SnapshotChunkType::NODES);
    EXPECT_EQ(parsed[2].type, SnapshotChunkType::NODES);
    EXPECT_EQ(parsed[3].type, SnapshotChunkType::NODES);
    EXPECT_EQ(parsed[4].type, SnapshotChunkType::SESSIONS);
    EXPECT_EQ(parsed[4].compressed_size, 40u);
}

/// Frames with gaps between them (non-contiguous) are still valid.
TEST(KeeperChunkedSnapshotHeader, RoundTripWithGapsBetweenFrames)
{
    const size_t hdr_size = chunkedSnapshotHeaderSize(3);
    const size_t gap = 128;
    std::vector<SnapshotChunkDescriptor> frames = {
        {SnapshotChunkType::METADATA, hdr_size,                        100},
        {SnapshotChunkType::NODES,    hdr_size + 100 + gap,            200},
        {SnapshotChunkType::SESSIONS, hdr_size + 100 + gap + 200 + gap, 50},
    };
    const size_t total_size = frames.back().compressed_offset + frames.back().compressed_size;

    std::vector<char> buf(total_size, '\0');
    packChunkedSnapshotHeader(frames, buf.data());

    auto parsed = parseAndValidateChunkedSnapshotHeader(buf.data(), total_size);
    ASSERT_EQ(parsed.size(), 3u);
    EXPECT_EQ(parsed[1].compressed_offset, hdr_size + 100 + gap);
}

// --- Rejection tests ---------------------------------------------------------

TEST(KeeperChunkedSnapshotHeader, RejectsBufTooSmall)
{
    // A buffer of 12 bytes (< 13) must be rejected immediately.
    std::vector<char> buf(12, '\0');
    EXPECT_THROW(parseAndValidateChunkedSnapshotHeader(buf.data(), 12), DB::Exception);
}

TEST(KeeperChunkedSnapshotHeader, RejectsWrongMagic)
{
    const size_t hdr_size = chunkedSnapshotHeaderSize(3);
    std::vector<SnapshotChunkDescriptor> frames = {
        {SnapshotChunkType::METADATA, hdr_size, 10},
        {SnapshotChunkType::NODES,    hdr_size + 10, 20},
        {SnapshotChunkType::SESSIONS, hdr_size + 30, 10},
    };
    std::vector<char> buf(hdr_size + 40, '\0');
    packChunkedSnapshotHeader(frames, buf.data());

    // Overwrite first magic byte with garbage.
    buf[0] = 'X';
    EXPECT_THROW(parseAndValidateChunkedSnapshotHeader(buf.data(), buf.size()), DB::Exception);
}

TEST(KeeperChunkedSnapshotHeader, RejectsChunkCountTwo)
{
    // chunk_count == 2 is always invalid (need at least METADATA+NODES+SESSIONS=3).
    const size_t hdr_size = chunkedSnapshotHeaderSize(2);
    std::vector<char> buf(hdr_size + 100, '\0');

    // Write a syntactically-valid 2-chunk header.
    std::vector<SnapshotChunkDescriptor> frames_2 = {
        {SnapshotChunkType::METADATA, hdr_size, 50},
        {SnapshotChunkType::SESSIONS, hdr_size + 50, 50},
    };
    packChunkedSnapshotHeader(frames_2, buf.data());

    // Should throw CORRUPTED_DATA (chunk_count < 3).
    EXPECT_THROW(parseAndValidateChunkedSnapshotHeader(buf.data(), buf.size()), DB::Exception);
}

TEST(KeeperChunkedSnapshotHeader, RejectsChunkCountZeroAndOne)
{
    for (uint64_t cc : {uint64_t{0}, uint64_t{1}})
    {
        const size_t hdr_size = chunkedSnapshotHeaderSize(cc);
        std::vector<char> buf(hdr_size + 10, '\0');
        // Write magic + version + chunk_count manually.
        memcpy(buf.data(), KEEPER_CHUNKED_SNAPSHOT_MAGIC.data(), 4);
        buf[4] = KEEPER_CHUNKED_SNAPSHOT_VERSION;
        memcpy(buf.data() + 5, &cc, 8);
        EXPECT_THROW(parseAndValidateChunkedSnapshotHeader(buf.data(), buf.size()), DB::Exception);
    }
}

TEST(KeeperChunkedSnapshotHeader, RejectsOffsetOverlappingHeader)
{
    // A frame that starts before the header ends.
    const size_t hdr_size = chunkedSnapshotHeaderSize(3);
    std::vector<SnapshotChunkDescriptor> frames = {
        {SnapshotChunkType::METADATA, hdr_size - 1, 100}, // overlaps header
        {SnapshotChunkType::NODES,    hdr_size + 100, 200},
        {SnapshotChunkType::SESSIONS, hdr_size + 300, 50},
    };
    std::vector<char> buf(hdr_size + 350, '\0');
    packChunkedSnapshotHeader(frames, buf.data());
    EXPECT_THROW(parseAndValidateChunkedSnapshotHeader(buf.data(), buf.size()), DB::Exception);
}

TEST(KeeperChunkedSnapshotHeader, RejectsOverlappingFrames)
{
    // Frame 1 starts before frame 0 ends (overlapping by 1 byte).
    const size_t hdr_size = chunkedSnapshotHeaderSize(3);
    std::vector<SnapshotChunkDescriptor> frames = {
        {SnapshotChunkType::METADATA, hdr_size, 100},
        {SnapshotChunkType::NODES,    hdr_size + 99, 200}, // starts 1 byte before frame 0 ends
        {SnapshotChunkType::SESSIONS, hdr_size + 299, 50},
    };
    std::vector<char> buf(hdr_size + 349, '\0');
    packChunkedSnapshotHeader(frames, buf.data());
    EXPECT_THROW(parseAndValidateChunkedSnapshotHeader(buf.data(), buf.size()), DB::Exception);
}

TEST(KeeperChunkedSnapshotHeader, RejectsFrameExceedingBufferSize)
{
    // A frame whose end extends beyond the buffer.
    const size_t hdr_size = chunkedSnapshotHeaderSize(3);
    std::vector<SnapshotChunkDescriptor> frames = {
        {SnapshotChunkType::METADATA, hdr_size, 100},
        {SnapshotChunkType::NODES,    hdr_size + 100, 200},
        {SnapshotChunkType::SESSIONS, hdr_size + 300, 9999}, // way too large
    };
    std::vector<char> buf(hdr_size + 350, '\0'); // total_size only covers first two frames
    packChunkedSnapshotHeader(frames, buf.data());
    EXPECT_THROW(parseAndValidateChunkedSnapshotHeader(buf.data(), buf.size()), DB::Exception);
}

TEST(KeeperChunkedSnapshotHeader, RejectsWrongFirstChunkType)
{
    // First chunk must be METADATA, not NODES.
    const size_t hdr_size = chunkedSnapshotHeaderSize(3);
    std::vector<SnapshotChunkDescriptor> frames = {
        {SnapshotChunkType::NODES,    hdr_size, 100},     // wrong: should be METADATA
        {SnapshotChunkType::NODES,    hdr_size + 100, 200},
        {SnapshotChunkType::SESSIONS, hdr_size + 300, 50},
    };
    std::vector<char> buf(hdr_size + 350, '\0');
    packChunkedSnapshotHeader(frames, buf.data());
    EXPECT_THROW(parseAndValidateChunkedSnapshotHeader(buf.data(), buf.size()), DB::Exception);
}

TEST(KeeperChunkedSnapshotHeader, RejectsWrongLastChunkType)
{
    // Last chunk must be SESSIONS, not NODES.
    const size_t hdr_size = chunkedSnapshotHeaderSize(3);
    std::vector<SnapshotChunkDescriptor> frames = {
        {SnapshotChunkType::METADATA, hdr_size, 100},
        {SnapshotChunkType::NODES,    hdr_size + 100, 200},
        {SnapshotChunkType::NODES,    hdr_size + 300, 50}, // wrong: should be SESSIONS
    };
    std::vector<char> buf(hdr_size + 350, '\0');
    packChunkedSnapshotHeader(frames, buf.data());
    EXPECT_THROW(parseAndValidateChunkedSnapshotHeader(buf.data(), buf.size()), DB::Exception);
}

TEST(KeeperChunkedSnapshotHeader, RejectsNonMonotonicOffsets)
{
    // First frame at a higher offset than the second — non-monotonic.
    const size_t hdr_size = chunkedSnapshotHeaderSize(3);
    std::vector<SnapshotChunkDescriptor> frames = {
        {SnapshotChunkType::METADATA, hdr_size + 300, 10},
        {SnapshotChunkType::NODES,    hdr_size + 100, 200}, // starts before frame 0 ends
        {SnapshotChunkType::SESSIONS, hdr_size + 300, 50},
    };
    std::vector<char> buf(hdr_size + 400, '\0');
    packChunkedSnapshotHeader(frames, buf.data());
    EXPECT_THROW(parseAndValidateChunkedSnapshotHeader(buf.data(), buf.size()), DB::Exception);
}

TEST(KeeperChunkedSnapshotHeader, ChunkCountExceedsBufferCapacity)
{
    // chunk_count so large that chunkedSnapshotHeaderSize(chunk_count) > buf_size.
    std::vector<char> buf(16, '\0');
    memcpy(buf.data(), KEEPER_CHUNKED_SNAPSHOT_MAGIC.data(), 4);
    buf[4] = KEEPER_CHUNKED_SNAPSHOT_VERSION;
    uint64_t big = 100; // chunkedSnapshotHeaderSize(100) = 2513, but buf is only 16 bytes
    memcpy(buf.data() + 5, &big, 8);
    EXPECT_THROW(parseAndValidateChunkedSnapshotHeader(buf.data(), buf.size()), DB::Exception);
}

/// Verify that the chunked snapshot write path produces a structurally valid snapshot buffer.
/// Serializes a small in-memory storage to chunked format via serializeSnapshotToBuffer
/// (`MAX_SUPPORTED_SNAPSHOT_VERSION` is V8; `SnapshotVersion::V8` is passed directly for clarity).
TEST(KeeperChunkedSnapshotWrite, InspectBytes)
{
    ChangelogDirTest snap_dir("./chunked_write_test_snap");

    // Set up a minimal KeeperContext with default settings.
    auto settings = std::make_shared<DB::CoordinationSettings>();
    auto keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_context->setLocalLogsPreprocessed();
    keeper_context->setRocksDBOptions();
    keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk", snap_dir.path));

    // Create a small storage with a few nodes.
    DB::KeeperMemoryStorage storage(500, "", keeper_context);
    addNode(storage, "/alpha",   "hello");
    addNode(storage, "/beta",    "world");
    addNode(storage, "/gamma",   "data");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 10;

    // Build a chunked snapshot (explicit version passed to the snapshot constructor).
    DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snapshot(
        &storage, /*up_to_log_idx=*/10, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);

    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> manager(3, keeper_context, /*compress_zstd=*/true);
    auto buf = manager.serializeSnapshotToBuffer(snapshot);

    ASSERT_NE(buf, nullptr);
    ASSERT_GT(buf->size(), 0u);

    const char * data = reinterpret_cast<const char *>(buf->data_begin());
    const size_t data_size = buf->size();

    // Parse and validate the chunked snapshot header; throws on any structural violation.
    auto frames = parseAndValidateChunkedSnapshotHeader(data, data_size);

    // Must have METADATA + at least 1 NODES + SESSIONS.
    ASSERT_GE(frames.size(), DB::KEEPER_CHUNKED_SNAPSHOT_MIN_CHUNK_COUNT);

    // Chunk ordering: METADATA first, SESSIONS last, all middle chunks are NODES.
    EXPECT_EQ(frames.front().type, SnapshotChunkType::METADATA);
    EXPECT_EQ(frames.back().type,  SnapshotChunkType::SESSIONS);
    for (size_t i = 1; i + 1 < frames.size(); ++i)
        EXPECT_EQ(frames[i].type, SnapshotChunkType::NODES) << "Chunk " << i << " should be NODES";

    // Every frame must begin with the ZSTD magic bytes (0x28 0xB5 0x2F 0xFD).
    static constexpr unsigned char kZstdMagic[4] = {0x28, 0xB5, 0x2F, 0xFD};
    for (size_t i = 0; i < frames.size(); ++i)
    {
        const auto & f = frames[i];
        ASSERT_GE(data_size, f.compressed_offset + 4u) << "Frame " << i << " too short for magic check";
        EXPECT_EQ(memcmp(data + f.compressed_offset, kZstdMagic, 4), 0)
            << "Frame " << i << " does not start with ZSTD magic";
    }

    // Verify that the first NODES frame descriptor carries a non-zero node_count.
    // node_count now lives in the header descriptor (not the frame body).
    // Storage has 4 non-system nodes: /, /alpha, /beta, /gamma (system nodes excluded).
    // With the default chunk_size_limit (100000), all 4 fit in one NODES frame.
    {
        const auto & nf = frames[1]; // first NODES frame
        EXPECT_GE(nf.node_count, 1u) << "NODES frame descriptor must carry at least the root node count";
    }
}

/// Verify the chunked snapshot disk write path (serializeSnapshotToDisk):
///  - chunk structure/ordering on raw file bytes
///  - ZSTD magic at each chunk offset
///  - KeeperSnapshotWrittenBytes equals actual file size (captured before seek-back)
TEST(KeeperChunkedSnapshotWrite, DiskInspectBytes)
{
    ChangelogDirTest snap_dir("./chunked_disk_test_snap");

    auto settings = std::make_shared<DB::CoordinationSettings>();
    auto keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_context->setLocalLogsPreprocessed();
    keeper_context->setRocksDBOptions();
    auto snap_disk = std::make_shared<DB::DiskLocal>("SnapDiskDisk", snap_dir.path);
    keeper_context->setSnapshotDisk(snap_disk);

    DB::KeeperMemoryStorage storage(500, "", keeper_context);
    addNode(storage, "/p", "v1");
    addNode(storage, "/q", "v2");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 5;

    // V8 is the maximum supported version; SnapshotVersion::V8 is passed directly for clarity.
    DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snapshot(
        &storage, /*up_to_log_idx=*/5, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);

    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> manager(3, keeper_context, /*compress_zstd=*/false);

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

    // Reported bytes must equal actual file size (count() captured before seek-back).
    EXPECT_EQ(reported_bytes, file_size)
        << "KeeperSnapshotWrittenBytes mismatch — count() must be captured before seek-back";

    ASSERT_GT(file_size, 0u);

    // Parse and validate the chunked snapshot header.
    auto frames = parseAndValidateChunkedSnapshotHeader(raw.data(), file_size);

    ASSERT_GE(frames.size(), DB::KEEPER_CHUNKED_SNAPSHOT_MIN_CHUNK_COUNT);
    EXPECT_EQ(frames.front().type, SnapshotChunkType::METADATA);
    EXPECT_EQ(frames.back().type,  SnapshotChunkType::SESSIONS);
    for (size_t i = 1; i + 1 < frames.size(); ++i)
        EXPECT_EQ(frames[i].type, SnapshotChunkType::NODES) << "Chunk " << i << " should be NODES";

    static constexpr unsigned char kZstdMagic[4] = {0x28, 0xB5, 0x2F, 0xFD};
    for (size_t i = 0; i < frames.size(); ++i)
    {
        ASSERT_GE(file_size, frames[i].compressed_offset + 4u) << "Chunk " << i << " too short";
        EXPECT_EQ(memcmp(raw.data() + frames[i].compressed_offset, kZstdMagic, 4), 0)
            << "Chunk " << i << " does not start with ZSTD magic";
    }
}

TEST(KeeperChunkedSnapshotHeader, HeaderSizeComputation)
{
    // Verify the constexpr formula: 13 + 25 * chunk_count.
    EXPECT_EQ(chunkedSnapshotHeaderSize(0),   13u);
    EXPECT_EQ(chunkedSnapshotHeaderSize(1),   38u);
    EXPECT_EQ(chunkedSnapshotHeaderSize(3),   88u);
    EXPECT_EQ(chunkedSnapshotHeaderSize(100), 2513u);
}

// ─── Chunked snapshot read-path tests (sequential deserialization + validating load API) ──────────

/// Serialize a small chunked snapshot and deserialize it back via `deserializeSnapshotFromBuffer`.
/// Verifies:
///  - 3-way detection routes chunked snapshots to `deserializeChunkedSnapshotFromBuffer` (not the legacy paths)
///  - Nodes (data, acl_id, ephemerals) are faithfully restored
///  - Session and auth state is restored
///  - ACL map is restored
///  - ACL usage counts are correct
TEST(KeeperChunkedSnapshotRead, RoundTripBasic)
{
    ChangelogDirTest snap_dir("./chunked_read_roundtrip");

    auto settings = std::make_shared<DB::CoordinationSettings>();
    auto keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_context->setLocalLogsPreprocessed();
    keeper_context->setRocksDBOptions();
    keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk", snap_dir.path));

    // Build a storage with a few nodes (some ephemeral) and sessions.
    DB::KeeperMemoryStorage storage(500, "", keeper_context);

    // Add ACL mapping id=1 → one ACL entry so the ACL map round-trip is covered.
    Coordination::ACL acl1;
    acl1.permissions = 0x1f; // all
    acl1.scheme = "auth";
    acl1.id = "";
    storage.acl_map.addMapping(1, {acl1});

    addNode(storage, "/persistent", "hello", /*ephemeral_owner=*/0, /*acl_id=*/1);
    addNode(storage, "/ephnode",    "tmp",   /*ephemeral_owner=*/42);
    addNode(storage, "/subdir",     "dir");
    addNode(storage, "/subdir/child", "child_data");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 7;
    storage.session_id_counter = 100;

    // Add a session (id=42) so ephemeral ownership is tracked.
    storage.committed_ephemerals[42].insert("/ephnode");
    ++storage.committed_ephemeral_nodes;
    storage.addSessionID(42, 30000);
    storage.committed_session_and_auth[42] = {{"digest", "user:pass"}};

    // Serialize as chunked format.
    DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
        &storage, /*up_to_log_idx=*/7, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);
    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr(3, keeper_context, /*compress_zstd=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // Verify 3-way detection: buffer starts with "CKFS" magic.
    ASSERT_GE(buf->size(), 4u);
    EXPECT_EQ(memcmp(buf->data_begin(), "CKFS", 4), 0) << "Chunked snapshot must start with CKFS magic";

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
TEST(KeeperChunkedSnapshotRead, MetadataOnlyExtraction)
{
    ChangelogDirTest snap_dir("./chunked_read_meta");

    auto settings = std::make_shared<DB::CoordinationSettings>();
    auto keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_context->setLocalLogsPreprocessed();
    keeper_context->setRocksDBOptions();
    keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk", snap_dir.path));

    DB::KeeperMemoryStorage storage(500, "", keeper_context);
    addNode(storage, "/a", "data");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 42;

    DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
        &storage, /*up_to_log_idx=*/42, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);
    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr(3, keeper_context, /*compress_zstd=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // Must extract snapshot_meta without loading nodes.
    auto meta = mgr.deserializeSnapshotMetadataFromBuffer(buf);
    ASSERT_NE(meta, nullptr);
    EXPECT_EQ(meta->get_last_log_idx(), 42u);
}

/// Verify paths-only mode: `load_full_storage=false` collects paths without building the storage.
TEST(KeeperChunkedSnapshotRead, PathsOnlyMode)
{
    ChangelogDirTest snap_dir("./chunked_read_paths");

    auto settings = std::make_shared<DB::CoordinationSettings>();
    auto keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_context->setLocalLogsPreprocessed();
    keeper_context->setRocksDBOptions();
    keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk", snap_dir.path));

    DB::KeeperMemoryStorage storage(500, "", keeper_context);
    addNode(storage, "/x", "x_data");
    addNode(storage, "/y", "y_data");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 5;

    DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
        &storage, /*up_to_log_idx=*/5, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);
    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr(3, keeper_context, /*compress_zstd=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // load_full_storage=false: paths collected, storage is null (or empty — implementation may vary).
    auto result = mgr.deserializeSnapshotFromBuffer(buf, /*load_full_storage=*/false);
    ASSERT_FALSE(result.paths.empty()) << "paths must be collected in path-only mode";

    // At minimum /, /x, /y should be present.
    const auto & paths = result.paths;
    EXPECT_NE(std::find(paths.begin(), paths.end(), "/"), paths.end())   << "root path missing";
    EXPECT_NE(std::find(paths.begin(), paths.end(), "/x"), paths.end())  << "/x missing";
    EXPECT_NE(std::find(paths.begin(), paths.end(), "/y"), paths.end())  << "/y missing";
}

/// Verify that a legacy ZSTD (V7) snapshot still deserializes correctly after the chunked snapshot
/// gate is opened and `MAX_SUPPORTED_SNAPSHOT_VERSION` is now V8.
TEST(KeeperChunkedSnapshotRead, LegacyV7SnapshotStillLoads)
{
    ChangelogDirTest snap_dir("./chunked_read_legacy");

    auto settings = std::make_shared<DB::CoordinationSettings>();
    auto keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_context->setLocalLogsPreprocessed();
    keeper_context->setRocksDBOptions();
    keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk", snap_dir.path));

    DB::KeeperMemoryStorage storage(500, "", keeper_context);
    addNode(storage, "/legacy", "old_data");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 3;

    // Write as V7 (legacy ZSTD).
    DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
        &storage, /*up_to_log_idx=*/3, /*cluster_config=*/nullptr, DB::SnapshotVersion::V7);
    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr(3, keeper_context, /*compress_zstd=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // Must NOT start with CKFS magic — it's a ZSTD-compressed legacy snapshot.
    ASSERT_GE(buf->size(), 4u);
    EXPECT_NE(memcmp(buf->data_begin(), "CKFS", 4), 0) << "V7 snapshot must not have CKFS magic";

    // 3-way detection should route to legacy ZSTD path.
    auto result = mgr.deserializeSnapshotFromBuffer(buf, /*load_full_storage=*/true);
    ASSERT_NE(result.storage, nullptr);
    ASSERT_NE(result.storage->container.find("/legacy"), result.storage->container.end());
}

// ─── Chunked snapshot validation / corruption tests ──────────────────────────────────────────────

/// Chunked snapshot round-trip produces semantically identical storage to V7 (same format for individual
/// nodes; only the chunked framing differs). Verifies the reader is not merely self-consistent with the
/// writer by accident — both formats yield identical results.
TEST(KeeperChunkedSnapshotValidation, ChunkedEqualsV7)
{
    ChangelogDirTest snap_dir("./chunked_val_eq_v7");

    auto settings = std::make_shared<DB::CoordinationSettings>();
    auto keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_context->setLocalLogsPreprocessed();
    keeper_context->setRocksDBOptions();
    keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk", snap_dir.path));

    // Populate a non-trivial storage: root + 3 children, one ephemeral.
    DB::KeeperMemoryStorage storage(500, "", keeper_context);
    addNode(storage, "/a",   "data_a");
    addNode(storage, "/b",   "data_b");
    addNode(storage, "/a/c", "data_c");
    addNode(storage, "/eph", "eph_data", /*ephemeral_owner=*/77);
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 15;
    storage.session_id_counter = 200;
    storage.committed_ephemerals[77].insert("/eph");
    ++storage.committed_ephemeral_nodes;
    storage.addSessionID(77, 60000);

    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr(3, keeper_context, /*compress_zstd=*/true);

    // Serialize V7 and V8 in separate scopes: KeeperStorageSnapshot enables snapshot mode in its
    // constructor and disables it in its destructor (chassert(!snapshot_mode) on entry).
    // Both snapshots use the same `storage`, so one must be fully destroyed before the next is
    // constructed — otherwise the second constructor trips the chassert in Debug/ASan builds.
    nuraft::ptr<nuraft::buffer> buf7, buf8;
    {
        DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap7(
            &storage, /*up_to_log_idx=*/15, /*cluster_config=*/nullptr, DB::SnapshotVersion::V7);
        buf7 = mgr.serializeSnapshotToBuffer(snap7);
    } // snap7 destroyed here → disableSnapshotMode(); snapshot_mode = false
    ASSERT_NE(buf7, nullptr);

    {
        DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap8(
            &storage, /*up_to_log_idx=*/15, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);
        buf8 = mgr.serializeSnapshotToBuffer(snap8);
    } // snap8 destroyed here → disableSnapshotMode(); snapshot_mode = false
    ASSERT_NE(buf8, nullptr);

    auto res7 = mgr.deserializeSnapshotFromBuffer(buf7, /*load_full_storage=*/true);
    ASSERT_NE(res7.storage, nullptr);
    auto res8 = mgr.deserializeSnapshotFromBuffer(buf8, /*load_full_storage=*/true);
    ASSERT_NE(res8.storage, nullptr);

    // Both must have the same node count.
    EXPECT_EQ(res7.storage->container.size(), res8.storage->container.size())
        << "V7 and chunked round-trips must produce the same number of nodes";

    // Spot-check key nodes.
    for (const char * path : {"/", "/a", "/b", "/a/c", "/eph"})
    {
        auto it7 = res7.storage->container.find(path);
        auto it8 = res8.storage->container.find(path);
        ASSERT_NE(it7, res7.storage->container.end()) << "V7 missing " << path;
        ASSERT_NE(it8, res8.storage->container.end()) << "Chunked missing " << path;
        EXPECT_EQ(it7->value.getData(), it8->value.getData())
            << "Data mismatch for " << path;
        EXPECT_EQ(it7->value.acl_id, it8->value.acl_id)
            << "acl_id mismatch for " << path;
        EXPECT_EQ(it7->value.numChildren(), it8->value.numChildren())
            << "numChildren mismatch for " << path;
    }

    // Ephemeral ownership must be preserved.
    EXPECT_EQ(res7.storage->committed_ephemeral_nodes, res8.storage->committed_ephemeral_nodes);
    auto eit7 = res7.storage->committed_ephemerals.find(77);
    auto eit8 = res8.storage->committed_ephemerals.find(77);
    ASSERT_NE(eit7, res7.storage->committed_ephemerals.end());
    ASSERT_NE(eit8, res8.storage->committed_ephemerals.end());
    EXPECT_EQ(eit7->second.count("/eph"), eit8->second.count("/eph"));

    // Session counter.
    EXPECT_EQ(res7.storage->session_id_counter, res8.storage->session_id_counter);

    // Both V7 and chunked format preserve the same embedded nodes_digest.
    // (The source storage has nodes_digest=0 since addNode bypasses commits; the serializer
    //  embeds that value and both loaders read it back consistently.)
    EXPECT_EQ(res7.storage->nodes_digest, res8.storage->nodes_digest)
        << "V7 and chunked round-trips must preserve the same nodes_digest";
}

/// After the F1 fix, deserializeSnapshotMetadataFromBuffer on a chunked snapshot must decompress
/// ONLY the METADATA chunk. Verify: (a) it returns correct metadata even when NODES chunks
/// are deliberately corrupt; (b) the full deserializer correctly detects the same corruption.
TEST(KeeperChunkedSnapshotValidation, MetadataOnlyFastPath)
{
    ChangelogDirTest snap_dir("./chunked_val_metafast");

    auto settings = std::make_shared<DB::CoordinationSettings>();
    auto keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_context->setLocalLogsPreprocessed();
    keeper_context->setRocksDBOptions();
    keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk", snap_dir.path));

    DB::KeeperMemoryStorage storage(500, "", keeper_context);
    addNode(storage, "/node1", "v1");
    addNode(storage, "/node2", "v2");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 99;

    DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
        &storage, /*up_to_log_idx=*/99, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);
    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr(3, keeper_context, /*compress_zstd=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // Locate the first NODES chunk and corrupt its middle bytes.
    const char * raw_const = reinterpret_cast<const char *>(buf->data_begin());
    const size_t buf_size = buf->size();
    auto frames = parseAndValidateChunkedSnapshotHeader(raw_const, buf_size);
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
    EXPECT_EQ(meta->get_last_log_idx(), 99u)
        << "Metadata-only path must return correct log index despite corrupt NODES chunk";

    // Full deserializer MUST detect the corruption.
    EXPECT_THROW(mgr.deserializeSnapshotFromBuffer(buf, /*load_full_storage=*/true), DB::Exception)
        << "Full deserializer must throw on NODES chunk corruption";
}

/// Corrupt the chunked snapshot header version byte (position 4 in the buffer) to a value != 8.
/// deserializeSnapshotFromBuffer must throw (UNKNOWN_FORMAT_VERSION from
/// parseAndValidateChunkedSnapshotHeader) before touching any chunk data.
TEST(KeeperChunkedSnapshotValidation, WrongHeaderVersionRejected)
{
    ChangelogDirTest snap_dir("./chunked_val_hdrver");

    auto settings = std::make_shared<DB::CoordinationSettings>();
    auto keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_context->setLocalLogsPreprocessed();
    keeper_context->setRocksDBOptions();
    keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk", snap_dir.path));

    DB::KeeperMemoryStorage storage(500, "", keeper_context);
    addNode(storage, "/z", "z_data");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 1;

    DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
        &storage, /*up_to_log_idx=*/1, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);
    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr(3, keeper_context, /*compress_zstd=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);
    ASSERT_GE(buf->size(), 5u);

    // Position 4 is the version byte in the chunked snapshot header (after 4 magic bytes).
    // Change it from 8 to 9 to simulate an unknown future version.
    auto * raw_mut = reinterpret_cast<char *>(buf->data_begin());
    ASSERT_EQ(static_cast<uint8_t>(raw_mut[4]), 8u) << "Expected version byte 8 at position 4";
    raw_mut[4] = 9;

    // Both public entry points must reject this.
    EXPECT_THROW(mgr.deserializeSnapshotFromBuffer(buf, true), DB::Exception)
        << "deserializeSnapshotFromBuffer must throw on unknown chunked snapshot header version";
    EXPECT_THROW(mgr.deserializeSnapshotMetadataFromBuffer(buf), DB::Exception)
        << "deserializeSnapshotMetadataFromBuffer must throw on unknown chunked snapshot header version";
}

/// Corrupt the middle of a NODES chunk compressed data. The ZSTD checksum embedded by
/// serializeChunkedSnapshot must detect the corruption during decompression and throw CORRUPTED_DATA.
TEST(KeeperChunkedSnapshotValidation, CorruptNodeFrameRejected)
{
    ChangelogDirTest snap_dir("./chunked_val_corrupt");

    auto settings = std::make_shared<DB::CoordinationSettings>();
    auto keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_context->setLocalLogsPreprocessed();
    keeper_context->setRocksDBOptions();
    keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk", snap_dir.path));

    DB::KeeperMemoryStorage storage(500, "", keeper_context);
    addNode(storage, "/n1", "v1");
    addNode(storage, "/n2", "v2");
    addNode(storage, "/n3", "v3");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 5;

    DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
        &storage, /*up_to_log_idx=*/5, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);
    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr(3, keeper_context, /*compress_zstd=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    const char * raw_const = reinterpret_cast<const char *>(buf->data_begin());
    const size_t buf_size = buf->size();
    auto frames = parseAndValidateChunkedSnapshotHeader(raw_const, buf_size);

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

/// Chunked snapshot path-only mode (load_full_storage=false) must not attempt to finalize storage.
/// After the F1 metadata-fast-path fix, the paths-only call should also be cheap.
/// This test verifies paths collected in analyzer mode don't accidentally load nodes.
TEST(KeeperChunkedSnapshotValidation, PathsOnlyDoesNotFinalize)
{
    ChangelogDirTest snap_dir("./chunked_val_pathsonly");

    auto settings = std::make_shared<DB::CoordinationSettings>();
    auto keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_context->setLocalLogsPreprocessed();
    keeper_context->setRocksDBOptions();
    keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk", snap_dir.path));

    DB::KeeperMemoryStorage storage(500, "", keeper_context);
    addNode(storage, "/dir",        "d");
    addNode(storage, "/dir/child1", "c1");
    addNode(storage, "/dir/child2", "c2");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 3;

    DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
        &storage, /*up_to_log_idx=*/3, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);
    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr(3, keeper_context, /*compress_zstd=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    auto result = mgr.deserializeSnapshotFromBuffer(buf, /*load_full_storage=*/false);

    // storage must be null or empty — analyzer mode must not build the map.
    EXPECT_TRUE(result.storage == nullptr || result.storage->container.size() == 0)
        << "Paths-only mode must not build the full storage";

    // All snapshot paths (including root) must appear in result.paths.
    const auto & paths = result.paths;
    EXPECT_NE(std::find(paths.begin(), paths.end(), "/"), paths.end())      << "/ missing";
    EXPECT_NE(std::find(paths.begin(), paths.end(), "/dir"), paths.end())   << "/dir missing";
    EXPECT_NE(std::find(paths.begin(), paths.end(), "/dir/child1"), paths.end()) << "/dir/child1 missing";
    EXPECT_NE(std::find(paths.begin(), paths.end(), "/dir/child2"), paths.end()) << "/dir/child2 missing";
}

// ── Chunked snapshot reader-level rejection tests ────────────────────────────
//
// These helpers let tests construct semantically invalid chunked NODES chunks and
// splice them into otherwise-valid chunked buffers (valid METADATA + SESSIONS from
// the real serialiser, replaced NODES).  Each test verifies that the reader
// rejects the tampered buffer with CORRUPTED_DATA.

/// One synthetic ZooKeeper node entry in the V7/chunked on-disk encoding.
/// num_children is int32_t so callers can pass -1 to exercise the parse-time
/// rejection in readChunkedSnapshotNode.
struct FakeNodeEntry
{
    std::string path      = {};
    std::string data      = {};
    uint32_t acl_id       = 0;
    int64_t  czxid        = 0;
    int64_t  mzxid        = 0;
    int64_t  ctime        = 0;
    int64_t  mtime        = 0;
    int32_t  version      = 0;
    int32_t  cversion     = 0;
    int32_t  aversion     = 0;
    int64_t  ephemeral_owner = 0;
    int32_t  num_children = 0;  ///< may be -1 for test 7c
    int64_t  pzxid        = 0;
    int64_t  seq_num      = 0;
};

/// Build uncompressed bytes for a chunked NODES chunk body.
/// Layout: node_count × [path_binary | V7-node-fields]   (NO in-body node_count prefix — it lives in the header descriptor).
/// Field order matches writeNode(V7) + readChunkedSnapshotNode exactly.
static std::string buildNodesChunkBytes(const std::vector<FakeNodeEntry> & entries)
{
    WriteBufferFromOwnString w;

    for (const auto & e : entries)
    {
        writeBinary(e.path, w);                     // VarUInt(len) + bytes

        // V7/chunked node encoding (matches writeNode(V7) and readChunkedSnapshotNode):
        writeVarUInt(e.data.size(), w);             // VarUInt data_size
        if (!e.data.empty())
            w.write(e.data.data(), e.data.size());

        writeBinary(e.acl_id, w);                   // uint32_t (V7+ layout)
        writeBinary(e.czxid, w);
        writeBinary(e.mzxid, w);
        writeBinary(e.ctime, w);
        writeBinary(e.mtime, w);
        writeBinary(e.version, w);
        writeBinary(e.cversion, w);
        writeBinary(e.aversion, w);
        writeBinary(e.ephemeral_owner, w);
        writeBinary(e.num_children, w);             // int32_t (can be -1)
        writeBinary(e.pzxid, w);
        writeBinary(e.seq_num, w);                  // int64_t (V7+ layout)
    }

    return w.str();
}

/// Decompress and return the uncompressed content of chunks[chunk_idx] in a chunked snapshot buffer.
static std::string decompressSnapshotChunk(nuraft::ptr<nuraft::buffer> buf, size_t chunk_idx)
{
    const char * raw = reinterpret_cast<const char *>(buf->data_begin());
    auto frames = parseAndValidateChunkedSnapshotHeader(raw, buf->size());
    if (chunk_idx >= frames.size())
        throw std::runtime_error("decompressSnapshotChunk: chunk_idx out of range");
    const SnapshotChunkDescriptor & fd = frames[chunk_idx];
    const char * src = raw + fd.compressed_offset;
    const size_t src_size = static_cast<size_t>(fd.compressed_size);
    const unsigned long long dsize = ZSTD_getFrameContentSize(src, src_size);
    if (dsize == ZSTD_CONTENTSIZE_ERROR || dsize == ZSTD_CONTENTSIZE_UNKNOWN)
        throw std::runtime_error("decompressSnapshotChunk: cannot determine content size");
    std::string out(static_cast<size_t>(dsize), '\0');
    const size_t actual = ZSTD_decompress(out.data(), dsize, src, src_size);
    if (ZSTD_isError(actual))
        throw std::runtime_error(
            std::string("decompressSnapshotChunk: decompress failed: ") + ZSTD_getErrorName(actual));
    return out;
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
    const size_t raw_size = orig_buf->size();

    auto frames = parseAndValidateChunkedSnapshotHeader(raw, raw_size);

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
    ZSTD_CCtx * cctx = ZSTD_createCCtx();
    if (!cctx) throw std::runtime_error("replaceFirstChunkOfType: ZSTD_createCCtx failed");
    SCOPE_EXIT({ ZSTD_freeCCtx(cctx); });
    ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, 3);
    ZSTD_CCtx_setParameter(cctx, ZSTD_c_checksumFlag, 1);

    const size_t max_cs = ZSTD_compressBound(new_frame_bytes.size());
    std::string new_compressed(max_cs, '\0');
    const size_t new_cs = ZSTD_compress2(
        cctx,
        new_compressed.data(), max_cs,
        new_frame_bytes.data(), new_frame_bytes.size());
    if (ZSTD_isError(new_cs))
        throw std::runtime_error(
            std::string("replaceFirstChunkOfType: ZSTD compress failed: ")
            + ZSTD_getErrorName(new_cs));

    // Compute new frame descriptors with updated offsets.
    const size_t header_sz = chunkedSnapshotHeaderSize(static_cast<uint64_t>(frames.size()));
    std::vector<SnapshotChunkDescriptor> new_descs;
    new_descs.reserve(frames.size());
    size_t total_sz = header_sz;
    for (size_t i = 0; i < frames.size(); ++i)
    {
        const size_t payload_sz = (i == target_idx) ? new_cs : frames[i].compressed_size;
        const uint64_t nc = (i == target_idx) ? node_count_for_replaced : frames[i].node_count;
        new_descs.push_back(SnapshotChunkDescriptor{
            frames[i].type,
            static_cast<uint64_t>(total_sz),
            static_cast<uint64_t>(payload_sz),
            nc});
        total_sz += payload_sz;
    }

    // Allocate and fill the new buffer.
    auto new_buf = nuraft::buffer::alloc(total_sz);
    char * dst = reinterpret_cast<char *>(new_buf->data_begin());

    packChunkedSnapshotHeader(std::span<const SnapshotChunkDescriptor>(new_descs), dst);

    size_t pos = header_sz;
    for (size_t i = 0; i < frames.size(); ++i)
    {
        if (i == target_idx)
        {
            memcpy(dst + pos, new_compressed.data(), new_cs);
            pos += new_cs;
        }
        else
        {
            memcpy(dst + pos, raw + frames[i].compressed_offset, frames[i].compressed_size);
            pos += frames[i].compressed_size;
        }
    }
    return new_buf;
}

/// Convenience wrapper — replace the first NODES chunk (most common test operation).
/// entries is passed to carry the node_count for the descriptor; new_nodes_bytes is the uncompressed body.
static nuraft::ptr<nuraft::buffer> replaceFirstNodesChunk(
    nuraft::ptr<nuraft::buffer> orig_buf,
    const std::string & new_nodes_bytes,
    const std::vector<FakeNodeEntry> & entries)
{
    return replaceFirstChunkOfType(
        orig_buf, SnapshotChunkType::NODES, new_nodes_bytes,
        static_cast<uint64_t>(entries.size()));
}

// ── Rejection tests ──────────────────────────────────────────────────────────

/// NODES chunk with node_count == 0 → no root → CORRUPTED_DATA
/// ("Chunked snapshot has no root '/' node").
TEST(KeeperChunkedSnapshotValidation, EmptyNodesFrameNoRoot)
{
    ChangelogDirTest snap_dir("./chunked_val_emptyroot");

    auto settings     = std::make_shared<DB::CoordinationSettings>();
    auto keeper_ctx   = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_ctx->setLocalLogsPreprocessed();
    keeper_ctx->setRocksDBOptions();
    keeper_ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk", snap_dir.path));

    DB::KeeperMemoryStorage storage(500, "", keeper_ctx);
    addNode(storage, "/n1", "v1");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 1;

    DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
        &storage, /*up_to_log_idx=*/1, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);
    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr(3, keeper_ctx, /*compress_zstd=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // Replace NODES with an empty frame (node_count=0, no nodes at all).
    // Deserialization must reject: no '/' root in container.
    const std::vector<FakeNodeEntry> empty_entries;
    auto tampered = replaceFirstNodesChunk(buf, buildNodesChunkBytes(empty_entries), empty_entries);

    EXPECT_THROW(
        mgr.deserializeSnapshotFromBuffer(tampered, /*load_full_storage=*/true),
        DB::Exception)
        << "Empty NODES frame must be rejected (no '/' root)";
}

/// NODES frame has '/' and '/a/b' but is missing '/a'.
/// Loading must throw CORRUPTED_DATA because the parent '/a' is absent from the container.
TEST(KeeperChunkedSnapshotValidation, MissingParentInNodesFrame)
{
    ChangelogDirTest snap_dir("./chunked_val_missingparent");

    auto settings   = std::make_shared<DB::CoordinationSettings>();
    auto keeper_ctx = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_ctx->setLocalLogsPreprocessed();
    keeper_ctx->setRocksDBOptions();
    keeper_ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk", snap_dir.path));

    DB::KeeperMemoryStorage storage(500, "", keeper_ctx);
    addNode(storage, "/a",   "va");
    addNode(storage, "/a/b", "vb");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 2;

    DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
        &storage, /*up_to_log_idx=*/2, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);
    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr(3, keeper_ctx, /*compress_zstd=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // Replace NODES with '/' + '/a/b', deliberately omitting '/a'.
    // When loading '/a/b', the parent '/a' is not found → CORRUPTED_DATA.
    std::vector<FakeNodeEntry> nodes;
    nodes.push_back(FakeNodeEntry{.path = "/"});     // root present
    nodes.push_back(FakeNodeEntry{.path = "/a/b"});  // child present but parent '/a' absent
    auto tampered = replaceFirstNodesChunk(buf, buildNodesChunkBytes(nodes), nodes);

    EXPECT_THROW(
        mgr.deserializeSnapshotFromBuffer(tampered, /*load_full_storage=*/true),
        DB::Exception)
        << "Missing parent '/a' must be rejected by updateValueForLoad";
}

/// A node whose declared numChildren is 0 but that has one actual child.
/// Loading must reject because children.size() > numChildren().
TEST(KeeperChunkedSnapshotValidation, NumChildrenOvercount)
{
    ChangelogDirTest snap_dir("./chunked_val_overcount");

    auto settings   = std::make_shared<DB::CoordinationSettings>();
    auto keeper_ctx = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_ctx->setLocalLogsPreprocessed();
    keeper_ctx->setRocksDBOptions();
    keeper_ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk", snap_dir.path));

    DB::KeeperMemoryStorage storage(500, "", keeper_ctx);
    addNode(storage, "/a", "v");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 1;

    DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
        &storage, /*up_to_log_idx=*/1, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);
    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr(3, keeper_ctx, /*compress_zstd=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // '/' declares numChildren=0 but '/a' is a real child.
    // addChild("a") → children.size()==1 > numChildren()==0 → CORRUPTED_DATA.
    FakeNodeEntry root;
    root.path         = "/";
    root.num_children = 0;   // under-declares: actual child '/a' will exceed this

    FakeNodeEntry child_a;
    child_a.path         = "/a";
    child_a.num_children = 0;

    const std::vector<FakeNodeEntry> entries_over{root, child_a};
    auto tampered = replaceFirstNodesChunk(buf, buildNodesChunkBytes(entries_over), entries_over);

    EXPECT_THROW(
        mgr.deserializeSnapshotFromBuffer(tampered, /*load_full_storage=*/true),
        DB::Exception)
        << "numChildren=0 with one actual child must be rejected (children.size() > numChildren())";
}

/// A node whose declared numChildren is 5 but that has only one actual child.
/// The post-load equality check must reject: out_non_root (1) != out_total_children (5).
TEST(KeeperChunkedSnapshotValidation, NumChildrenUndercount)
{
    ChangelogDirTest snap_dir("./chunked_val_undercount");

    auto settings   = std::make_shared<DB::CoordinationSettings>();
    auto keeper_ctx = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_ctx->setLocalLogsPreprocessed();
    keeper_ctx->setRocksDBOptions();
    keeper_ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk", snap_dir.path));

    DB::KeeperMemoryStorage storage(500, "", keeper_ctx);
    addNode(storage, "/a", "v");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 1;

    DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
        &storage, /*up_to_log_idx=*/1, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);
    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr(3, keeper_ctx, /*compress_zstd=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // '/' over-declares numChildren=5 but only '/a' exists (1 non-root node).
    // The per-child check passes (1 ≤ 5); the post-load equality check rejects: out_non_root(1) != out_total_children(5).
    FakeNodeEntry root;
    root.path         = "/";
    root.num_children = 5;   // over-declares: only one actual child

    FakeNodeEntry child_a;
    child_a.path         = "/a";
    child_a.num_children = 0;

    const std::vector<FakeNodeEntry> entries_under{root, child_a};
    auto tampered = replaceFirstNodesChunk(buf, buildNodesChunkBytes(entries_under), entries_under);

    EXPECT_THROW(
        mgr.deserializeSnapshotFromBuffer(tampered, /*load_full_storage=*/true),
        DB::Exception)
        << "numChildren=5 with only 1 actual child must be rejected (out_non_root != out_total_children)";
}

/// (7c) A node whose raw num_children field is -1.
/// readChunkedSnapshotNode must reject before finalizeMemorySnapshotLoad is ever reached.
TEST(KeeperChunkedSnapshotValidation, NegativeNumChildrenRejected)
{
    ChangelogDirTest snap_dir("./chunked_val_negchildren");

    auto settings   = std::make_shared<DB::CoordinationSettings>();
    auto keeper_ctx = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_ctx->setLocalLogsPreprocessed();
    keeper_ctx->setRocksDBOptions();
    keeper_ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk", snap_dir.path));

    DB::KeeperMemoryStorage storage(500, "", keeper_ctx);
    addNode(storage, "/n1", "v1");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 1;

    DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
        &storage, /*up_to_log_idx=*/1, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);
    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr(3, keeper_ctx, /*compress_zstd=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // Root node has num_children = -1.
    // readChunkedSnapshotNode must throw CORRUPTED_DATA before finalize is reached.
    FakeNodeEntry root;
    root.path         = "/";
    root.num_children = -1;  // invalid — must be rejected by readChunkedSnapshotNode

    const std::vector<FakeNodeEntry> entries_neg{root};
    auto tampered = replaceFirstNodesChunk(buf, buildNodesChunkBytes(entries_neg), entries_neg);

    EXPECT_THROW(
        mgr.deserializeSnapshotFromBuffer(tampered, /*load_full_storage=*/true),
        DB::Exception)
        << "Negative num_children must be rejected by readChunkedSnapshotNode";
}

/// Append a junk byte after the ACL map in the METADATA chunk.  The metadata-only
/// fast path (deserializeSnapshotMetadataFromBuffer) must throw CORRUPTED_DATA —
/// verifying the F1-metadata-only-chunk-tail-unvalidated fix.
TEST(KeeperChunkedSnapshotValidation, MetadataTailTamperedRejected)
{
    ChangelogDirTest snap_dir("./chunked_val_metatail");

    auto settings = std::make_shared<DB::CoordinationSettings>();
    auto keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_context->setLocalLogsPreprocessed();
    keeper_context->setRocksDBOptions();
    keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk", snap_dir.path));

    DB::KeeperMemoryStorage storage(500, "", keeper_context);
    addNode(storage, "/node1", "v1");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 10;

    DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
        &storage, /*up_to_log_idx=*/10, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);
    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr(3, keeper_context, /*compress_zstd=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // Decompress METADATA chunk (always chunks[0]) and append a junk byte after the ACL map.
    std::string meta_bytes = decompressSnapshotChunk(buf, 0);
    meta_bytes += '\xFF'; // trailing garbage

    // Replace the METADATA chunk with the tampered version.
    auto tampered = replaceFirstChunkOfType(buf, SnapshotChunkType::METADATA, meta_bytes);

    // The metadata-only fast path must detect the trailing byte (F1 fix).
    EXPECT_THROW(mgr.deserializeSnapshotMetadataFromBuffer(tampered), DB::Exception)
        << "deserializeSnapshotMetadataFromBuffer must throw on trailing bytes in METADATA chunk";

    // The full deserialiser must also detect it.
    EXPECT_THROW(mgr.deserializeSnapshotFromBuffer(tampered, /*load_full_storage=*/true), DB::Exception)
        << "deserializeSnapshotFromBuffer must throw on trailing bytes in METADATA chunk";
}

/// Append a junk byte after the cluster config in the SESSIONS chunk.  The full
/// deserialiser must throw CORRUPTED_DATA — verifying the F2-sessions-chunk-no-eof-drain fix.
/// Verifies the SESSIONS-chunk EOF drain (the second `if (!rbuf.eof())` check, after the
/// optional cluster-config section is fully consumed).
///
/// The snapshot is serialized WITH a cluster config so the SESSIONS chunk contains:
///   active_sessions_size (8 bytes)  +  VarUInt(cluster_config_size)  +  cluster_config_bytes
/// A single trailing garbage byte is then appended AFTER the complete cluster-config bytes,
/// so readVarUInt and readStrict both succeed and the corruption is only detected by the drain
/// check.  This ensures the test exercises that specific code path rather than a readVarUInt
/// EOF error from an incomplete VarUInt sequence.
TEST(KeeperChunkedSnapshotValidation, SessionsTrailingBytesRejected)
{
    ChangelogDirTest snap_dir("./chunked_val_sesseof");

    auto settings = std::make_shared<DB::CoordinationSettings>();
    auto keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
    keeper_context->setLocalLogsPreprocessed();
    keeper_context->setRocksDBOptions();
    keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk", snap_dir.path));

    DB::KeeperMemoryStorage storage(500, "", keeper_context);
    addNode(storage, "/node1", "v1");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 7;

    // Serialize WITH a cluster config so the SESSIONS frame includes a length-prefixed
    // cluster-config block.  The trailing byte is appended AFTER the complete cluster-config
    // so only the drain `if (!rbuf.eof())` fires — not readVarUInt.
    auto cluster_cfg = std::make_shared<nuraft::cluster_config>();
    DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
        &storage, /*up_to_log_idx=*/7, cluster_cfg, DB::SnapshotVersion::V8);
    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr(3, keeper_context, /*compress_zstd=*/true);
    auto buf = mgr.serializeSnapshotToBuffer(snap);
    ASSERT_NE(buf, nullptr);

    // Find the SESSIONS chunk index.
    const char * raw_const = reinterpret_cast<const char *>(buf->data_begin());
    auto frames = parseAndValidateChunkedSnapshotHeader(raw_const, buf->size());
    size_t sess_idx = frames.size();
    for (size_t i = 0; i < frames.size(); ++i)
    {
        if (frames[i].type == SnapshotChunkType::SESSIONS)
        {
            sess_idx = i;
            break;
        }
    }
    ASSERT_LT(sess_idx, frames.size()) << "No SESSIONS chunk found";

    // Decompress the SESSIONS chunk (already contains the complete cluster-config block)
    // and append a single trailing garbage byte AFTER it.
    std::string sess_bytes = decompressSnapshotChunk(buf, sess_idx);
    sess_bytes += '\xAB'; // one byte past the last valid byte of the cluster-config block

    // Replace the SESSIONS chunk with the tampered version.
    auto tampered = replaceFirstChunkOfType(buf, SnapshotChunkType::SESSIONS, sess_bytes);

    // Must throw CORRUPTED_DATA from the drain check (not a generic read error).
    try
    {
        mgr.deserializeSnapshotFromBuffer(tampered, /*load_full_storage=*/true);
        FAIL() << "deserializeSnapshotFromBuffer must throw on trailing bytes in SESSIONS chunk";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::CORRUPTED_DATA)
            << "Expected CORRUPTED_DATA from SESSIONS-chunk drain, got: " << e.message();
    }
}

/// Regression: when a chunked snapshot chunk's compressed_size is shrunk to exclude the
/// 4-byte ZSTD content-checksum trailer, deserialization must throw.
///
/// The streaming ZstdInflatingReadBuffer path can silently accept inner-buffer EOF without
/// checking that ZSTD_decompressStream returned 0 (i.e. the full frame was consumed).
/// The chunked snapshot header only enforces non-overlap between chunks (not contiguity), so
/// 4 dropped checksum bytes become a legal inter-chunk gap — parseAndValidateChunkedSnapshotHeader
/// passes, the ZSTD reader decompresses the payload, and the corruption is invisible.
///
/// The fix adds require_frame_complete=true to ZstdInflatingReadBuffer for chunked snapshot reads:
/// when the inner buffer reaches EOF with ZSTD_decompressStream returning non-zero, we throw.
/// This test verifies that a tampered NODES chunk (serial and parallel paths) and a tampered
/// METADATA chunk are both rejected.
TEST(KeeperChunkedSnapshotValidation, F1ZstdFrameTrailerDropped)
{
    ChangelogDirTest snap_dir("./chunked_val_f1_trailer");

    // chunk_size=1: each node gets its own NODES chunk.
    // With nodes /, /a, /b that gives K=3 NODES chunks → parallel path (K > 1) is reachable.
    auto settings_src = std::make_shared<DB::CoordinationSettings>();
    (*settings_src)[DB::CoordinationSetting::snapshot_chunk_size] = 1;
    auto ctx_src = std::make_shared<DB::KeeperContext>(true, settings_src);
    ctx_src->setLocalLogsPreprocessed();
    ctx_src->setRocksDBOptions();
    ctx_src->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDiskF1Src", snap_dir.path));
    ctx_src->setDigestEnabled(false);

    DB::KeeperMemoryStorage storage_src(500, "", ctx_src);
    addNode(storage_src, "/a", "aaa");
    addNode(storage_src, "/b", "bbb");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage_src.zxid) = 10;
    storage_src.session_id_counter = 50;

    nuraft::ptr<nuraft::buffer> good_buf;
    {
        DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
            &storage_src, /*up_to_log_idx=*/10, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);
        DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr_src(3, ctx_src, /*compress_zstd=*/true);
        good_buf = mgr_src.serializeSnapshotToBuffer(snap);
    }
    ASSERT_NE(good_buf, nullptr);

    // Parse header and verify K ≥ 2 NODES chunks (needed for the parallel path test).
    std::vector<DB::SnapshotChunkDescriptor> frames;
    {
        const char * raw = reinterpret_cast<const char *>(good_buf->data_begin());
        frames = DB::parseAndValidateChunkedSnapshotHeader(raw, good_buf->size());
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

    // Return a copy of good_buf with chunks[chunk_idx].compressed_size shrunk by 4.
    // The 4 bytes are the ZSTD content-checksum epilogue (ZSTD_c_checksumFlag=1).
    // They become an inter-chunk gap — legal under the non-overlap-only header check —
    // so parseAndValidateChunkedSnapshotHeader still passes on the tampered buffer.
    // Chunked snapshot header layout: compressed_size of chunk i is at byte offset 22 + 25*i.
    // (magic=4 + version=1 + chunk_count=8 = 13; then per descriptor: type=1 + offset=8 = 9 more; 13+9=22)
    // Each descriptor is 25 bytes (type=1 + compressed_offset=8 + compressed_size=8 + node_count=8).
    auto makeTrailerDropped = [&](size_t frame_idx) -> nuraft::ptr<nuraft::buffer>
    {
        nuraft::ptr<nuraft::buffer> bad = nuraft::buffer::alloc(good_buf->size());
        std::memcpy(bad->data_begin(), good_buf->data_begin(), good_buf->size());
        char * hdr = reinterpret_cast<char *>(bad->data_begin());
        uint64_t cs = 0;
        std::memcpy(&cs, hdr + 22 + 25 * frame_idx, 8);
        cs -= 4; // drop the 4-byte ZSTD checksum trailer
        std::memcpy(hdr + 22 + 25 * frame_idx, &cs, 8);
        return bad;
    };

    // ── NODES chunk, serial path (deser_threads=1) ──────────────────────────────────────────
    {
        auto bad = makeTrailerDropped(first_nodes_frame_idx);
        auto s = std::make_shared<DB::CoordinationSettings>();
        (*s)[DB::CoordinationSetting::snapshot_deser_threads] = 1;
        auto ctx = std::make_shared<DB::KeeperContext>(true, s);
        ctx->setLocalLogsPreprocessed();
        ctx->setRocksDBOptions();
        ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDiskF1T1", snap_dir.path));
        DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr(3, ctx, /*compress_zstd=*/true);
        EXPECT_THROW(mgr.deserializeSnapshotFromBuffer(bad, /*load_full_storage=*/true), DB::Exception)
            << "NODES chunk: dropped ZSTD trailer must throw (serial, deser_threads=1)";
    }

    // ── NODES chunk, parallel path (deser_threads=8, K=3 → pool dispatch) ──────────────────
    {
        auto bad = makeTrailerDropped(first_nodes_frame_idx);
        auto s = std::make_shared<DB::CoordinationSettings>();
        (*s)[DB::CoordinationSetting::snapshot_deser_threads] = 8;
        auto ctx = std::make_shared<DB::KeeperContext>(true, s);
        ctx->setLocalLogsPreprocessed();
        ctx->setRocksDBOptions();
        ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDiskF1T8", snap_dir.path));
        DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr(3, ctx, /*compress_zstd=*/true);
        EXPECT_THROW(mgr.deserializeSnapshotFromBuffer(bad, /*load_full_storage=*/true), DB::Exception)
            << "NODES chunk: dropped ZSTD trailer must throw (parallel, deser_threads=8)";
    }

    // ── METADATA chunk (always read serially regardless of deser_threads) ───────────────────
    {
        auto bad = makeTrailerDropped(0); // chunks[0] is always METADATA
        auto s = std::make_shared<DB::CoordinationSettings>();
        (*s)[DB::CoordinationSetting::snapshot_deser_threads] = 1;
        auto ctx = std::make_shared<DB::KeeperContext>(true, s);
        ctx->setLocalLogsPreprocessed();
        ctx->setRocksDBOptions();
        ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDiskF1Meta", snap_dir.path));
        DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr(3, ctx, /*compress_zstd=*/true);
        EXPECT_THROW(mgr.deserializeSnapshotFromBuffer(bad, /*load_full_storage=*/true), DB::Exception)
            << "METADATA chunk: dropped ZSTD trailer must throw";
    }
}

/// ── Digest recalculation and parallel load determinism ────────────────────────────────────────

/// Verify that loading a chunked snapshot that has NO_DIGEST in its METADATA chunk
/// (serialized with digestEnabled=false) under a digest-enabled KeeperContext
/// correctly recalculates nodes_digest from the node data (the recalculate_digest path).
TEST(KeeperChunkedSnapshotParallel, DigestRecalculationOnLoad)
{
    ChangelogDirTest snap_dir("./chunked_digest_recalc");

    // 1. Serialize with digest DISABLED → NO_DIGEST in METADATA chunk.
    auto src_settings = std::make_shared<DB::CoordinationSettings>();
    auto src_ctx = std::make_shared<DB::KeeperContext>(true, src_settings);
    src_ctx->setLocalLogsPreprocessed();
    src_ctx->setRocksDBOptions();
    src_ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDiskND", snap_dir.path));
    src_ctx->setDigestEnabled(false);

    DB::KeeperMemoryStorage storage_src(500, "", src_ctx);
    addNode(storage_src, "/alpha", "val_alpha");
    addNode(storage_src, "/beta",  "val_beta");
    addNode(storage_src, "/alpha/child", "val_child");
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage_src.zxid) = 42;
    storage_src.session_id_counter = 7;

    nuraft::ptr<nuraft::buffer> buf_no_digest;
    {
        DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
            &storage_src, /*up_to_log_idx=*/42, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);
        DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr_nd(3, src_ctx, /*compress_zstd=*/true);
        buf_no_digest = mgr_nd.serializeSnapshotToBuffer(snap);
    }
    ASSERT_NE(buf_no_digest, nullptr);

    // 2. Load with digest ENABLED → triggers recalculate_digest path.
    auto load_settings = std::make_shared<DB::CoordinationSettings>();
    auto load_ctx = std::make_shared<DB::KeeperContext>(true, load_settings);
    load_ctx->setLocalLogsPreprocessed();
    load_ctx->setRocksDBOptions();
    load_ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDiskD", snap_dir.path));
    load_ctx->setDigestEnabled(true);

    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr_load(3, load_ctx, /*compress_zstd=*/true);
    auto res = mgr_load.deserializeSnapshotFromBuffer(buf_no_digest, /*load_full_storage=*/true);
    ASSERT_NE(res.storage, nullptr);

    // 3. Load a second time — must produce the same digest (recalculation is deterministic).
    auto res2 = mgr_load.deserializeSnapshotFromBuffer(buf_no_digest, /*load_full_storage=*/true);
    ASSERT_NE(res2.storage, nullptr);

    EXPECT_NE(res.storage->nodes_digest, 0u)
        << "Recalculated nodes_digest must be non-zero for a non-empty chunked snapshot";
    EXPECT_EQ(res.storage->nodes_digest, res2.storage->nodes_digest)
        << "Recalculated nodes_digest must be the same across two loads of the same snapshot";
    EXPECT_EQ(res.storage->container.size(), res2.storage->container.size());
}

/// Load the same multi-chunk chunked snapshot with snapshot_deser_threads=1 (serial) and =8
/// (parallel) and verify: identical node counts, identical data per path, identical digest,
/// identical ephemeral ownership, and byte-identical re-serialization.
TEST(KeeperChunkedSnapshotParallel, ParallelVsSequential)
{
    ChangelogDirTest snap_dir("./chunked_parallel_det");

    // Source context: small chunk size → multiple NODES chunks; digest disabled → NO_DIGEST
    // so that the recalculate_digest path exercises the parallel path as well.
    auto src_settings = std::make_shared<DB::CoordinationSettings>();
    (*src_settings)[DB::CoordinationSetting::snapshot_chunk_size] = 4;
    auto src_ctx = std::make_shared<DB::KeeperContext>(true, src_settings);
    src_ctx->setLocalLogsPreprocessed();
    src_ctx->setRocksDBOptions();
    src_ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDiskSrc", snap_dir.path));
    src_ctx->setDigestEnabled(false);

    // Build a storage with enough nodes to span ≥ 2 NODES chunks (chunk=4 → ceil(N/4) chunks).
    DB::KeeperMemoryStorage storage_src(500, "", src_ctx);
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
        DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
            &storage_src, /*up_to_log_idx=*/50, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);
        DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr_src(3, src_ctx, /*compress_zstd=*/true);
        source_buf = mgr_src.serializeSnapshotToBuffer(snap);
    }
    ASSERT_NE(source_buf, nullptr);

    // Verify we actually got multiple NODES chunks (otherwise the parallel path is not tested).
    {
        const char * raw = reinterpret_cast<const char *>(source_buf->data_begin());
        auto frames = parseAndValidateChunkedSnapshotHeader(raw, source_buf->size());
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
    ctx1->setRocksDBOptions();
    ctx1->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk1", snap_dir.path));
    ctx1->setDigestEnabled(true); // recalculate_digest=true since snapshot has NO_DIGEST
    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr1(3, ctx1, /*compress_zstd=*/true);
    auto res1 = mgr1.deserializeSnapshotFromBuffer(source_buf, /*load_full_storage=*/true);
    ASSERT_NE(res1.storage, nullptr);

    // Load with parallel path (threads=8).
    auto settings8 = std::make_shared<DB::CoordinationSettings>();
    (*settings8)[DB::CoordinationSetting::snapshot_deser_threads] = 8;
    auto ctx8 = std::make_shared<DB::KeeperContext>(true, settings8);
    ctx8->setLocalLogsPreprocessed();
    ctx8->setRocksDBOptions();
    ctx8->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapDisk8", snap_dir.path));
    ctx8->setDigestEnabled(true); // recalculate_digest=true since snapshot has NO_DIGEST
    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> mgr8(3, ctx8, /*compress_zstd=*/true);
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
    EXPECT_NE(res1.storage->nodes_digest, 0u)
        << "Recalculated digest must be non-zero";
    EXPECT_EQ(res1.storage->nodes_digest, res8.storage->nodes_digest)
        << "Serial and parallel digests must match";

    // Ephemeral ownership must be preserved identically.
    EXPECT_EQ(res1.storage->committed_ephemeral_nodes, res8.storage->committed_ephemeral_nodes);
    {
        auto eph1 = res1.storage->committed_ephemerals.find(100);
        auto eph8 = res8.storage->committed_ephemerals.find(100);
        ASSERT_NE(eph1, res1.storage->committed_ephemerals.end()) << "Owner 100 missing in serial";
        ASSERT_NE(eph8, res8.storage->committed_ephemerals.end()) << "Owner 100 missing in parallel";
        EXPECT_EQ(eph1->second, eph8->second) << "Ephemeral path sets must match";
    }

    // Re-serialize both and verify byte-identical output (deterministic chunk-order splice).
    nuraft::ptr<nuraft::buffer> rebuf1, rebuf8;
    {
        DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
            res1.storage.get(), /*up_to_log_idx=*/50, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);
        rebuf1 = mgr1.serializeSnapshotToBuffer(snap);
    }
    ASSERT_NE(rebuf1, nullptr);
    {
        DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
            res8.storage.get(), /*up_to_log_idx=*/50, /*cluster_config=*/nullptr, DB::SnapshotVersion::V8);
        rebuf8 = mgr8.serializeSnapshotToBuffer(snap);
    }
    ASSERT_NE(rebuf8, nullptr);

    ASSERT_EQ(rebuf1->size(), rebuf8->size())
        << "Re-serialized buffers must have identical size";
    EXPECT_EQ(std::memcmp(rebuf1->data_begin(), rebuf8->data_begin(), rebuf1->size()), 0)
        << "Re-serialized buffers must be byte-identical (deterministic chunk order)";
}

/// Follower `apply_snapshot` of a chunked snapshot buffer replaces committed state and
/// correctly restores ephemerals, ACL usage, and nodes_digest.
///
/// Uses `snapshot_chunk_size=2` to force multiple NODES chunks so the parallel
/// decompression+parse path is exercised inside the storage lock.
/// `snapshot_deser_threads=2` is set explicitly on the receiving state machine so
/// the parallel path runs deterministically regardless of host CPU count.
TEST(KeeperMemorySnapshotApplyTest, ApplyChunkedSnapshotReplacesCommittedState)
{
    ChangelogDirTest snapshots("./chunked_apply_snapshots");
    ChangelogDirTest rocks("./chunked_apply_rocksdb");

    // State machine uses snapshot_deser_threads=2 to force the parallel deserialization
    // path regardless of the CI host's CPU count (default 0=auto may resolve to 1
    // on low-core machines and skip the parallel code path).
    auto sm_settings = std::make_shared<DB::CoordinationSettings>();
    (*sm_settings)[DB::CoordinationSetting::compress_snapshots_with_zstd_format] = true;
    (*sm_settings)[DB::CoordinationSetting::snapshot_deser_threads] = 2;
    auto sm_ctx = std::make_shared<DB::KeeperContext>(true, sm_settings);
    sm_ctx->setLocalLogsPreprocessed();
    sm_ctx->setDigestEnabled(true);
    sm_ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("ChunkedApplySmDisk", "./chunked_apply_snapshots"));
    sm_ctx->setRocksDBDisk(std::make_shared<DB::DiskLocal>("ChunkedApplySmRocksDisk", "./chunked_apply_rocksdb"));
    sm_ctx->setRocksDBOptions();

    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine<DB::KeeperMemoryStorage>>(
        nullptr, snapshots_queue, sm_ctx, nullptr);
    state_machine->init();

    // Commit a node that the chunked snapshot must overwrite.
    auto old_entry = makeCreateEntry(*state_machine, "/old_before_chunked", "stale_data");
    state_machine->pre_commit(1, old_entry->get_buf());
    state_machine->commit(1, old_entry->get_buf());
    ASSERT_TRUE(state_machine->getStorageUnsafe().container.contains("/old_before_chunked"));

    // Build source storage: 5 user nodes + 1 ephemeral, spanning multiple chunks.
    // chunk_size=2 → 6 user nodes (root + /a + /b + /a/c + /eph + /b_acl) → 3 NODES chunks (2+2+2).
    // /b_acl carries a non-zero acl_id to exercise the ACL restoration path.
    auto src_settings = std::make_shared<DB::CoordinationSettings>();
    (*src_settings)[DB::CoordinationSetting::snapshot_chunk_size] = 2;
    auto src_ctx = std::make_shared<DB::KeeperContext>(true, src_settings);
    src_ctx->setLocalLogsPreprocessed();
    src_ctx->setRocksDBOptions();
    src_ctx->setSnapshotDisk(std::make_shared<DB::DiskLocal>("ChunkedApplySrcDisk", "./chunked_apply_snapshots"));
    src_ctx->setDigestEnabled(true);

    DB::KeeperMemoryStorage snap_storage(500, "", src_ctx);

    // Register an ACL in the source storage so it is serialized in the METADATA chunk.
    // acl_id is deterministically 1 (first registration).
    const DB::ACLId test_acl_id = snap_storage.acl_map.convertACLs({{31, "world", "anyone"}});
    snap_storage.acl_map.addUsage(test_acl_id);
    const Coordination::ACLs expected_acl = {{31, "world", "anyone"}};

    addNode(snap_storage, "/a",     "data_a");
    addNode(snap_storage, "/b",     "data_b");
    addNode(snap_storage, "/a/c",   "data_ac");
    addNode(snap_storage, "/eph",   "eph_val", /*ephemeral_owner=*/42);
    addNode(snap_storage, "/b_acl", "acl_data", /*ephemeral_owner=*/0, test_acl_id);
    TSA_SUPPRESS_WARNING_FOR_WRITE(snap_storage.zxid) = 10;
    snap_storage.session_id_counter = 100;
    snap_storage.committed_ephemerals[42].insert("/eph");
    ++snap_storage.committed_ephemeral_nodes;
    snap_storage.addSessionID(42, 30000);

    // Serialize as chunked format using a KeeperSnapshotManager backed by the source context.
    DB::KeeperSnapshotManager<DB::KeeperMemoryStorage> src_mgr(3, src_ctx, /*compress_zstd=*/true);
    nuraft::ptr<nuraft::buffer> chunked_buf;
    {
        DB::KeeperStorageSnapshot<DB::KeeperMemoryStorage> snap(
            &snap_storage, /*up_to_log_idx=*/10, /*cluster_config=*/nullptr,
            DB::SnapshotVersion::V8);
        chunked_buf = src_mgr.serializeSnapshotToBuffer(snap);
    }
    ASSERT_NE(chunked_buf, nullptr);
    // Verify the buffer is actually a chunked snapshot (starts with "CKFS" magic).
    ASSERT_GE(chunked_buf->size(), 4u);
    EXPECT_EQ(std::memcmp(chunked_buf->data_begin(), "CKFS", 4), 0)
        << "Serialized buffer must start with chunked snapshot magic 'CKFS'";

    // Install the chunked snapshot into the state machine via the follower path.
    nuraft::snapshot snapshot_meta(10, 0, std::make_shared<nuraft::cluster_config>());
    saveSingleObjectSnapshot(*state_machine, snapshot_meta, chunked_buf);
    EXPECT_TRUE(state_machine->apply_snapshot(snapshot_meta));

    // ── Post-apply assertions ────────────────────────────────────────────────────────────────
    const auto & storage = state_machine->getStorageUnsafe();

    // Committed state replaced: old node gone, new nodes present.
    EXPECT_FALSE(storage.container.contains("/old_before_chunked"))
        << "Pre-snapshot node must be removed after apply_snapshot";
    ASSERT_TRUE(storage.container.contains("/a"));
    ASSERT_TRUE(storage.container.contains("/b"));
    ASSERT_TRUE(storage.container.contains("/a/c"));
    ASSERT_TRUE(storage.container.contains("/eph"));
    ASSERT_TRUE(storage.container.contains("/b_acl"));
    EXPECT_EQ(std::string(storage.container.getValue("/a").getData()),     "data_a");
    EXPECT_EQ(std::string(storage.container.getValue("/b").getData()),     "data_b");
    EXPECT_EQ(std::string(storage.container.getValue("/a/c").getData()),   "data_ac");
    EXPECT_EQ(std::string(storage.container.getValue("/eph").getData()),   "eph_val");
    EXPECT_EQ(std::string(storage.container.getValue("/b_acl").getData()), "acl_data");

    // Children rebuilt: /a has exactly one child (/a/c).
    EXPECT_EQ(storage.container.getValue("/a").getChildren().size(), 1u)
        << "/a must have exactly one child after chunked apply_snapshot";

    // Ephemeral bookkeeping correct.
    EXPECT_EQ(storage.committed_ephemeral_nodes, 1u)
        << "committed_ephemeral_nodes must be 1 after chunked apply";
    {
        auto eit = storage.committed_ephemerals.find(42);
        ASSERT_NE(eit, storage.committed_ephemerals.end())
            << "Owner 42 must be present in committed_ephemerals";
        EXPECT_EQ(eit->second.count("/eph"), 1u)
            << "/eph must be tracked as an ephemeral owned by session 42";
    }

    // ACL usage correctly restored: the ACL mapping for test_acl_id must be present
    // in the loaded storage's acl_map (serialized in the METADATA chunk, deserialized
    // by the parallel NODES-chunk path that batches acl_usage into acl_map.addUsageBatch).
    {
        const auto & mapping = storage.acl_map.getMapping();
        ASSERT_EQ(mapping.count(test_acl_id), 1u)
            << "ACL id " << test_acl_id << " must be present in acl_map after chunked apply_snapshot";
        EXPECT_EQ(mapping.at(test_acl_id), expected_acl)
            << "ACL for id " << test_acl_id << " must match the original ACL after chunked apply_snapshot";
        EXPECT_EQ(storage.container.getValue("/b_acl").acl_id, test_acl_id)
            << "/b_acl must retain its acl_id after chunked apply_snapshot";
    }

    // Digest must be non-zero (recalculated from node data since digestEnabled=true on load).
    EXPECT_NE(storage.nodes_digest, 0u)
        << "nodes_digest must be non-zero after chunked apply_snapshot with digest enabled";

    // last_commit_index must reflect the snapshot index.
    EXPECT_EQ(state_machine->last_commit_index(), 10u);
}

} // namespace DB

#endif
