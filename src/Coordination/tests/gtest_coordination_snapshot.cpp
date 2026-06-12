#include "config.h"

#if USE_NURAFT
#include <Coordination/tests/gtest_coordination_common.h>

#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/SnapshotableHashTable.h>
#include <Coordination/KeeperStorage.h>

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

#include <limits>
#include <stdexcept>
#include <thread>

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsBool compress_snapshots_with_zstd_format;
    extern const CoordinationSettingsUInt64 snapshot_transfer_chunk_size;
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

    /// (7) one-shot consumption: a second apply of 6 throws (pending was cleared).
    {
        nuraft::snapshot s6(6, 0, std::make_shared<nuraft::cluster_config>());
        EXPECT_THROW(state_machine->apply_snapshot(s6), DB::Exception);
    }

    /// (8) protection moved to 6: save 7 -> remove 2 then 4 -> {5,6,7}; mark 6 + its file survive.
    saveInstallSnapshot(*state_machine, ctx, 7, "/snap7");
    EXPECT_FALSE(fs::exists("./snapshots/snapshot_2.bin.zstd"));
    EXPECT_FALSE(fs::exists("./snapshots/snapshot_4.bin.zstd"));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_6.bin.zstd"));
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 6);
}

/// Applying a snapshot with no preceding save_logical_snp_obj is a logical error (master would
/// have applied on `mark == s.idx`).
TEST(KeeperMemorySnapshotApplyTest, ApplyWithoutPendingSaveThrowsLogicalError)
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
    nuraft::snapshot s1(1, 0, std::make_shared<nuraft::cluster_config>());
    {
        auto info = executeCreateSnapshotTask(*state_machine, snapshots_queue, s1);
        ASSERT_NE(info, nullptr);
    }

    nuraft::snapshot s1_apply(1, 0, std::make_shared<nuraft::cluster_config>());
    EXPECT_THROW(state_machine->apply_snapshot(s1_apply), DB::Exception);
    EXPECT_TRUE(state_machine->getStorageUnsafe().container.contains("/n1"));
}

/// Applying above the saved pending throws and must NOT clear the surviving pending.
TEST(KeeperMemorySnapshotApplyTest, ApplyAboveSavedPendingThrowsAndPreservesPending)
{
    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest rocks("./rocksdb");

    auto ctx = makeMemoryContextForSnapshotApply("./snapshots", "./rocksdb");
    DB::SnapshotsQueue snapshots_queue{1};
    auto state_machine = std::make_shared<DB::KeeperStateMachine<DB::KeeperMemoryStorage>>(nullptr, snapshots_queue, ctx, nullptr);
    state_machine->init();

    saveInstallSnapshot(*state_machine, ctx, 3, "/from_snap3");

    nuraft::snapshot s5(5, 0, std::make_shared<nuraft::cluster_config>());
    EXPECT_THROW(state_machine->apply_snapshot(s5), DB::Exception); /// 5 > pending 3

    /// The non-matching pending (3) survived the throwing exit and can still be applied.
    nuraft::snapshot s3(3, 0, std::make_shared<nuraft::cluster_config>());
    EXPECT_TRUE(state_machine->apply_snapshot(s3));
    EXPECT_TRUE(state_machine->getStorageUnsafe().container.contains("/from_snap3"));
    EXPECT_EQ(state_machine->last_snapshot()->get_last_log_idx(), 3);
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

/// An uncovered displaced apply (s.idx < pending, local commits do NOT cover s.idx) fails closed
/// instead of silently skipping; the displacing pending survives and applies.
TEST(KeeperMemorySnapshotApplyTest, UncoveredDisplacedApplyFailsClosed)
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
    EXPECT_EQ(state_machine->last_commit_index(), 2);

    saveInstallSnapshot(*state_machine, ctx, 3, "/from_snap3"); /// pending 3
    saveInstallSnapshot(*state_machine, ctx, 5, "/from_snap5"); /// pending displaced to 5

    /// 3 < pending 5 and last committed (2) does not cover 3 -> fail closed.
    nuraft::snapshot s3(3, 0, std::make_shared<nuraft::cluster_config>());
    EXPECT_THROW(state_machine->apply_snapshot(s3), DB::Exception);
    EXPECT_TRUE(state_machine->getStorageUnsafe().container.contains("/n1"));
    EXPECT_FALSE(state_machine->getStorageUnsafe().container.contains("/from_snap5"));
    EXPECT_EQ(state_machine->last_commit_index(), 2);

    /// The displacing pending (5) survived and applies (in-process equivalent of post-restart recovery).
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

#endif
