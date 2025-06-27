#include "config.h"

#if USE_NURAFT
#include <Coordination/tests/gtest_coordination_common.h>

#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/SnapshotableHashTable.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperStorage.h>

#include <Common/SipHash.h>

namespace
{

struct IntNode
{
    int value;
    IntNode(int value_) : value(value_) { } /// NOLINT(google-explicit-constructor)
    [[maybe_unused]] UInt64 sizeInBytes() const { return sizeof value; }
    [[maybe_unused]] bool operator==(const int & rhs) const { return value == rhs; }
    [[maybe_unused]] bool operator!=(const int & rhs) const { return rhs != this->value; }
};

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
    map_snp.enableSnapshotMode(100000);
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
    map_snp.disableSnapshotMode();
}

TYPED_TEST(CoordinationTest, SnapshotableHashMapDataSize)
{
    /// int
    DB::SnapshotableHashTable<IntNode> hello;
    hello.disableSnapshotMode();
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

    hello.enableSnapshotMode(10000);
    hello.insert("hello", 1);
    EXPECT_EQ(hello.getApproximateDataSize(), 9);
    hello.updateValue("hello", [](IntNode & value) { value = 2; });
    EXPECT_EQ(hello.getApproximateDataSize(), 18);
    hello.insertOrReplace("hello", 1);
    EXPECT_EQ(hello.getApproximateDataSize(), 27);

    hello.clearOutdatedNodes();
    EXPECT_EQ(hello.getApproximateDataSize(), 9);

    hello.erase("hello");
    EXPECT_EQ(hello.getApproximateDataSize(), 9);

    hello.clearOutdatedNodes();
    EXPECT_EQ(hello.getApproximateDataSize(), 0);

    /// Node
    using Node = DB::KeeperMemoryStorage::Node;
    DB::SnapshotableHashTable<Node> world;
    Node n1;
    n1.setData("1234");
    Node n2;
    n2.setData("123456");
    n2.addChild("");

    /// Note: Below, we check in many cases only that getApproximateDataSize() > 0. This is because
    ///       the SnapshotableHashTable's approximate data size includes Node's sizeInBytes(). The
    ///       latter includes sizeof(absl::flat_hash_set) which is surprisingly not constant across
    ///       different runs. The approximate size is only used for statistics accounting, so this
    ///       should be okay.

    world.disableSnapshotMode();
    world.insert("world", n1);
    EXPECT_GT(world.getApproximateDataSize(), 0);
    world.updateValue("world", [&](Node & value) { value = n2; });
    EXPECT_GT(world.getApproximateDataSize(), 0);

    world.erase("world");
    EXPECT_EQ(world.getApproximateDataSize(), 0);

    world.enableSnapshotMode(100000);
    world.insert("world", n1);
    EXPECT_GT(world.getApproximateDataSize(), 0);
    world.updateValue("world", [&](Node & value) { value = n2; });
    EXPECT_GT(world.getApproximateDataSize(), 0);

    world.clearOutdatedNodes();
    EXPECT_GT(world.getApproximateDataSize(), 0);

    world.erase("world");
    EXPECT_GT(world.getApproximateDataSize(), 0);

    world.clear();
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
    addNode(storage, "/hello1", "world", 1);
    addNode(storage, "/hello2", "somedata", 3);
    storage.session_id_counter = 5;
    TSA_SUPPRESS_WARNING_FOR_WRITE(storage.zxid) = 2;
    storage.committed_ephemerals[3] = {"/hello2"};
    storage.committed_ephemerals[1] = {"/hello1"};
    storage.getSessionID(130);
    storage.getSessionID(130);

    DB::KeeperStorageSnapshot<Storage> snapshot(&storage, 2);

    EXPECT_EQ(snapshot.snapshot_meta->get_last_log_idx(), 2);
    EXPECT_EQ(snapshot.session_id, 7);
    EXPECT_EQ(snapshot.snapshot_container_size, 6);
    EXPECT_EQ(snapshot.session_and_timeout.size(), 2);

    auto buf = manager.serializeSnapshotToBuffer(snapshot);
    manager.serializeSnapshotBufferToDisk(*buf, 2);
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_2.bin" + this->extension));


    auto debuf = manager.deserializeSnapshotBufferFromDisk(2);

    auto [restored_storage, snapshot_meta, _] = manager.deserializeSnapshotFromBuffer(debuf);

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

    DB::KeeperStorageSnapshot<Storage> snapshot(&storage, 50);
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
    auto [restored_storage, meta, _] = manager.deserializeSnapshotFromBuffer(debuf);

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

        DB::KeeperStorageSnapshot<Storage> snapshot(&storage, j * 50);
        auto buf = manager.serializeSnapshotToBuffer(snapshot);
        manager.serializeSnapshotBufferToDisk(*buf, j * 50);
        EXPECT_TRUE(fs::exists(std::string{"./snapshots/snapshot_"} + std::to_string(j * 50) + ".bin" + this->extension));
    }

    EXPECT_FALSE(fs::exists("./snapshots/snapshot_50.bin" + this->extension));
    EXPECT_FALSE(fs::exists("./snapshots/snapshot_100.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_150.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_200.bin" + this->extension));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_250.bin" + this->extension));


    auto [restored_storage, meta, _] = manager.restoreFromLatestSnapshot();

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
        addNode(storage, "/hello_" + std::to_string(i), "world_" + std::to_string(i));
    }

    {
        DB::KeeperStorageSnapshot<Storage> snapshot(&storage, 50);
        for (size_t i = 0; i < 50; ++i)
        {
            addNode(storage, "/hello_" + std::to_string(i), "wlrd_" + std::to_string(i));
        }
        for (size_t i = 0; i < 50; ++i)
        {
            EXPECT_EQ(storage.container.getValue("/hello_" + std::to_string(i)).getData(), "wlrd_" + std::to_string(i));
        }
        for (size_t i = 0; i < 50; ++i)
        {
            if (i % 2 == 0)
                storage.container.erase("/hello_" + std::to_string(i));
        }
        EXPECT_EQ(storage.container.size(), 29);
        if constexpr (Storage::use_rocksdb)
            EXPECT_EQ(storage.container.snapshotSizeWithVersion().first, 54);
        else
            EXPECT_EQ(storage.container.snapshotSizeWithVersion().first, 105);
        EXPECT_EQ(storage.container.snapshotSizeWithVersion().second, 1);
        auto buf = manager.serializeSnapshotToBuffer(snapshot);
        manager.serializeSnapshotBufferToDisk(*buf, 50);
    }
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_50.bin" + this->extension));
    EXPECT_EQ(storage.container.size(), 29);
    storage.clearGarbageAfterSnapshot();
    EXPECT_EQ(storage.container.snapshotSizeWithVersion().first, 29);
    for (size_t i = 0; i < 50; ++i)
    {
        if (i % 2 != 0)
            EXPECT_EQ(storage.container.getValue("/hello_" + std::to_string(i)).getData(), "wlrd_" + std::to_string(i));
        else
            EXPECT_FALSE(storage.container.contains("/hello_" + std::to_string(i)));
    }

    auto [restored_storage, meta, _] = manager.restoreFromLatestSnapshot();

    for (size_t i = 0; i < 50; ++i)
    {
        EXPECT_EQ(restored_storage->container.getValue("/hello_" + std::to_string(i)).getData(), "world_" + std::to_string(i));
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
        DB::KeeperStorageSnapshot<Storage> snapshot(&storage, 50);
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

    DB::KeeperStorageSnapshot<Storage> snapshot(&storage, 2);

    auto buf = manager.serializeSnapshotToBuffer(snapshot);
    manager.serializeSnapshotBufferToDisk(*buf, 2);
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_2.bin" + this->extension));

    DB::KeeperSnapshotManager<Storage> new_manager(3, this->keeper_context, !this->enable_compression);

    auto debuf = new_manager.deserializeSnapshotBufferFromDisk(2);

    auto [restored_storage, snapshot_meta, _] = new_manager.deserializeSnapshotFromBuffer(debuf);

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

        DB::KeeperStorageSnapshot<Storage> snapshot(&storage, storage.getZXID());

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
    static constexpr StringRef path = "/hello";
    static constexpr uint64_t acl_id = 42;
    addNode(storage, std::string{path}, "world", /*ephemeral_owner=*/0, acl_id);
    DB::KeeperStorageSnapshot<Storage> snapshot(&storage, 50);
    auto buf = manager.serializeSnapshotToBuffer(snapshot);
    manager.serializeSnapshotBufferToDisk(*buf, 50);

    EXPECT_TRUE(fs::exists("./snapshots/snapshot_50.bin" + this->extension));
    {
        auto debuf = manager.deserializeSnapshotBufferFromDisk(50);
        auto [restored_storage, meta, _] = manager.deserializeSnapshotFromBuffer(debuf);

        EXPECT_EQ(restored_storage->container.size(), 5);
        EXPECT_EQ(restored_storage->container.getValue(path).acl_id, acl_id);
    }

    {
        this->keeper_context->setBlockACL(true);
        auto debuf = manager.deserializeSnapshotBufferFromDisk(50);
        auto [restored_storage, meta, _] = manager.deserializeSnapshotFromBuffer(debuf);

        EXPECT_EQ(restored_storage->container.size(), 5);
        EXPECT_EQ(restored_storage->container.getValue(path).acl_id, 0);
    }
}

#endif
