#include <gtest/gtest.h>
#include "config.h"

#if USE_NURAFT and USE_ROCKSDB

#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperLogStore.h>
#include <Coordination/KeeperStorage.h>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/KeeperSnapshotManager.h>
#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <Common/Logger.h>
#include <Disks/DiskLocal.h>
#include <Poco/ConsoleChannel.h>

namespace fs = std::filesystem;
struct ChangelogDirTest
{
    std::string path;
    bool drop;
    explicit ChangelogDirTest(std::string path_, bool drop_ = true) : path(path_), drop(drop_)
    {
        EXPECT_FALSE(fs::exists(path)) << "Path " << path << " already exists, remove it to run test";
        fs::create_directory(path);
    }

    ~ChangelogDirTest()
    {
        if (fs::exists(path) && drop)
            fs::remove_all(path);
    }
};

class RocksKeeperTest : public ::testing::Test
{
protected:
    DB::KeeperContextPtr keeper_context;
    LoggerPtr log{getLogger("RocksKeeperTest")};

    void SetUp() override
    {
        Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
        Poco::Logger::root().setChannel(channel);
        Poco::Logger::root().setLevel("trace");

        auto settings = std::make_shared<DB::CoordinationSettings>();
        settings->use_rocksdb = true;
        keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
        keeper_context->setLocalLogsPreprocessed();
        keeper_context->setRocksDBOptions(nullptr);
    }

    void setSnapshotDirectory(const std::string & path)
    {
        keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapshotDisk", path));
    }

    void setRocksDBDirectory(const std::string & path)
    {
        keeper_context->setRocksDBDisk(std::make_shared<DB::DiskLocal>("RocksDisk", path));
    }
};

void addNode(DB::KeeperRocksStorage & storage, const std::string & path, const std::string & data, int64_t ephemeral_owner = 0)
{
    using Node = DB::KeeperRocksStorage::Node;
    Node node{};
    node.setData(data);
    node.setEphemeralOwner(ephemeral_owner);
    storage.container.insertOrReplace(path, node);
    storage.container.updateValue(
        DB::parentNodePath(StringRef{path}),
        [&](auto & parent)
        {
            parent.increaseNumChildren();
        });
}

namespace
{
void waitDurableLogs(nuraft::log_store & log_store)
{
    while (log_store.last_durable_index() != log_store.next_slot() - 1)
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
}
}

TEST_F(RocksKeeperTest, TestStorageSnapshotSimple)
{
    ChangelogDirTest test("./snapshots");
    ChangelogDirTest rocks("./rocksdb");
    setSnapshotDirectory("./snapshots");
    setRocksDBDirectory("./rocksdb");

    DB::KeeperSnapshotManager<DB::KeeperRocksStorage> manager(3, keeper_context);

    DB::KeeperRocksStorage storage(500, "", keeper_context);
    addNode(storage, "/hello1", "world", 1);
    addNode(storage, "/hello2", "somedata", 3);
    storage.session_id_counter = 5;
    storage.zxid = 2;
    storage.ephemerals[3] = {"/hello2"};
    storage.ephemerals[1] = {"/hello1"};
    storage.getSessionID(130);
    storage.getSessionID(130);

    DB::KeeperStorageSnapshot<DB::KeeperRocksStorage> snapshot(&storage, 2);

    EXPECT_EQ(snapshot.snapshot_meta->get_last_log_idx(), 2);
    EXPECT_EQ(snapshot.session_id, 7);
    EXPECT_EQ(snapshot.snapshot_container_size, 6);
    EXPECT_EQ(snapshot.session_and_timeout.size(), 2);

    auto buf = manager.serializeSnapshotToBuffer(snapshot);
    manager.serializeSnapshotBufferToDisk(*buf, 2);
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_2.bin.zstd"));


    auto debuf = manager.deserializeSnapshotBufferFromDisk(2);

    auto [restored_storage, snapshot_meta, _] = manager.deserializeSnapshotFromBuffer(debuf);

    EXPECT_EQ(restored_storage->container.size(), 6);
    EXPECT_EQ(restored_storage->container.find("/")->value.numChildren(), 3);
    EXPECT_EQ(restored_storage->container.find("/hello1")->value.numChildren(), 0);
    EXPECT_EQ(restored_storage->container.find("/hello2")->value.numChildren(), 0);

    EXPECT_EQ(restored_storage->container.find("/")->value.getData(), "");
    EXPECT_EQ(restored_storage->container.find("/hello1")->value.getData(), "world");
    EXPECT_EQ(restored_storage->container.find("/hello2")->value.getData(), "somedata");
    EXPECT_EQ(restored_storage->session_id_counter, 7);
    EXPECT_EQ(restored_storage->zxid, 2);
    EXPECT_EQ(restored_storage->ephemerals.size(), 2);
    EXPECT_EQ(restored_storage->ephemerals[3].size(), 1);
    EXPECT_EQ(restored_storage->ephemerals[1].size(), 1);
    EXPECT_EQ(restored_storage->session_and_timeout.size(), 2);
}

TEST_F(RocksKeeperTest, TestStorageSnapshotMoreWrites)
{
    ChangelogDirTest test("./snapshots");
    ChangelogDirTest rocks("./rocksdb");
    setSnapshotDirectory("./snapshots");
    setRocksDBDirectory("./rocksdb");

    DB::KeeperSnapshotManager<DB::KeeperRocksStorage> manager(3, keeper_context);

    DB::KeeperRocksStorage storage(500, "", keeper_context);
    storage.getSessionID(130);

    for (size_t i = 0; i < 50; ++i)
    {
        addNode(storage, "/hello_" + std::to_string(i), "world_" + std::to_string(i));
    }

    DB::KeeperStorageSnapshot<DB::KeeperRocksStorage> snapshot(&storage, 50);
    EXPECT_EQ(snapshot.snapshot_meta->get_last_log_idx(), 50);
    EXPECT_EQ(snapshot.snapshot_container_size, 54);

    for (size_t i = 50; i < 100; ++i)
    {
        addNode(storage, "/hello_" + std::to_string(i), "world_" + std::to_string(i));
    }

    EXPECT_EQ(storage.container.size(), 104);

    auto buf = manager.serializeSnapshotToBuffer(snapshot);
    manager.serializeSnapshotBufferToDisk(*buf, 50);
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_50.bin.zstd"));


    auto debuf = manager.deserializeSnapshotBufferFromDisk(50);
    auto [restored_storage, meta, _] = manager.deserializeSnapshotFromBuffer(debuf);

    EXPECT_EQ(restored_storage->container.size(), 54);
    for (size_t i = 0; i < 50; ++i)
    {
        EXPECT_EQ(restored_storage->container.find("/hello_" + std::to_string(i))->value.getData(), "world_" + std::to_string(i));
    }
}

TEST_F(RocksKeeperTest, TestStorageSnapshotManySnapshots)
{
    ChangelogDirTest test("./snapshots");
    ChangelogDirTest rocks("./rocksdb");
    setSnapshotDirectory("./snapshots");
    setRocksDBDirectory("./rocksdb");

    DB::KeeperSnapshotManager<DB::KeeperRocksStorage> manager(3, keeper_context);

    DB::KeeperRocksStorage storage(500, "", keeper_context);
    storage.getSessionID(130);

    for (size_t j = 1; j <= 5; ++j)
    {
        for (size_t i = (j - 1) * 50; i < j * 50; ++i)
        {
            addNode(storage, "/hello_" + std::to_string(i), "world_" + std::to_string(i));
        }

        DB::KeeperStorageSnapshot<DB::KeeperRocksStorage> snapshot(&storage, j * 50);
        auto buf = manager.serializeSnapshotToBuffer(snapshot);
        manager.serializeSnapshotBufferToDisk(*buf, j * 50);
        EXPECT_TRUE(fs::exists(std::string{"./snapshots/snapshot_"} + std::to_string(j * 50) + ".bin.zstd"));
    }

    EXPECT_FALSE(fs::exists("./snapshots/snapshot_50.bin.zstd"));
    EXPECT_FALSE(fs::exists("./snapshots/snapshot_100.bin.zstd"));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_150.bin.zstd"));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_200.bin.zstd"));
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_250.bin.zstd"));


    auto [restored_storage, meta, _] = manager.restoreFromLatestSnapshot();

    EXPECT_EQ(restored_storage->container.size(), 254);

    for (size_t i = 0; i < 250; ++i)
    {
        EXPECT_EQ(restored_storage->container.find("/hello_" + std::to_string(i))->value.getData(), "world_" + std::to_string(i));
    }
}

TEST_F(RocksKeeperTest, TestStorageSnapshotMode)
{
    ChangelogDirTest test("./snapshots");
    setSnapshotDirectory("./snapshots");
    ChangelogDirTest rocks("./rocksdb");
    setRocksDBDirectory("./rocksdb");

    DB::KeeperSnapshotManager<DB::KeeperRocksStorage> manager(3, keeper_context);
    DB::KeeperRocksStorage storage(500, "", keeper_context);
    for (size_t i = 0; i < 50; ++i)
    {
        addNode(storage, "/hello_" + std::to_string(i), "world_" + std::to_string(i));
    }

    {
        DB::KeeperStorageSnapshot<DB::KeeperRocksStorage> snapshot(&storage, 50);
        for (size_t i = 0; i < 50; ++i)
        {
            addNode(storage, "/hello_" + std::to_string(i), "wlrd_" + std::to_string(i));
        }
        for (size_t i = 0; i < 50; ++i)
        {
            EXPECT_EQ(storage.container.find("/hello_" + std::to_string(i))->value.getData(), "wlrd_" + std::to_string(i));
        }
        for (size_t i = 0; i < 50; ++i)
        {
            if (i % 2 == 0)
                storage.container.erase("/hello_" + std::to_string(i));
        }
        EXPECT_EQ(storage.container.size(), 29);
        EXPECT_EQ(storage.container.snapshotSizeWithVersion().first, 54);
        EXPECT_EQ(storage.container.snapshotSizeWithVersion().second, 1);
        auto buf = manager.serializeSnapshotToBuffer(snapshot);
        manager.serializeSnapshotBufferToDisk(*buf, 50);
    }
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_50.bin.zstd"));
    EXPECT_EQ(storage.container.size(), 29);
    storage.clearGarbageAfterSnapshot();
    for (size_t i = 0; i < 50; ++i)
    {
        if (i % 2 != 0)
            EXPECT_EQ(storage.container.find("/hello_" + std::to_string(i))->value.getData(), "wlrd_" + std::to_string(i));
        else
            EXPECT_FALSE(storage.container.contains("/hello_" + std::to_string(i)));
    }

    auto [restored_storage, meta, _] = manager.restoreFromLatestSnapshot();

    for (size_t i = 0; i < 50; ++i)
    {
        EXPECT_EQ(restored_storage->container.find("/hello_" + std::to_string(i))->value.getData(), "world_" + std::to_string(i));
    }
}

TEST_F(RocksKeeperTest, TestStorageSnapshotBroken)
{
    ChangelogDirTest test("./snapshots");
    setSnapshotDirectory("./snapshots");
    ChangelogDirTest rocks("./rocksdb");
    setRocksDBDirectory("./rocksdb");

    DB::KeeperSnapshotManager<DB::KeeperRocksStorage> manager(3, keeper_context);
    DB::KeeperRocksStorage storage(500, "", keeper_context);
    for (size_t i = 0; i < 50; ++i)
    {
        addNode(storage, "/hello_" + std::to_string(i), "world_" + std::to_string(i));
    }
    {
        DB::KeeperStorageSnapshot<DB::KeeperRocksStorage> snapshot(&storage, 50);
        auto buf = manager.serializeSnapshotToBuffer(snapshot);
        manager.serializeSnapshotBufferToDisk(*buf, 50);
    }
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_50.bin.zstd"));

    /// Let's corrupt file
    DB::WriteBufferFromFile plain_buf(
        "./snapshots/snapshot_50.bin.zstd", DB::DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY);
    plain_buf.truncate(34);
    plain_buf.sync();

    EXPECT_THROW(manager.restoreFromLatestSnapshot(), DB::Exception);
}

nuraft::ptr<nuraft::buffer> getBufferFromZKRequest(int64_t session_id, int64_t zxid, const Coordination::ZooKeeperRequestPtr & request);

nuraft::ptr<nuraft::log_entry>
getLogEntryFromZKRequest(size_t term, int64_t session_id, int64_t zxid, const Coordination::ZooKeeperRequestPtr & request);

static void testLogAndStateMachine(
    DB::CoordinationSettingsPtr settings,
    uint64_t total_logs,
    bool enable_compression = true)
{
    using namespace Coordination;
    using namespace DB;

    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest logs("./logs");
    ChangelogDirTest rocks("./rocksdb");

    auto get_keeper_context = [&]
    {
        auto local_keeper_context = std::make_shared<DB::KeeperContext>(true, settings);
        local_keeper_context->setSnapshotDisk(std::make_shared<DiskLocal>("SnapshotDisk", "./snapshots"));
        local_keeper_context->setLogDisk(std::make_shared<DiskLocal>("LogDisk", "./logs"));
        local_keeper_context->setRocksDBDisk(std::make_shared<DiskLocal>("RocksDisk", "./rocksdb"));
        local_keeper_context->setRocksDBOptions(nullptr);
        return local_keeper_context;
    };

    ResponsesQueue queue(std::numeric_limits<size_t>::max());
    SnapshotsQueue snapshots_queue{1};

    auto keeper_context = get_keeper_context();
    auto state_machine = std::make_shared<KeeperStateMachine<DB::KeeperRocksStorage>>(queue, snapshots_queue, keeper_context, nullptr);

    state_machine->init();
    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = true, .compress_logs = enable_compression, .rotate_interval = settings->rotate_log_storage_interval},
        DB::FlushSettings(),
        keeper_context);
    changelog.init(state_machine->last_commit_index() + 1, settings->reserved_log_items);

    for (size_t i = 1; i < total_logs + 1; ++i)
    {
        std::shared_ptr<ZooKeeperCreateRequest> request = std::make_shared<ZooKeeperCreateRequest>();
        request->path = "/hello_" + std::to_string(i);
        auto entry = getLogEntryFromZKRequest(0, 1, i, request);
        changelog.append(entry);
        changelog.end_of_append_batch(0, 0);

        waitDurableLogs(changelog);

        state_machine->pre_commit(i, changelog.entry_at(i)->get_buf());
        state_machine->commit(i, changelog.entry_at(i)->get_buf());
        bool snapshot_created = false;
        if (i % settings->snapshot_distance == 0)
        {
            nuraft::snapshot s(i, 0, std::make_shared<nuraft::cluster_config>());
            nuraft::async_result<bool>::handler_type when_done
                = [&snapshot_created](bool & ret, nuraft::ptr<std::exception> & /*exception*/)
            {
                snapshot_created = ret;
            };

            state_machine->create_snapshot(s, when_done);
            CreateSnapshotTask snapshot_task;
            bool pop_result = snapshots_queue.pop(snapshot_task);
            EXPECT_TRUE(pop_result);

            snapshot_task.create_snapshot(std::move(snapshot_task.snapshot), false);
        }

        if (snapshot_created && changelog.size() > settings->reserved_log_items)
            changelog.compact(i - settings->reserved_log_items);
    }

    SnapshotsQueue snapshots_queue1{1};
    keeper_context = get_keeper_context();
    auto restore_machine = std::make_shared<KeeperStateMachine<DB::KeeperRocksStorage>>(queue, snapshots_queue1, keeper_context, nullptr);
    restore_machine->init();
    EXPECT_EQ(restore_machine->last_commit_index(), total_logs - total_logs % settings->snapshot_distance);

    DB::KeeperLogStore restore_changelog(
        DB::LogFileSettings{
            .force_sync = true, .compress_logs = enable_compression, .rotate_interval = settings->rotate_log_storage_interval},
        DB::FlushSettings(),
        keeper_context);
    restore_changelog.init(restore_machine->last_commit_index() + 1, settings->reserved_log_items);

    EXPECT_EQ(restore_changelog.size(), std::min(settings->reserved_log_items + total_logs % settings->snapshot_distance, total_logs));
    EXPECT_EQ(restore_changelog.next_slot(), total_logs + 1);
    if (total_logs > settings->reserved_log_items + 1)
        EXPECT_EQ(
            restore_changelog.start_index(), total_logs - total_logs % settings->snapshot_distance - settings->reserved_log_items + 1);
    else
        EXPECT_EQ(restore_changelog.start_index(), 1);

    for (size_t i = restore_machine->last_commit_index() + 1; i < restore_changelog.next_slot(); ++i)
    {
        restore_machine->pre_commit(i, changelog.entry_at(i)->get_buf());
        restore_machine->commit(i, changelog.entry_at(i)->get_buf());
    }

    auto & source_storage = state_machine->getStorageUnsafe();
    auto & restored_storage = restore_machine->getStorageUnsafe();

    EXPECT_EQ(source_storage.container.size(), restored_storage.container.size());
    for (size_t i = 1; i < total_logs + 1; ++i)
    {
        auto path = "/hello_" + std::to_string(i);
        EXPECT_EQ(source_storage.container.find(path)->value.getData(), restored_storage.container.find(path)->value.getData());
    }
}

TEST_F(RocksKeeperTest, TestStateMachineAndLogStore)
{
    using namespace Coordination;
    using namespace DB;

    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 10;
        settings->reserved_log_items = 10;
        settings->rotate_log_storage_interval = 10;

        testLogAndStateMachine(settings, 37);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 10;
        settings->reserved_log_items = 10;
        settings->rotate_log_storage_interval = 10;
        testLogAndStateMachine(settings, 11);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 10;
        settings->reserved_log_items = 10;
        settings->rotate_log_storage_interval = 10;
        testLogAndStateMachine(settings, 40);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 10;
        settings->reserved_log_items = 20;
        settings->rotate_log_storage_interval = 30;
        testLogAndStateMachine(settings, 40);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 10;
        settings->reserved_log_items = 0;
        settings->rotate_log_storage_interval = 10;
        testLogAndStateMachine(settings, 40);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 1;
        settings->reserved_log_items = 1;
        settings->rotate_log_storage_interval = 32;
        testLogAndStateMachine(settings, 32);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 10;
        settings->reserved_log_items = 7;
        settings->rotate_log_storage_interval = 1;
        testLogAndStateMachine(settings, 33);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 37;
        settings->reserved_log_items = 1000;
        settings->rotate_log_storage_interval = 5000;
        testLogAndStateMachine(settings, 33);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        settings->snapshot_distance = 37;
        settings->reserved_log_items = 1000;
        settings->rotate_log_storage_interval = 5000;
        testLogAndStateMachine(settings, 45);
    }
}

TEST_F(RocksKeeperTest, TestEphemeralNodeRemove)
{
    using namespace Coordination;
    using namespace DB;

    ChangelogDirTest snapshots("./snapshots");
    setSnapshotDirectory("./snapshots");
    ChangelogDirTest rocks("./rocksdb");
    setRocksDBDirectory("./rocksdb");

    ResponsesQueue queue(std::numeric_limits<size_t>::max());
    SnapshotsQueue snapshots_queue{1};

    auto state_machine = std::make_shared<KeeperStateMachine<DB::KeeperRocksStorage>>(queue, snapshots_queue, keeper_context, nullptr);
    state_machine->init();

    std::shared_ptr<ZooKeeperCreateRequest> request_c = std::make_shared<ZooKeeperCreateRequest>();
    request_c->path = "/hello";
    request_c->is_ephemeral = true;
    auto entry_c = getLogEntryFromZKRequest(0, 1, state_machine->getNextZxid(), request_c);
    state_machine->pre_commit(1, entry_c->get_buf());
    state_machine->commit(1, entry_c->get_buf());
    const auto & storage = state_machine->getStorageUnsafe();

    EXPECT_EQ(storage.ephemerals.size(), 1);
    std::shared_ptr<ZooKeeperRemoveRequest> request_d = std::make_shared<ZooKeeperRemoveRequest>();
    request_d->path = "/hello";
    /// Delete from other session
    auto entry_d = getLogEntryFromZKRequest(0, 2, state_machine->getNextZxid(), request_d);
    state_machine->pre_commit(2, entry_d->get_buf());
    state_machine->commit(2, entry_d->get_buf());

    EXPECT_EQ(storage.ephemerals.size(), 0);
}

TEST_F(RocksKeeperTest, TestCreateNodeWithAuthSchemeForAclWhenAuthIsPrecommitted)
{
    using namespace Coordination;
    using namespace DB;

    ChangelogDirTest snapshots("./snapshots");
    setSnapshotDirectory("./snapshots");
    ChangelogDirTest rocks("./rocksdb");
    setRocksDBDirectory("./rocksdb");
    ResponsesQueue queue(std::numeric_limits<size_t>::max());
    SnapshotsQueue snapshots_queue{1};

    auto state_machine = std::make_shared<KeeperStateMachine<DB::KeeperRocksStorage>>(queue, snapshots_queue, keeper_context, nullptr);
    state_machine->init();

    String user_auth_data = "test_user:test_password";
    String digest = KeeperRocksStorage::generateDigest(user_auth_data);

    std::shared_ptr<ZooKeeperAuthRequest> auth_req = std::make_shared<ZooKeeperAuthRequest>();
    auth_req->scheme = "digest";
    auth_req->data = user_auth_data;

    // Add auth data to the session
    auto auth_entry = getLogEntryFromZKRequest(0, 1, state_machine->getNextZxid(), auth_req);
    state_machine->pre_commit(1, auth_entry->get_buf());

    // Create a node with 'auth' scheme for ACL
    String node_path = "/hello";
    std::shared_ptr<ZooKeeperCreateRequest> create_req = std::make_shared<ZooKeeperCreateRequest>();
    create_req->path = node_path;
    // When 'auth' scheme is used the creator must have been authenticated by the server (for example, using 'digest' scheme) before it can
    // create nodes with this ACL.
    create_req->acls = {{.permissions = 31, .scheme = "auth", .id = ""}};
    auto create_entry = getLogEntryFromZKRequest(0, 1, state_machine->getNextZxid(), create_req);
    state_machine->pre_commit(2, create_entry->get_buf());

    const auto & uncommitted_state = state_machine->getStorageUnsafe().uncommitted_state;
    ASSERT_TRUE(uncommitted_state.nodes.contains(node_path));

    // commit log entries
    state_machine->commit(1, auth_entry->get_buf());
    state_machine->commit(2, create_entry->get_buf());

    auto node = uncommitted_state.getNode(node_path);
    ASSERT_NE(node, nullptr);
    auto acls = uncommitted_state.getACLs(node_path);
    ASSERT_EQ(acls.size(), 1);
    EXPECT_EQ(acls[0].scheme, "digest");
    EXPECT_EQ(acls[0].id, digest);
    EXPECT_EQ(acls[0].permissions, 31);
}

TEST_F(RocksKeeperTest, TestSetACLWithAuthSchemeForAclWhenAuthIsPrecommitted)
{
    using namespace Coordination;
    using namespace DB;

    ChangelogDirTest snapshots("./snapshots");
    setSnapshotDirectory("./snapshots");
    ChangelogDirTest rocks("./rocksdb");
    setRocksDBDirectory("./rocksdb");

    ResponsesQueue queue(std::numeric_limits<size_t>::max());
    SnapshotsQueue snapshots_queue{1};

    auto state_machine = std::make_shared<KeeperStateMachine<DB::KeeperRocksStorage>>(queue, snapshots_queue, keeper_context, nullptr);
    state_machine->init();

    String user_auth_data = "test_user:test_password";
    String digest = KeeperRocksStorage::generateDigest(user_auth_data);

    std::shared_ptr<ZooKeeperAuthRequest> auth_req = std::make_shared<ZooKeeperAuthRequest>();
    auth_req->scheme = "digest";
    auth_req->data = user_auth_data;

    // Add auth data to the session
    auto auth_entry = getLogEntryFromZKRequest(0, 1, state_machine->getNextZxid(), auth_req);
    state_machine->pre_commit(1, auth_entry->get_buf());

    // Create a node
    String node_path = "/hello";
    std::shared_ptr<ZooKeeperCreateRequest> create_req = std::make_shared<ZooKeeperCreateRequest>();
    create_req->path = node_path;
    auto create_entry = getLogEntryFromZKRequest(0, 1, state_machine->getNextZxid(), create_req);
    state_machine->pre_commit(2, create_entry->get_buf());

    // Set ACL with 'auth' scheme for ACL
    std::shared_ptr<ZooKeeperSetACLRequest> set_acl_req = std::make_shared<ZooKeeperSetACLRequest>();
    set_acl_req->path = node_path;
    // When 'auth' scheme is used the creator must have been authenticated by the server (for example, using 'digest' scheme) before it can
    // set this ACL.
    set_acl_req->acls = {{.permissions = 31, .scheme = "auth", .id = ""}};
    auto set_acl_entry = getLogEntryFromZKRequest(0, 1, state_machine->getNextZxid(), set_acl_req);
    state_machine->pre_commit(3, set_acl_entry->get_buf());

    // commit all entries
    state_machine->commit(1, auth_entry->get_buf());
    state_machine->commit(2, create_entry->get_buf());
    state_machine->commit(3, set_acl_entry->get_buf());

    const auto & uncommitted_state = state_machine->getStorageUnsafe().uncommitted_state;
    auto node = uncommitted_state.getNode(node_path);

    ASSERT_NE(node, nullptr);
    auto acls = uncommitted_state.getACLs(node_path);
    ASSERT_EQ(acls.size(), 1);
    EXPECT_EQ(acls[0].scheme, "digest");
    EXPECT_EQ(acls[0].id, digest);
    EXPECT_EQ(acls[0].permissions, 31);
}

TEST_F(RocksKeeperTest, TestStorageSnapshotEqual)
{
    ChangelogDirTest test("./snapshots");
    ChangelogDirTest rocks("./rocksdb");
    setSnapshotDirectory("./snapshots");
    setRocksDBDirectory("./rocksdb");

    std::optional<UInt128> snapshot_hash;
    for (size_t i = 0; i < 15; ++i)
    {
        DB::KeeperSnapshotManager<DB::KeeperRocksStorage> manager(3, keeper_context);

        DB::KeeperRocksStorage storage(500, "", keeper_context);
        addNode(storage, "/hello", "");
        for (size_t j = 0; j < 100; ++j)
        {
            addNode(storage, "/hello_" + std::to_string(j), "world", 1);
            addNode(storage, "/hello/somepath_" + std::to_string(j), "somedata", 3);
        }

        storage.session_id_counter = 5;

        storage.ephemerals[3] = {"/hello"};
        storage.ephemerals[1] = {"/hello/somepath"};

        for (size_t j = 0; j < 3333; ++j)
            storage.getSessionID(130 * j);

        DB::KeeperStorageSnapshot<DB::KeeperRocksStorage> snapshot(&storage, storage.zxid);

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

TEST_F(RocksKeeperTest, TestStorageSnapshotDifferentCompressions)
{
    ChangelogDirTest test("./snapshots");
    setSnapshotDirectory("./snapshots");
    ChangelogDirTest rocks("./rocksdb");
    setRocksDBDirectory("./rocksdb");

    DB::KeeperSnapshotManager<DB::KeeperRocksStorage> manager(3, keeper_context);

    DB::KeeperRocksStorage storage(500, "", keeper_context);
    addNode(storage, "/hello1", "world", 1);
    addNode(storage, "/hello2", "somedata", 3);
    storage.session_id_counter = 5;
    storage.zxid = 2;
    storage.ephemerals[3] = {"/hello2"};
    storage.ephemerals[1] = {"/hello1"};
    storage.getSessionID(130);
    storage.getSessionID(130);

    DB::KeeperStorageSnapshot<DB::KeeperRocksStorage> snapshot(&storage, 2);

    auto buf = manager.serializeSnapshotToBuffer(snapshot);
    manager.serializeSnapshotBufferToDisk(*buf, 2);
    EXPECT_TRUE(fs::exists("./snapshots/snapshot_2.bin.zstd"));

    DB::KeeperSnapshotManager<DB::KeeperRocksStorage> new_manager(3, keeper_context, false);

    auto debuf = new_manager.deserializeSnapshotBufferFromDisk(2);

    auto [restored_storage, snapshot_meta, _] = new_manager.deserializeSnapshotFromBuffer(debuf);

    EXPECT_EQ(restored_storage->container.size(), 6);
    EXPECT_EQ(restored_storage->container.find("/")->value.numChildren(), 3);
    EXPECT_EQ(restored_storage->container.find("/hello1")->value.numChildren(), 0);
    EXPECT_EQ(restored_storage->container.find("/hello2")->value.numChildren(), 0);

    EXPECT_EQ(restored_storage->container.find("/")->value.getData(), "");
    EXPECT_EQ(restored_storage->container.find("/hello1")->value.getData(), "world");
    EXPECT_EQ(restored_storage->container.find("/hello2")->value.getData(), "somedata");
    EXPECT_EQ(restored_storage->session_id_counter, 7);
    EXPECT_EQ(restored_storage->zxid, 2);
    EXPECT_EQ(restored_storage->ephemerals.size(), 2);
    EXPECT_EQ(restored_storage->ephemerals[3].size(), 1);
    EXPECT_EQ(restored_storage->ephemerals[1].size(), 1);
    EXPECT_EQ(restored_storage->session_and_timeout.size(), 2);
}

template <typename ResponseType>
ResponseType getSingleResponse(const auto & responses)
{
    EXPECT_FALSE(responses.empty());
    return dynamic_cast<ResponseType &>(*responses[0].response);
}

TEST_F(RocksKeeperTest, TestUncommittedStateBasicCrud)
{
    ChangelogDirTest test("./rocksdb");
    setRocksDBDirectory("./rocksdb");

    using namespace DB;
    using namespace Coordination;

    DB::KeeperRocksStorage storage{500, "", keeper_context};

    constexpr std::string_view path = "/test";

    const auto get_committed_data = [&]() -> std::optional<String>
    {
        auto request = std::make_shared<ZooKeeperGetRequest>();
        request->path = path;
        auto responses = storage.processRequest(request, 0, std::nullopt, true, true);
        const auto & get_response = getSingleResponse<ZooKeeperGetResponse>(responses);

        if (get_response.error != Error::ZOK)
            return std::nullopt;

        return get_response.data;
    };

    const auto preprocess_get = [&](int64_t zxid)
    {
        auto get_request = std::make_shared<ZooKeeperGetRequest>();
        get_request->path = path;
        storage.preprocessRequest(get_request, 0, 0, zxid);
        return get_request;
    };

    const auto create_request = std::make_shared<ZooKeeperCreateRequest>();
    create_request->path = path;
    create_request->data = "initial_data";
    storage.preprocessRequest(create_request, 0, 0, 1);
    storage.preprocessRequest(create_request, 0, 0, 2);

    ASSERT_FALSE(get_committed_data());

    const auto after_create_get = preprocess_get(3);

    ASSERT_FALSE(get_committed_data());

    const auto set_request = std::make_shared<ZooKeeperSetRequest>();
    set_request->path = path;
    set_request->data = "new_data";
    storage.preprocessRequest(set_request, 0, 0, 4);

    const auto after_set_get = preprocess_get(5);

    ASSERT_FALSE(get_committed_data());

    const auto remove_request = std::make_shared<ZooKeeperRemoveRequest>();
    remove_request->path = path;
    storage.preprocessRequest(remove_request, 0, 0, 6);
    storage.preprocessRequest(remove_request, 0, 0, 7);

    const auto after_remove_get = preprocess_get(8);

    ASSERT_FALSE(get_committed_data());

    {
        const auto responses = storage.processRequest(create_request, 0, 1);
        const auto & create_response = getSingleResponse<ZooKeeperCreateResponse>(responses);
        ASSERT_EQ(create_response.error, Error::ZOK);
    }

    {
        const auto responses = storage.processRequest(create_request, 0, 2);
        const auto & create_response = getSingleResponse<ZooKeeperCreateResponse>(responses);
        ASSERT_EQ(create_response.error, Error::ZNODEEXISTS);
    }

    {
        const auto responses = storage.processRequest(after_create_get, 0, 3);
        const auto & get_response = getSingleResponse<ZooKeeperGetResponse>(responses);
        ASSERT_EQ(get_response.error, Error::ZOK);
        ASSERT_EQ(get_response.data, "initial_data");
    }

    ASSERT_EQ(get_committed_data(), "initial_data");

    {
        const auto responses = storage.processRequest(set_request, 0, 4);
        const auto & create_response = getSingleResponse<ZooKeeperSetResponse>(responses);
        ASSERT_EQ(create_response.error, Error::ZOK);
    }

    {
        const auto responses = storage.processRequest(after_set_get, 0, 5);
        const auto & get_response = getSingleResponse<ZooKeeperGetResponse>(responses);
        ASSERT_EQ(get_response.error, Error::ZOK);
        ASSERT_EQ(get_response.data, "new_data");
    }

    ASSERT_EQ(get_committed_data(), "new_data");

    {
        const auto responses = storage.processRequest(remove_request, 0, 6);
        const auto & create_response = getSingleResponse<ZooKeeperRemoveResponse>(responses);
        ASSERT_EQ(create_response.error, Error::ZOK);
    }

    {
        const auto responses = storage.processRequest(remove_request, 0, 7);
        const auto & remove_response = getSingleResponse<ZooKeeperRemoveResponse>(responses);
        ASSERT_EQ(remove_response.error, Error::ZNONODE);
    }

    {
        const auto responses = storage.processRequest(after_remove_get, 0, 8);
        const auto & get_response = getSingleResponse<ZooKeeperGetResponse>(responses);
        ASSERT_EQ(get_response.error, Error::ZNONODE);
    }

    ASSERT_FALSE(get_committed_data());
}

TEST_F(RocksKeeperTest, TestListRequestTypes)
{
    ChangelogDirTest test("./rocksdb");
    setRocksDBDirectory("./rocksdb");

    using namespace DB;
    using namespace Coordination;
    KeeperRocksStorage storage{500, "", keeper_context};

    int32_t zxid = 0;

    static constexpr std::string_view test_path = "/list_request_type/node";

    const auto create_path = [&](const auto & path, bool is_ephemeral, bool is_sequential = true)
    {
        const auto create_request = std::make_shared<ZooKeeperCreateRequest>();
        int new_zxid = ++zxid;
        create_request->path = path;
        create_request->is_sequential = is_sequential;
        create_request->is_ephemeral = is_ephemeral;
        storage.preprocessRequest(create_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(create_request, 1, new_zxid);

        EXPECT_GE(responses.size(), 1);
        EXPECT_EQ(responses[0].response->error, Coordination::Error::ZOK) << "Failed to create " << path;
        const auto & create_response = dynamic_cast<ZooKeeperCreateResponse &>(*responses[0].response);
        return create_response.path_created;
    };

    create_path(parentNodePath(StringRef{test_path}).toString(), false, false);

    static constexpr size_t persistent_num = 5;
    std::unordered_set<std::string> expected_persistent_children;
    for (size_t i = 0; i < persistent_num; ++i)
    {
        expected_persistent_children.insert(getBaseNodeName(create_path(test_path, false)).toString());
    }
    ASSERT_EQ(expected_persistent_children.size(), persistent_num);

    static constexpr size_t ephemeral_num = 5;
    std::unordered_set<std::string> expected_ephemeral_children;
    for (size_t i = 0; i < ephemeral_num; ++i)
    {
        expected_ephemeral_children.insert(getBaseNodeName(create_path(test_path, true)).toString());
    }
    ASSERT_EQ(expected_ephemeral_children.size(), ephemeral_num);

    const auto get_children = [&](const auto list_request_type)
    {
        const auto list_request = std::make_shared<ZooKeeperFilteredListRequest>();
        int new_zxid = ++zxid;
        list_request->path = parentNodePath(StringRef{test_path}).toString();
        list_request->list_request_type = list_request_type;
        storage.preprocessRequest(list_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(list_request, 1, new_zxid);

        EXPECT_GE(responses.size(), 1);
        const auto & list_response = dynamic_cast<ZooKeeperListResponse &>(*responses[0].response);
        EXPECT_EQ(list_response.error, Coordination::Error::ZOK);
        return list_response.names;
    };

    const auto persistent_children = get_children(ListRequestType::PERSISTENT_ONLY);
    EXPECT_EQ(persistent_children.size(), persistent_num);
    for (const auto & child : persistent_children)
    {
        EXPECT_TRUE(expected_persistent_children.contains(child)) << "Missing persistent child " << child;
    }

    const auto ephemeral_children = get_children(ListRequestType::EPHEMERAL_ONLY);
    EXPECT_EQ(ephemeral_children.size(), ephemeral_num);
    for (const auto & child : ephemeral_children)
    {
        EXPECT_TRUE(expected_ephemeral_children.contains(child)) << "Missing ephemeral child " << child;
    }

    const auto all_children = get_children(ListRequestType::ALL);
    EXPECT_EQ(all_children.size(), ephemeral_num + persistent_num);
    for (const auto & child : all_children)
    {
        EXPECT_TRUE(expected_ephemeral_children.contains(child) || expected_persistent_children.contains(child))
            << "Missing child " << child;
    }
}

TEST_F(RocksKeeperTest, TestFeatureFlags)
{
    ChangelogDirTest test("./rocksdb");
    setRocksDBDirectory("./rocksdb");

    using namespace Coordination;
    KeeperMemoryStorage storage{500, "", keeper_context};
    auto request = std::make_shared<ZooKeeperGetRequest>();
    request->path = DB::keeper_api_feature_flags_path;
    auto responses = storage.processRequest(request, 0, std::nullopt, true, true);
    const auto & get_response = getSingleResponse<ZooKeeperGetResponse>(responses);
    DB::KeeperFeatureFlags feature_flags;
    feature_flags.setFeatureFlags(get_response.data);
    ASSERT_TRUE(feature_flags.isEnabled(KeeperFeatureFlag::FILTERED_LIST));
    ASSERT_TRUE(feature_flags.isEnabled(KeeperFeatureFlag::MULTI_READ));
    ASSERT_FALSE(feature_flags.isEnabled(KeeperFeatureFlag::CHECK_NOT_EXISTS));
}

TEST_F(RocksKeeperTest, TestSystemNodeModify)
{
    ChangelogDirTest test("./rocksdb");
    setRocksDBDirectory("./rocksdb");

    using namespace Coordination;
    int64_t zxid{0};

    // On INIT we abort when a system path is modified
    keeper_context->setServerState(KeeperContext::Phase::RUNNING);
    KeeperRocksStorage storage{500, "", keeper_context};
    const auto assert_create = [&](const std::string_view path, const auto expected_code)
    {
        auto request = std::make_shared<ZooKeeperCreateRequest>();
        request->path = path;
        storage.preprocessRequest(request, 0, 0, zxid);
        auto responses = storage.processRequest(request, 0, zxid);
        ASSERT_FALSE(responses.empty());

        const auto & response = responses[0];
        ASSERT_EQ(response.response->error, expected_code) << "Unexpected error for path " << path;

        ++zxid;
    };

    assert_create("/keeper", Error::ZBADARGUMENTS);
    assert_create("/keeper/with_child", Error::ZBADARGUMENTS);
    assert_create(DB::keeper_api_version_path, Error::ZBADARGUMENTS);

    assert_create("/keeper_map", Error::ZOK);
    assert_create("/keeper1", Error::ZOK);
    assert_create("/keepe", Error::ZOK);
    assert_create("/keeper1/test", Error::ZOK);
}

TEST_F(RocksKeeperTest, TestCheckNotExistsRequest)
{
    ChangelogDirTest test("./rocksdb");
    setRocksDBDirectory("./rocksdb");

    using namespace DB;
    using namespace Coordination;

    KeeperRocksStorage storage{500, "", keeper_context};

    int32_t zxid = 0;

    const auto create_path = [&](const auto & path)
    {
        const auto create_request = std::make_shared<ZooKeeperCreateRequest>();
        int new_zxid = ++zxid;
        create_request->path = path;
        storage.preprocessRequest(create_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(create_request, 1, new_zxid);

        EXPECT_GE(responses.size(), 1);
        EXPECT_EQ(responses[0].response->error, Coordination::Error::ZOK) << "Failed to create " << path;
    };

    const auto check_request = std::make_shared<ZooKeeperCheckRequest>();
    check_request->path = "/test_node";
    check_request->not_exists = true;

    {
        SCOPED_TRACE("CheckNotExists returns ZOK");
        int new_zxid = ++zxid;
        storage.preprocessRequest(check_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(check_request, 1, new_zxid);
        EXPECT_GE(responses.size(), 1);
        auto error = responses[0].response->error;
        EXPECT_EQ(error, Coordination::Error::ZOK) << "CheckNotExists returned invalid result: " << errorMessage(error);
    }

    create_path("/test_node");
    auto node_it = storage.container.find("/test_node");
    ASSERT_NE(node_it, storage.container.end());
    auto node_version = node_it->value.version;

    {
        SCOPED_TRACE("CheckNotExists returns ZNODEEXISTS");
        int new_zxid = ++zxid;
        storage.preprocessRequest(check_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(check_request, 1, new_zxid);
        EXPECT_GE(responses.size(), 1);
        auto error = responses[0].response->error;
        EXPECT_EQ(error, Coordination::Error::ZNODEEXISTS) << "CheckNotExists returned invalid result: " << errorMessage(error);
    }

    {
        SCOPED_TRACE("CheckNotExists returns ZNODEEXISTS for same version");
        int new_zxid = ++zxid;
        check_request->version = node_version;
        storage.preprocessRequest(check_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(check_request, 1, new_zxid);
        EXPECT_GE(responses.size(), 1);
        auto error = responses[0].response->error;
        EXPECT_EQ(error, Coordination::Error::ZNODEEXISTS) << "CheckNotExists returned invalid result: " << errorMessage(error);
    }

    {
        SCOPED_TRACE("CheckNotExists returns ZOK for different version");
        int new_zxid = ++zxid;
        check_request->version = node_version + 1;
        storage.preprocessRequest(check_request, 1, 0, new_zxid);
        auto responses = storage.processRequest(check_request, 1, new_zxid);
        EXPECT_GE(responses.size(), 1);
        auto error = responses[0].response->error;
        EXPECT_EQ(error, Coordination::Error::ZOK) << "CheckNotExists returned invalid result: " << errorMessage(error);
    }
}

TEST_F(RocksKeeperTest, TestReapplyingDeltas)
{
    ChangelogDirTest test("./rocksdb");
    setRocksDBDirectory("./rocksdb");

    using namespace DB;
    using namespace Coordination;

    static constexpr int64_t initial_zxid = 100;

    const auto create_request = std::make_shared<ZooKeeperCreateRequest>();
    create_request->path = "/test/data";
    create_request->is_sequential = true;

    const auto process_create = [](KeeperRocksStorage & storage, const auto & request, int64_t zxid)
    {
        storage.preprocessRequest(request, 1, 0, zxid);
        auto responses = storage.processRequest(request, 1, zxid);
        EXPECT_GE(responses.size(), 1);
        EXPECT_EQ(responses[0].response->error, Error::ZOK);
    };

    const auto commit_initial_data = [&](auto & storage)
    {
        int64_t zxid = 1;

        const auto root_create = std::make_shared<ZooKeeperCreateRequest>();
        root_create->path = "/test";
        process_create(storage, root_create, zxid);
        ++zxid;

        for (; zxid <= initial_zxid; ++zxid)
            process_create(storage, create_request, zxid);
    };

    KeeperRocksStorage storage1{500, "", keeper_context};
    commit_initial_data(storage1);

    for (int64_t zxid = initial_zxid + 1; zxid < initial_zxid + 50; ++zxid)
        storage1.preprocessRequest(create_request, 1, 0, zxid, /*check_acl=*/true, /*digest=*/std::nullopt, /*log_idx=*/zxid);

    /// create identical new storage
    KeeperRocksStorage storage2{500, "", keeper_context};
    commit_initial_data(storage2);

    storage1.applyUncommittedState(storage2, initial_zxid);

    const auto commit_unprocessed = [&](KeeperRocksStorage & storage)
    {
        for (int64_t zxid = initial_zxid + 1; zxid < initial_zxid + 50; ++zxid)
        {
            auto responses = storage.processRequest(create_request, 1, zxid);
            EXPECT_GE(responses.size(), 1);
            EXPECT_EQ(responses[0].response->error, Error::ZOK);
        }
    };

    commit_unprocessed(storage1);
    commit_unprocessed(storage2);

    const auto get_children = [&](KeeperRocksStorage & storage)
    {
        const auto list_request = std::make_shared<ZooKeeperListRequest>();
        list_request->path = "/test";
        auto responses = storage.processRequest(list_request, 1, std::nullopt, /*check_acl=*/true, /*is_local=*/true);
        EXPECT_EQ(responses.size(), 1);
        const auto * list_response = dynamic_cast<const ListResponse *>(responses[0].response.get());
        EXPECT_TRUE(list_response);
        return list_response->names;
    };

    auto children1 = get_children(storage1);
    std::unordered_set<std::string> children1_set(children1.begin(), children1.end());

    auto children2 = get_children(storage2);
    std::unordered_set<std::string> children2_set(children2.begin(), children2.end());

    ASSERT_TRUE(children1_set == children2_set);
}

#endif
