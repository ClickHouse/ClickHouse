#include "config.h"

#if USE_NURAFT

#include <Coordination/tests/gtest_coordination_common.h>

#include <Coordination/InMemoryLogStore.h>
#include <Coordination/SummingStateMachine.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperConstants.h>
#include <Coordination/KeeperStorage.h>
#include <Common/ZooKeeper/KeeperFeatureFlags.h>
#include <Common/ZooKeeper/Types.h>
#include <Coordination/KeeperLogStore.h>
#include <Coordination/KeeperStateMachine.h>
#include <Coordination/KeeperStateManager.h>
#include <Coordination/RaftServerConfig.h>

#include <Coordination/WriteBufferFromNuraftBuffer.h>
#include <Coordination/ReadBufferFromNuraftBuffer.h>

#include <Coordination/LoggerWrapper.h>

#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/logger_useful.h>

TYPED_TEST(CoordinationTest, RaftServerConfigParse)
{
    auto parse = Coordination::RaftServerConfig::parse;
    using Cfg = std::optional<DB::RaftServerConfig>;

    EXPECT_EQ(parse(""), std::nullopt);
    EXPECT_EQ(parse("="), std::nullopt);
    EXPECT_EQ(parse("=;"), std::nullopt);
    EXPECT_EQ(parse("=;;"), std::nullopt);
    EXPECT_EQ(parse("=:80"), std::nullopt);
    EXPECT_EQ(parse("server."), std::nullopt);
    EXPECT_EQ(parse("server.=:80"), std::nullopt);
    EXPECT_EQ(parse("server.-5=1:2"), std::nullopt);
    EXPECT_EQ(parse("server.1=host;-123"), std::nullopt);
    EXPECT_EQ(parse("server.1=host:999"), (Cfg{{1, "host:999"}}));
    EXPECT_EQ(parse("server.1=host:999;learner"), (Cfg{{1, "host:999", true}}));
    EXPECT_EQ(parse("server.1=host:999;participant"), (Cfg{{1, "host:999", false}}));
    EXPECT_EQ(parse("server.1=host:999;learner;25"), (Cfg{{1, "host:999", true, 25}}));

    EXPECT_EQ(parse("server.1=127.0.0.1:80"), (Cfg{{1, "127.0.0.1:80"}}));
    EXPECT_EQ(
        parse("server.1=2001:0db8:85a3:0000:0000:8a2e:0370:7334:80"),
        (Cfg{{1, "2001:0db8:85a3:0000:0000:8a2e:0370:7334:80"}}));
}

TYPED_TEST(CoordinationTest, RaftServerClusterConfigParse)
{
    auto parse = Coordination::parseRaftServers;
    using Cfg = DB::RaftServerConfig;
    using Servers = DB::RaftServers;

    EXPECT_EQ(parse(""), Servers{});
    EXPECT_EQ(parse(","), Servers{});
    EXPECT_EQ(parse("1,2"), Servers{});
    EXPECT_EQ(parse("server.1=host:80,server.1=host2:80"), Servers{});
    EXPECT_EQ(parse("server.1=host:80,server.2=host:80"), Servers{});
    EXPECT_EQ(
        parse("server.1=host:80,server.2=host:81"),
        (Servers{Cfg{1, "host:80"}, Cfg{2, "host:81"}}));
}

TYPED_TEST(CoordinationTest, BuildTest)
{
    DB::InMemoryLogStore store;
    DB::SummingStateMachine machine;
    EXPECT_EQ(1, 1);
}

TYPED_TEST(CoordinationTest, BufferSerde)
{
    Coordination::ZooKeeperRequestPtr request = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Get);
    request->xid = 3;
    dynamic_cast<Coordination::ZooKeeperGetRequest &>(*request).path = "/path/value";

    const auto test_serde = [&](bool use_xid_64)
    {
        size_t xid_size = use_xid_64 ? sizeof(int64_t) : sizeof(int32_t);
        DB::WriteBufferFromNuraftBuffer wbuf;
        request->write(wbuf, use_xid_64);
        auto nuraft_buffer = wbuf.getBuffer();
        EXPECT_EQ(nuraft_buffer->size(), 24 + xid_size);

        DB::ReadBufferFromNuraftBuffer rbuf(nuraft_buffer);

        int32_t length;
        Coordination::read(length, rbuf);
        EXPECT_EQ(length + sizeof(length), nuraft_buffer->size());

        int64_t xid = 0;
        if (use_xid_64)
        {
            Coordination::read(xid, rbuf);
        }
        else
        {
            int32_t xid_32 = 0;
            Coordination::read(xid_32, rbuf);
            xid = xid_32;
        }

        EXPECT_EQ(xid, request->xid);

        Coordination::OpNum opnum;
        Coordination::read(opnum, rbuf);

        Coordination::ZooKeeperRequestPtr request_read = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
        request_read->xid = xid;
        request_read->readImpl(rbuf);

        EXPECT_EQ(request_read->getOpNum(), Coordination::OpNum::Get);
        EXPECT_EQ(request_read->xid, 3);
        EXPECT_EQ(dynamic_cast<Coordination::ZooKeeperGetRequest &>(*request_read).path, "/path/value");
    };

    {
        SCOPED_TRACE("32bit XID");
        test_serde(/*use_xid_64=*/false);
    }
    {
        SCOPED_TRACE("64bit XID");
        test_serde(/*use_xid_64=*/true);
    }
}

template <typename StateMachine>
struct SimpliestRaftServer
{
    SimpliestRaftServer(
        int server_id_, const std::string & hostname_, int port_, DB::KeeperContextPtr keeper_context)
        : server_id(server_id_)
        , hostname(hostname_)
        , port(port_)
        , endpoint(hostname + ":" + std::to_string(port))
        , state_machine(nuraft::cs_new<StateMachine>())
        , state_manager(nuraft::cs_new<DB::KeeperStateManager>(server_id, hostname, port, keeper_context))
    {
        state_manager->loadLogStore(1, 0);
        nuraft::raft_params params;
        params.heart_beat_interval_ = 100;
        params.election_timeout_lower_bound_ = 200;
        params.election_timeout_upper_bound_ = 400;
        params.reserved_log_items_ = 5;
        params.snapshot_distance_ = 1; /// forcefully send snapshots
        params.client_req_timeout_ = 3000;
        params.return_method_ = nuraft::raft_params::blocking;
        params.parallel_log_appending_ = true;

        nuraft::raft_server::init_options opts;
        opts.start_server_in_constructor_ = false;
        raft_instance = launcher.init(
            state_machine,
            state_manager,
            nuraft::cs_new<DB::LoggerWrapper>("ToyRaftLogger", DB::LogsLevel::trace),
            port,
            nuraft::asio_service::options{},
            params,
            opts);

        if (!raft_instance)
        {
            std::cerr << "Failed to initialize launcher" << std::endl;
            _exit(1);
        }

        state_manager->getLogStore()->setRaftServer(raft_instance);

        raft_instance->start_server(false);

        std::cout << "init Raft instance " << server_id;
        for (size_t ii = 0; ii < 20; ++ii)
        {
            if (raft_instance->is_initialized())
            {
                std::cout << " done" << std::endl;
                break;
            }
            std::cout << "." << std::flush;
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }

    ~SimpliestRaftServer()
    {
        state_manager->flushAndShutDownLogStore();
    }

    // Server ID.
    int server_id;

    // Server address.
    std::string hostname;

    // Server port.
    int port;

    std::string endpoint;

    // State machine.
    nuraft::ptr<StateMachine> state_machine;

    // State manager.
    nuraft::ptr<DB::KeeperStateManager> state_manager;

    // Raft launcher.
    nuraft::raft_launcher launcher;

    // Raft server instance.
    nuraft::ptr<nuraft::raft_server> raft_instance;
};

using SummingRaftServer = SimpliestRaftServer<DB::SummingStateMachine>;

nuraft::ptr<nuraft::buffer> getBuffer(int64_t number)
{
    nuraft::ptr<nuraft::buffer> ret = nuraft::buffer::alloc(sizeof(number));
    nuraft::buffer_serializer bs(ret);
    bs.put_raw(&number, sizeof(number));
    return ret;
}

TYPED_TEST(CoordinationTest, TestSummingRaft1)
{
    ChangelogDirTest test("./logs");
    this->setLogDirectory("./logs");
    this->setStateFileDirectory(".");

    SummingRaftServer s1(1, "localhost", 0, this->keeper_context);
    SCOPE_EXIT(if (std::filesystem::exists("./state")) std::filesystem::remove("./state"););

    /// Single node is leader
    EXPECT_EQ(s1.raft_instance->get_leader(), 1);

    auto entry1 = getBuffer(143);
    auto ret = s1.raft_instance->append_entries({entry1});
    EXPECT_TRUE(ret->get_accepted()) << "failed to replicate: entry 1" << ret->get_result_code();
    EXPECT_EQ(ret->get_result_code(), nuraft::cmd_result_code::OK) << "failed to replicate: entry 1" << ret->get_result_code();

    while (s1.state_machine->getValue() != 143)
    {
        LOG_INFO(this->log, "Waiting s1 to apply entry");
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    EXPECT_EQ(s1.state_machine->getValue(), 143);

    s1.launcher.shutdown(5);
}

template<typename Storage>
void testLogAndStateMachine(
    DB::CoordinationSettingsPtr settings,
    uint64_t total_logs,
    bool enable_compression)
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
        local_keeper_context->setRocksDBOptions();
        return local_keeper_context;
    };

    ResponsesQueue queue(std::numeric_limits<size_t>::max());
    SnapshotsQueue snapshots_queue{1};

    auto keeper_context = get_keeper_context();
    auto state_machine = std::make_shared<KeeperStateMachine<Storage>>(queue, snapshots_queue, keeper_context, nullptr);

    state_machine->init();
    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = true, .compress_logs = enable_compression, .rotate_interval = (*settings)[DB::CoordinationSetting::rotate_log_storage_interval]},
        DB::FlushSettings(),
        keeper_context);
    changelog.init(state_machine->last_commit_index() + 1, (*settings)[DB::CoordinationSetting::reserved_log_items]);

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
        if (i % (*settings)[DB::CoordinationSetting::snapshot_distance] == 0)
        {
            nuraft::snapshot s(i, 0, std::make_shared<nuraft::cluster_config>());
            nuraft::async_result<bool>::handler_type when_done
                = [&snapshot_created](bool & ret, nuraft::ptr<std::exception> & /*exception*/)
            {
                snapshot_created = ret;
                LOG_INFO(getLogger("CoordinationTest"), "Snapshot finished");
            };

            state_machine->create_snapshot(s, when_done);
            CreateSnapshotTask snapshot_task;
            bool pop_result = snapshots_queue.pop(snapshot_task);
            EXPECT_TRUE(pop_result);

            snapshot_task.create_snapshot(std::move(snapshot_task.snapshot), /*execute_only_cleanup=*/false);
        }

        if (snapshot_created && changelog.size() > (*settings)[DB::CoordinationSetting::reserved_log_items])
            changelog.compact(i - (*settings)[DB::CoordinationSetting::reserved_log_items]);
    }

    SnapshotsQueue snapshots_queue1{1};
    keeper_context = get_keeper_context();
    auto restore_machine = std::make_shared<KeeperStateMachine<Storage>>(queue, snapshots_queue1, keeper_context, nullptr);
    restore_machine->init();
    EXPECT_EQ(restore_machine->last_commit_index(), total_logs - total_logs % (*settings)[DB::CoordinationSetting::snapshot_distance]);

    DB::KeeperLogStore restore_changelog(
        DB::LogFileSettings{
            .force_sync = true, .compress_logs = enable_compression, .rotate_interval = (*settings)[DB::CoordinationSetting::rotate_log_storage_interval]},
        DB::FlushSettings(),
        keeper_context);
    restore_changelog.init(restore_machine->last_commit_index() + 1, (*settings)[DB::CoordinationSetting::reserved_log_items]);

    EXPECT_EQ(restore_changelog.size(), std::min((*settings)[DB::CoordinationSetting::reserved_log_items] + total_logs % (*settings)[DB::CoordinationSetting::snapshot_distance], total_logs));
    EXPECT_EQ(restore_changelog.next_slot(), total_logs + 1);
    if (total_logs > (*settings)[DB::CoordinationSetting::reserved_log_items] + 1)
        EXPECT_EQ(
            restore_changelog.start_index(), total_logs - total_logs % (*settings)[DB::CoordinationSetting::snapshot_distance] - (*settings)[DB::CoordinationSetting::reserved_log_items] + 1);
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
        EXPECT_EQ(source_storage.container.getValue(path).getData(), restored_storage.container.getValue(path).getData());
    }
}

TYPED_TEST(CoordinationTest, TestStateMachineAndLogStore)
{
    using namespace Coordination;
    using namespace DB;

    using Storage = typename TestFixture::Storage;

    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        (*settings)[DB::CoordinationSetting::snapshot_distance] = 10;
        (*settings)[DB::CoordinationSetting::reserved_log_items] = 10;
        (*settings)[DB::CoordinationSetting::rotate_log_storage_interval] = 10;

        testLogAndStateMachine<Storage>(settings, 37, this->enable_compression);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        (*settings)[DB::CoordinationSetting::snapshot_distance] = 10;
        (*settings)[DB::CoordinationSetting::reserved_log_items] = 10;
        (*settings)[DB::CoordinationSetting::rotate_log_storage_interval] = 10;
        testLogAndStateMachine<Storage>(settings, 11, this->enable_compression);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        (*settings)[DB::CoordinationSetting::snapshot_distance] = 10;
        (*settings)[DB::CoordinationSetting::reserved_log_items] = 10;
        (*settings)[DB::CoordinationSetting::rotate_log_storage_interval] = 10;
        testLogAndStateMachine<Storage>(settings, 40, this->enable_compression);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        (*settings)[DB::CoordinationSetting::snapshot_distance] = 10;
        (*settings)[DB::CoordinationSetting::reserved_log_items] = 20;
        (*settings)[DB::CoordinationSetting::rotate_log_storage_interval] = 30;
        testLogAndStateMachine<Storage>(settings, 40, this->enable_compression);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        (*settings)[DB::CoordinationSetting::snapshot_distance] = 10;
        (*settings)[DB::CoordinationSetting::reserved_log_items] = 0;
        (*settings)[DB::CoordinationSetting::rotate_log_storage_interval] = 10;
        testLogAndStateMachine<Storage>(settings, 40, this->enable_compression);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        (*settings)[DB::CoordinationSetting::snapshot_distance] = 1;
        (*settings)[DB::CoordinationSetting::reserved_log_items] = 1;
        (*settings)[DB::CoordinationSetting::rotate_log_storage_interval] = 32;
        testLogAndStateMachine<Storage>(settings, 32, this->enable_compression);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        (*settings)[DB::CoordinationSetting::snapshot_distance] = 10;
        (*settings)[DB::CoordinationSetting::reserved_log_items] = 7;
        (*settings)[DB::CoordinationSetting::rotate_log_storage_interval] = 1;
        testLogAndStateMachine<Storage>(settings, 33, this->enable_compression);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        (*settings)[DB::CoordinationSetting::snapshot_distance] = 37;
        (*settings)[DB::CoordinationSetting::reserved_log_items] = 1000;
        (*settings)[DB::CoordinationSetting::rotate_log_storage_interval] = 5000;
        testLogAndStateMachine<Storage>(settings, 33, this->enable_compression);
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        (*settings)[DB::CoordinationSetting::snapshot_distance] = 37;
        (*settings)[DB::CoordinationSetting::reserved_log_items] = 1000;
        (*settings)[DB::CoordinationSetting::rotate_log_storage_interval] = 5000;
        testLogAndStateMachine<Storage>(settings, 45, this->enable_compression);
    }
}

TYPED_TEST(CoordinationTest, TestEphemeralNodeRemove)
{
    using namespace Coordination;
    using namespace DB;

    ChangelogDirTest snapshots("./snapshots");
    this->setSnapshotDirectory("./snapshots");

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    ResponsesQueue queue(std::numeric_limits<size_t>::max());
    SnapshotsQueue snapshots_queue{1};

    auto state_machine = std::make_shared<KeeperStateMachine<Storage>>(queue, snapshots_queue, this->keeper_context, nullptr);
    state_machine->init();

    std::shared_ptr<ZooKeeperCreateRequest> request_c = std::make_shared<ZooKeeperCreateRequest>();
    request_c->path = "/hello";
    request_c->is_ephemeral = true;
    auto entry_c = getLogEntryFromZKRequest(0, 1, state_machine->getNextZxid(), request_c);
    state_machine->pre_commit(1, entry_c->get_buf());
    state_machine->commit(1, entry_c->get_buf());
    const auto & storage = state_machine->getStorageUnsafe();

    EXPECT_EQ(storage.committed_ephemerals.size(), 1);
    std::shared_ptr<ZooKeeperRemoveRequest> request_d = std::make_shared<ZooKeeperRemoveRequest>();
    request_d->path = "/hello";
    /// Delete from other session
    auto entry_d = getLogEntryFromZKRequest(0, 2, state_machine->getNextZxid(), request_d);
    state_machine->pre_commit(2, entry_d->get_buf());
    state_machine->commit(2, entry_d->get_buf());

    EXPECT_EQ(storage.committed_ephemerals.size(), 0);
}


TYPED_TEST(CoordinationTest, TestCreateNodeWithAuthSchemeForAclWhenAuthIsPrecommitted)
{
    using namespace Coordination;
    using namespace DB;

    ChangelogDirTest snapshots("./snapshots");
    this->setSnapshotDirectory("./snapshots");

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    ResponsesQueue queue(std::numeric_limits<size_t>::max());
    SnapshotsQueue snapshots_queue{1};

    auto state_machine = std::make_shared<KeeperStateMachine<Storage>>(queue, snapshots_queue, this->keeper_context, nullptr);
    state_machine->init();

    String user_auth_data = "test_user:test_password";
    String digest = KeeperMemoryStorage::generateDigest(user_auth_data);

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

TYPED_TEST(CoordinationTest, TestPreprocessWhenCloseSessionIsPrecommitted)
{
    using namespace Coordination;
    using namespace DB;

    ChangelogDirTest snapshots("./snapshots");
    this->setSnapshotDirectory("./snapshots");

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");
    ResponsesQueue queue(std::numeric_limits<size_t>::max());
    SnapshotsQueue snapshots_queue{1};
    int64_t session_without_auth = 1;
    int64_t session_with_auth = 2;
    size_t term = 0;

    auto state_machine = std::make_shared<KeeperStateMachine<Storage>>(queue, snapshots_queue, this->keeper_context, nullptr);
    state_machine->init();

    auto & storage = state_machine->getStorageUnsafe();
    const auto & uncommitted_state = storage.uncommitted_state;

    auto auth_req = std::make_shared<ZooKeeperAuthRequest>();
    auth_req->scheme = "digest";
    auth_req->data = "test_user:test_password";

    // Add auth data to the session
    auto auth_entry = getLogEntryFromZKRequest(term, session_with_auth, state_machine->getNextZxid(), auth_req);
    state_machine->pre_commit(1, auth_entry->get_buf());
    state_machine->commit(1, auth_entry->get_buf());

    std::string node_without_acl = "/node_without_acl";
    {
        auto create_req = std::make_shared<ZooKeeperCreateRequest>();
        create_req->path = node_without_acl;
        create_req->data = "notmodified";
        auto create_entry = getLogEntryFromZKRequest(term, session_with_auth, state_machine->getNextZxid(), create_req);
        state_machine->pre_commit(2, create_entry->get_buf());
        state_machine->commit(2, create_entry->get_buf());
        ASSERT_TRUE(storage.container.contains(node_without_acl));
    }

    std::string node_with_acl = "/node_with_acl";
    {
        auto create_req = std::make_shared<ZooKeeperCreateRequest>();
        create_req->path = node_with_acl;
        create_req->data = "notmodified";
        create_req->acls = {{.permissions = ACL::All, .scheme = "auth", .id = ""}};
        auto create_entry = getLogEntryFromZKRequest(term, session_with_auth, state_machine->getNextZxid(), create_req);
        state_machine->pre_commit(3, create_entry->get_buf());
        state_machine->commit(3, create_entry->get_buf());
        ASSERT_TRUE(storage.container.contains(node_with_acl));
    }

    auto set_req_with_acl = std::make_shared<ZooKeeperSetRequest>();
    set_req_with_acl->path = node_with_acl;
    set_req_with_acl->data = "modified";

    auto set_req_without_acl = std::make_shared<ZooKeeperSetRequest>();
    set_req_without_acl->path = node_without_acl;
    set_req_without_acl->data = "modified";

    const auto reset_node_value
        = [&](const auto & path) { storage.container.updateValue(path, [](auto & node) { node.setData("notmodified"); }); };

    auto close_req = std::make_shared<ZooKeeperCloseRequest>();

    {
        SCOPED_TRACE("Session with Auth");

        // test we can modify both nodes
        auto set_entry = getLogEntryFromZKRequest(term, session_with_auth, state_machine->getNextZxid(), set_req_with_acl);
        state_machine->pre_commit(5, set_entry->get_buf());
        state_machine->commit(5, set_entry->get_buf());
        ASSERT_TRUE(storage.container.find(node_with_acl)->value.getData() == "modified");
        reset_node_value(node_with_acl);

        set_entry = getLogEntryFromZKRequest(term, session_with_auth, state_machine->getNextZxid(), set_req_without_acl);
        state_machine->pre_commit(6, set_entry->get_buf());
        state_machine->commit(6, set_entry->get_buf());
        ASSERT_TRUE(storage.container.find(node_without_acl)->value.getData() == "modified");
        reset_node_value(node_without_acl);

        auto close_entry = getLogEntryFromZKRequest(term, session_with_auth, state_machine->getNextZxid(), close_req);

        // Pre-commit close session
        state_machine->pre_commit(7, close_entry->get_buf());

        /// will be rejected because we don't have required auth
        auto set_entry_with_acl = getLogEntryFromZKRequest(term, session_with_auth, state_machine->getNextZxid(), set_req_with_acl);
        state_machine->pre_commit(8, set_entry_with_acl->get_buf());

        /// will be accepted because no ACL
        auto set_entry_without_acl = getLogEntryFromZKRequest(term, session_with_auth, state_machine->getNextZxid(), set_req_without_acl);
        state_machine->pre_commit(9, set_entry_without_acl->get_buf());

        ASSERT_TRUE(uncommitted_state.getNode(node_with_acl)->getData() == "notmodified");
        ASSERT_TRUE(uncommitted_state.getNode(node_without_acl)->getData() == "modified");

        state_machine->rollback(9, set_entry_without_acl->get_buf());
        state_machine->rollback(8, set_entry_with_acl->get_buf());

        // let's commit close and verify we get same outcome
        state_machine->commit(7, close_entry->get_buf());

        /// will be rejected because we don't have required auth
        set_entry_with_acl = getLogEntryFromZKRequest(term, session_with_auth, state_machine->getNextZxid(), set_req_with_acl);
        state_machine->pre_commit(8, set_entry_with_acl->get_buf());

        /// will be accepted because no ACL
        set_entry_without_acl = getLogEntryFromZKRequest(term, session_with_auth, state_machine->getNextZxid(), set_req_without_acl);
        state_machine->pre_commit(9, set_entry_without_acl->get_buf());

        ASSERT_TRUE(uncommitted_state.getNode(node_with_acl)->getData() == "notmodified");
        ASSERT_TRUE(uncommitted_state.getNode(node_without_acl)->getData() == "modified");

        state_machine->commit(8, set_entry_with_acl->get_buf());
        state_machine->commit(9, set_entry_without_acl->get_buf());

        ASSERT_TRUE(storage.container.find(node_with_acl)->value.getData() == "notmodified");
        ASSERT_TRUE(storage.container.find(node_without_acl)->value.getData() == "modified");

        reset_node_value(node_without_acl);
    }

    {
        SCOPED_TRACE("Session without Auth");

        // test we can modify only node without acl
        auto set_entry = getLogEntryFromZKRequest(term, session_without_auth, state_machine->getNextZxid(), set_req_with_acl);
        state_machine->pre_commit(10, set_entry->get_buf());
        state_machine->commit(10, set_entry->get_buf());
        ASSERT_TRUE(storage.container.find(node_with_acl)->value.getData() == "notmodified");

        set_entry = getLogEntryFromZKRequest(term, session_without_auth, state_machine->getNextZxid(), set_req_without_acl);
        state_machine->pre_commit(11, set_entry->get_buf());
        state_machine->commit(11, set_entry->get_buf());
        ASSERT_TRUE(storage.container.find(node_without_acl)->value.getData() == "modified");
        reset_node_value(node_without_acl);

        auto close_entry = getLogEntryFromZKRequest(term, session_without_auth, state_machine->getNextZxid(), close_req);

        // Pre-commit close session
        state_machine->pre_commit(12, close_entry->get_buf());

        /// will be rejected because we don't have required auth
        auto set_entry_with_acl = getLogEntryFromZKRequest(term, session_without_auth, state_machine->getNextZxid(), set_req_with_acl);
        state_machine->pre_commit(13, set_entry_with_acl->get_buf());

        /// will be accepted because no ACL
        auto set_entry_without_acl = getLogEntryFromZKRequest(term, session_without_auth, state_machine->getNextZxid(), set_req_without_acl);
        state_machine->pre_commit(14, set_entry_without_acl->get_buf());

        ASSERT_TRUE(uncommitted_state.getNode(node_with_acl)->getData() == "notmodified");
        ASSERT_TRUE(uncommitted_state.getNode(node_without_acl)->getData() == "modified");

        state_machine->rollback(14, set_entry_without_acl->get_buf());
        state_machine->rollback(13, set_entry_with_acl->get_buf());

        // let's commit close and verify we get same outcome
        state_machine->commit(12, close_entry->get_buf());

        /// will be rejected because we don't have required auth
        set_entry_with_acl = getLogEntryFromZKRequest(term, session_without_auth, state_machine->getNextZxid(), set_req_with_acl);
        state_machine->pre_commit(13, set_entry_with_acl->get_buf());

        /// will be accepted because no ACL
        set_entry_without_acl = getLogEntryFromZKRequest(term, session_without_auth, state_machine->getNextZxid(), set_req_without_acl);
        state_machine->pre_commit(14, set_entry_without_acl->get_buf());

        ASSERT_TRUE(uncommitted_state.getNode(node_with_acl)->getData() == "notmodified");
        ASSERT_TRUE(uncommitted_state.getNode(node_without_acl)->getData() == "modified");

        state_machine->commit(13, set_entry_with_acl->get_buf());
        state_machine->commit(14, set_entry_without_acl->get_buf());

        ASSERT_TRUE(storage.container.find(node_with_acl)->value.getData() == "notmodified");
        ASSERT_TRUE(storage.container.find(node_without_acl)->value.getData() == "modified");

        reset_node_value(node_without_acl);
    }
}

TYPED_TEST(CoordinationTest, TestMultiRequestWithNoAuth)
{
    using namespace Coordination;
    using namespace DB;

    ChangelogDirTest snapshots("./snapshots");
    this->setSnapshotDirectory("./snapshots");

    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");
    ResponsesQueue queue(std::numeric_limits<size_t>::max());
    SnapshotsQueue snapshots_queue{1};
    int64_t session_without_auth = 1;
    int64_t session_with_auth = 2;
    size_t term = 0;

    auto state_machine = std::make_shared<KeeperStateMachine<Storage>>(queue, snapshots_queue, this->keeper_context, nullptr);
    state_machine->init();

    auto & storage = state_machine->getStorageUnsafe();

    auto auth_req = std::make_shared<ZooKeeperAuthRequest>();
    auth_req->scheme = "digest";
    auth_req->data = "test_user:test_password";

    // Add auth data to the session
    auto auth_entry = getLogEntryFromZKRequest(term, session_with_auth, state_machine->getNextZxid(), auth_req);
    state_machine->pre_commit(1, auth_entry->get_buf());
    state_machine->commit(1, auth_entry->get_buf());

    std::string node_with_acl = "/node_with_acl";
    {
        auto create_req = std::make_shared<ZooKeeperCreateRequest>();
        create_req->path = node_with_acl;
        create_req->data = "notmodified";
        create_req->acls = {{.permissions = ACL::Read, .scheme = "auth", .id = ""}};
        auto create_entry = getLogEntryFromZKRequest(term, session_with_auth, state_machine->getNextZxid(), create_req);
        state_machine->pre_commit(3, create_entry->get_buf());
        state_machine->commit(3, create_entry->get_buf());
        ASSERT_TRUE(storage.container.contains(node_with_acl));
    }
    Requests ops;
    ops.push_back(zkutil::makeSetRequest(node_with_acl, "modified", -1));
    ops.push_back(zkutil::makeCheckRequest("/nonexistentnode", -1));
    auto multi_req = std::make_shared<ZooKeeperMultiRequest>(ops, ACLs{});
    auto multi_entry = getLogEntryFromZKRequest(term, session_without_auth, state_machine->getNextZxid(), multi_req);
    state_machine->pre_commit(4, multi_entry->get_buf());
    state_machine->commit(4, multi_entry->get_buf());

    auto node_it = storage.container.find(node_with_acl);
    ASSERT_FALSE(node_it == storage.container.end());
    ASSERT_TRUE(node_it->value.getData() == "notmodified");
}

TYPED_TEST(CoordinationTest, TestSetACLWithAuthSchemeForAclWhenAuthIsPrecommitted)
{
    using namespace Coordination;
    using namespace DB;

    ChangelogDirTest snapshots("./snapshots");
    this->setSnapshotDirectory("./snapshots");

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    ResponsesQueue queue(std::numeric_limits<size_t>::max());
    SnapshotsQueue snapshots_queue{1};

    using Storage = typename TestFixture::Storage;
    auto state_machine = std::make_shared<KeeperStateMachine<Storage>>(queue, snapshots_queue, this->keeper_context, nullptr);
    state_machine->init();

    String user_auth_data = "test_user:test_password";
    String digest = KeeperMemoryStorage::generateDigest(user_auth_data);

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

TYPED_TEST(CoordinationTest, TestSessionExpiryQueue)
{
    using namespace Coordination;
    SessionExpiryQueue queue(500);

    queue.addNewSessionOrUpdate(1, 1000);

    for (size_t i = 0; i < 2; ++i)
    {
        EXPECT_EQ(queue.getExpiredSessions(), std::vector<int64_t>({}));
        std::this_thread::sleep_for(std::chrono::milliseconds(400));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(700));
    EXPECT_EQ(queue.getExpiredSessions(), std::vector<int64_t>({1}));
}

TYPED_TEST(CoordinationTest, TestDurableState)
{
    ChangelogDirTest logs("./logs");
    this->setLogDirectory("./logs");
    this->setStateFileDirectory(".");

    auto state = nuraft::cs_new<nuraft::srv_state>();
    std::optional<DB::KeeperStateManager> state_manager;

    const auto reload_state_manager = [&]
    {
        state_manager.emplace(1, "localhost", 9181, this->keeper_context);
        state_manager->loadLogStore(1, 0);
    };

    reload_state_manager();
    ASSERT_EQ(state_manager->read_state(), nullptr);

    state->set_term(1);
    state->set_voted_for(2);
    state->allow_election_timer(true);
    state_manager->save_state(*state);

    const auto assert_read_state = [&]
    {
        auto read_state = state_manager->read_state();
        ASSERT_NE(read_state, nullptr);
        ASSERT_EQ(read_state->get_term(), state->get_term());
        ASSERT_EQ(read_state->get_voted_for(), state->get_voted_for());
        ASSERT_EQ(read_state->is_election_timer_allowed(), state->is_election_timer_allowed());
    };

    assert_read_state();

    reload_state_manager();
    assert_read_state();

    {
        SCOPED_TRACE("Read from corrupted file");
        state_manager.reset();
        DB::WriteBufferFromFile write_buf("./state", DB::DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY);
        write_buf.seek(20, SEEK_SET);
        DB::writeIntBinary(31, write_buf);
        write_buf.sync();
        write_buf.close();
        reload_state_manager();
#    ifdef NDEBUG
        ASSERT_EQ(state_manager->read_state(), nullptr);
#    else
        ASSERT_THROW(state_manager->read_state(), DB::Exception);
#    endif
    }

    {
        SCOPED_TRACE("Read from file with invalid size");
        state_manager.reset();

        DB::WriteBufferFromFile write_buf("./state", DB::DBMS_DEFAULT_BUFFER_SIZE, O_TRUNC | O_CREAT | O_WRONLY);
        DB::writeIntBinary(20, write_buf);
        write_buf.sync();
        write_buf.close();
        reload_state_manager();
        ASSERT_EQ(state_manager->read_state(), nullptr);
    }

    {
        SCOPED_TRACE("State file is missing");
        state_manager.reset();
        std::filesystem::remove("./state");
        reload_state_manager();
        ASSERT_EQ(state_manager->read_state(), nullptr);
    }
}

TYPED_TEST(CoordinationTest, TestFeatureFlags)
{
    using namespace Coordination;
    using Storage = typename TestFixture::Storage;

    ChangelogDirTest rocks("./rocksdb");
    this->setRocksDBDirectory("./rocksdb");

    Storage storage{500, "", this->keeper_context};
    auto request = std::make_shared<ZooKeeperGetRequest>();
    request->path = DB::keeper_api_feature_flags_path;
    auto responses = storage.processRequest(request, 0, std::nullopt, true, true);
    const auto & get_response = getSingleResponse<ZooKeeperGetResponse>(responses);
    DB::KeeperFeatureFlags feature_flags;
    feature_flags.setFeatureFlags(get_response.data);
    ASSERT_TRUE(feature_flags.isEnabled(KeeperFeatureFlag::FILTERED_LIST));
    ASSERT_TRUE(feature_flags.isEnabled(KeeperFeatureFlag::MULTI_READ));
    ASSERT_FALSE(feature_flags.isEnabled(KeeperFeatureFlag::CHECK_NOT_EXISTS));
    ASSERT_FALSE(feature_flags.isEnabled(KeeperFeatureFlag::CREATE_IF_NOT_EXISTS));
    ASSERT_FALSE(feature_flags.isEnabled(KeeperFeatureFlag::REMOVE_RECURSIVE));
}

#endif
