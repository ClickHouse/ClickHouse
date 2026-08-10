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
#include <IO/WriteBufferFromFileDecorator.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>

#include <Poco/Util/XMLConfiguration.h>

#include <algorithm>
#include <limits>
#include <sstream>

TEST(CoordinationSettingsValidation, RejectZeroBatchSizes)
{
    auto load = [](const std::string & xml)
    {
        std::istringstream stream(xml); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(stream);
        DB::CoordinationSettings settings;
        settings.loadFromConfig("keeper_server.coordination_settings", *config);
    };

    /// A zero max_requests_batch_size caused an infinite append-entries loop in a multi-node
    /// setup, because it defaults max_requests_append_size to zero too. See issue #84099.
    EXPECT_THROW(
        load("<clickhouse><keeper_server><coordination_settings>"
             "<max_requests_batch_size>0</max_requests_batch_size>"
             "</coordination_settings></keeper_server></clickhouse>"),
        DB::Exception);

    /// max_requests_append_size feeds NuRaft's max_append_size_ directly and must not be zero either.
    EXPECT_THROW(
        load("<clickhouse><keeper_server><coordination_settings>"
             "<max_requests_append_size>0</max_requests_append_size>"
             "</coordination_settings></keeper_server></clickhouse>"),
        DB::Exception);

    /// Non-zero values are accepted.
    EXPECT_NO_THROW(
        load("<clickhouse><keeper_server><coordination_settings>"
             "<max_requests_batch_size>1</max_requests_batch_size>"
             "<max_requests_append_size>1</max_requests_append_size>"
             "</coordination_settings></keeper_server></clickhouse>"));
}

TEST_P(CoordinationTest, RaftServerConfigParse)
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

TEST_P(CoordinationTest, RaftServerClusterConfigParse)
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

TEST_P(CoordinationTest, BuildTest)
{
    DB::InMemoryLogStore store;
    DB::SummingStateMachine machine;
    EXPECT_EQ(1, 1);
}

TEST_P(CoordinationTest, BufferSerde)
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

        int32_t length = {};
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

        Coordination::OpNum opnum = {};
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

TEST_P(CoordinationTest, ContainerCreateSerde)
{
    auto request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
    request->path = "/container";
    request->is_container = true;
    EXPECT_EQ(request->getOpNum(), Coordination::OpNum::CreateContainer);

    DB::WriteBufferFromNuraftBuffer wbuf;
    request->write(wbuf, /*use_xid_64=*/true);
    auto nuraft_buffer = wbuf.getBuffer();

    DB::ReadBufferFromNuraftBuffer rbuf(nuraft_buffer);
    int32_t length = 0;
    Coordination::read(length, rbuf);
    int64_t xid = 0;
    Coordination::read(xid, rbuf);
    Coordination::OpNum opnum = {};
    Coordination::read(opnum, rbuf);
    EXPECT_EQ(opnum, Coordination::OpNum::CreateContainer);

    auto request_read = Coordination::ZooKeeperRequestFactory::instance().get(opnum);
    request_read->readImpl(rbuf);
    auto & create_read = dynamic_cast<Coordination::ZooKeeperCreateRequest &>(*request_read);
    EXPECT_TRUE(create_read.is_container);
    EXPECT_FALSE(create_read.is_sequential);
    EXPECT_FALSE(create_read.is_ephemeral);
    EXPECT_FALSE(create_read.include_ttl);
}

TEST_P(CoordinationTest, ContainerCreateModeMismatchRejected)
{
    /// An opnum/create-mode mismatch must be rejected, not silently accepted as both a
    /// container and a sequential node — that would diverge the Raft log across replicas.
    DB::WriteBufferFromNuraftBuffer wbuf;
    Coordination::write(std::string{"/container"}, wbuf); /// path
    Coordination::write(std::string{}, wbuf);             /// data
    Coordination::write(Coordination::ACLs{}, wbuf);      /// acls
    Coordination::write(static_cast<int32_t>(2), wbuf);   /// CreateMode::PERSISTENT_SEQUENTIAL

    auto request_read = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::CreateContainer);
    DB::ReadBufferFromNuraftBuffer rbuf(wbuf.getBuffer());
    EXPECT_THROW(request_read->readImpl(rbuf), Coordination::Exception);
}

TEST_P(CoordinationTest, Create2WithContainerFlagRejected)
{
    /// A Create2 opnum carrying the CONTAINER flag must be rejected: getOpNum() prioritizes
    /// include_stats over is_container, so this combination would otherwise validate and log
    /// as Create2 (gated by CREATE_WITH_STATS) while still creating a container node (gated
    /// separately by CREATE_CONTAINER) — bypassing the CreateContainer feature-flag gate.
    DB::WriteBufferFromNuraftBuffer wbuf;
    Coordination::write(std::string{"/container"}, wbuf); /// path
    Coordination::write(std::string{}, wbuf);             /// data
    Coordination::write(Coordination::ACLs{}, wbuf);      /// acls
    Coordination::write(static_cast<int32_t>(4), wbuf);   /// CreateMode::CONTAINER

    auto request_read = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Create2);
    DB::ReadBufferFromNuraftBuffer rbuf(wbuf.getBuffer());
    EXPECT_THROW(request_read->readImpl(rbuf), Coordination::Exception);
}

TEST_P(CoordinationTest, PlainCreateWithContainerFlagRejected)
{
    DB::WriteBufferFromNuraftBuffer wbuf;
    Coordination::write(std::string{"/container"}, wbuf); /// path
    Coordination::write(std::string{}, wbuf);             /// data
    Coordination::write(Coordination::ACLs{}, wbuf);      /// acls
    Coordination::write(static_cast<int32_t>(4), wbuf);   /// CreateMode::CONTAINER

    auto request_read = Coordination::ZooKeeperRequestFactory::instance().get(Coordination::OpNum::Create);
    DB::ReadBufferFromNuraftBuffer rbuf(wbuf.getBuffer());
    EXPECT_THROW(request_read->readImpl(rbuf), Coordination::Exception);
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

static nuraft::ptr<nuraft::buffer> getBuffer(int64_t number)
{
    nuraft::ptr<nuraft::buffer> ret = nuraft::buffer::alloc(sizeof(number));
    nuraft::buffer_serializer bs(ret);
    bs.put_raw(&number, sizeof(number));
    return ret;
}

TEST_P(CoordinationTest, TestSummingRaft1)
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

static void testLogAndStateMachine(
    DB::CoordinationSettingsPtr settings,
    uint64_t total_logs,
    const StorageTypeAndCompression & param)
{
    using namespace Coordination;
    using namespace DB;

    ChangelogDirTest snapshots("./snapshots");
    ChangelogDirTest logs("./logs");

    auto get_keeper_context = [&]
    {
        auto local_keeper_context = makeKeeperContext(param.use_lsmt_storage, settings);
        local_keeper_context->setSnapshotDisk(std::make_shared<DiskLocal>("SnapshotDisk", "./snapshots"));
        local_keeper_context->setLogDisk(std::make_shared<DiskLocal>("LogDisk", "./logs"));
        return local_keeper_context;
    };

    SnapshotsQueue snapshots_queue{1};

    auto keeper_context = get_keeper_context();
    auto state_machine = std::make_shared<KeeperStateMachine>(nullptr, snapshots_queue, keeper_context, nullptr);

    state_machine->init();
    DB::KeeperLogStore changelog(
        DB::LogFileSettings{
            .force_sync = true, .compress_logs = param.enable_compression, .rotate_interval = (*settings)[DB::CoordinationSetting::rotate_log_storage_interval]},
        DB::FlushSettings(),
        keeper_context);
    changelog.init(state_machine->last_commit_index(), (*settings)[DB::CoordinationSetting::reserved_log_items]);

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
    auto restore_machine = std::make_shared<KeeperStateMachine>(nullptr, snapshots_queue1, keeper_context, nullptr);
    restore_machine->init();
    EXPECT_EQ(restore_machine->last_commit_index(), total_logs - total_logs % (*settings)[DB::CoordinationSetting::snapshot_distance]);

    DB::KeeperLogStore restore_changelog(
        DB::LogFileSettings{
            .force_sync = true, .compress_logs = param.enable_compression, .rotate_interval = (*settings)[DB::CoordinationSetting::rotate_log_storage_interval]},
        DB::FlushSettings(),
        keeper_context);
    restore_changelog.init(restore_machine->last_commit_index(), (*settings)[DB::CoordinationSetting::reserved_log_items]);

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

    EXPECT_EQ(source_storage.getStorageStats().nodes_count, restored_storage.getStorageStats().nodes_count);
    for (size_t i = 1; i < total_logs + 1; ++i)
    {
        auto path = "/hello_" + std::to_string(i);
        EXPECT_EQ(committedNodeData(source_storage, path), committedNodeData(restored_storage, path));
    }
}

TEST_P(CoordinationTestWithCompression, TestStateMachineAndLogStore)
{
    using namespace Coordination;
    using namespace DB;

    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        (*settings)[DB::CoordinationSetting::snapshot_distance] = 10;
        (*settings)[DB::CoordinationSetting::reserved_log_items] = 10;
        (*settings)[DB::CoordinationSetting::rotate_log_storage_interval] = 10;

        testLogAndStateMachine(settings, 37, GetParam());
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        (*settings)[DB::CoordinationSetting::snapshot_distance] = 10;
        (*settings)[DB::CoordinationSetting::reserved_log_items] = 10;
        (*settings)[DB::CoordinationSetting::rotate_log_storage_interval] = 10;
        testLogAndStateMachine(settings, 11, GetParam());
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        (*settings)[DB::CoordinationSetting::snapshot_distance] = 10;
        (*settings)[DB::CoordinationSetting::reserved_log_items] = 10;
        (*settings)[DB::CoordinationSetting::rotate_log_storage_interval] = 10;
        testLogAndStateMachine(settings, 40, GetParam());
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        (*settings)[DB::CoordinationSetting::snapshot_distance] = 10;
        (*settings)[DB::CoordinationSetting::reserved_log_items] = 20;
        (*settings)[DB::CoordinationSetting::rotate_log_storage_interval] = 30;
        testLogAndStateMachine(settings, 40, GetParam());
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        (*settings)[DB::CoordinationSetting::snapshot_distance] = 10;
        (*settings)[DB::CoordinationSetting::reserved_log_items] = 0;
        (*settings)[DB::CoordinationSetting::rotate_log_storage_interval] = 10;
        testLogAndStateMachine(settings, 40, GetParam());
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        (*settings)[DB::CoordinationSetting::snapshot_distance] = 1;
        (*settings)[DB::CoordinationSetting::reserved_log_items] = 1;
        (*settings)[DB::CoordinationSetting::rotate_log_storage_interval] = 32;
        testLogAndStateMachine(settings, 32, GetParam());
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        (*settings)[DB::CoordinationSetting::snapshot_distance] = 10;
        (*settings)[DB::CoordinationSetting::reserved_log_items] = 7;
        (*settings)[DB::CoordinationSetting::rotate_log_storage_interval] = 1;
        testLogAndStateMachine(settings, 33, GetParam());
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        (*settings)[DB::CoordinationSetting::snapshot_distance] = 37;
        (*settings)[DB::CoordinationSetting::reserved_log_items] = 1000;
        (*settings)[DB::CoordinationSetting::rotate_log_storage_interval] = 5000;
        testLogAndStateMachine(settings, 33, GetParam());
    }
    {
        CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
        (*settings)[DB::CoordinationSetting::snapshot_distance] = 37;
        (*settings)[DB::CoordinationSetting::reserved_log_items] = 1000;
        (*settings)[DB::CoordinationSetting::rotate_log_storage_interval] = 5000;
        testLogAndStateMachine(settings, 45, GetParam());
    }
}

TEST_P(CoordinationTest, TestEphemeralNodeRemove)
{
    using namespace Coordination;
    using namespace DB;

    ChangelogDirTest snapshots("./snapshots");
    this->setSnapshotDirectory("./snapshots");

    SnapshotsQueue snapshots_queue{1};

    auto state_machine = std::make_shared<KeeperStateMachine>(nullptr, snapshots_queue, this->keeper_context, nullptr);
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


TEST_P(CoordinationTest, TestCreateNodeWithAuthSchemeForAclWhenAuthIsPrecommitted)
{
    using namespace Coordination;
    using namespace DB;

    ChangelogDirTest snapshots("./snapshots");
    this->setSnapshotDirectory("./snapshots");

    SnapshotsQueue snapshots_queue{1};

    auto state_machine = std::make_shared<KeeperStateMachine>(nullptr, snapshots_queue, this->keeper_context, nullptr);
    state_machine->init();

    String user_auth_data = "test_user:test_password";
    String digest = KeeperStorage::generateDigest(user_auth_data);

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

    auto & storage = state_machine->getStorageUnsafe();
    ASSERT_TRUE(storage.nodes_storage->getUncommittedNodeSimple(node_path, /*out_stats=*/nullptr, /*out_data=*/nullptr));

    // commit log entries
    state_machine->commit(1, auth_entry->get_buf());
    state_machine->commit(2, create_entry->get_buf());

    ASSERT_TRUE(storage.nodes_storage->getUncommittedNodeSimple(node_path, /*out_stats=*/nullptr, /*out_data=*/nullptr));
    auto acls = getUncommittedACLs(storage, node_path);
    ASSERT_EQ(acls.size(), 1);
    EXPECT_EQ(acls[0].scheme, "digest");
    EXPECT_EQ(acls[0].id, digest);
    EXPECT_EQ(acls[0].permissions, 31);
}

TEST_P(CoordinationTest, TestPreprocessWhenCloseSessionIsPrecommitted)
{
    using namespace Coordination;
    using namespace DB;

    ChangelogDirTest snapshots("./snapshots");
    this->setSnapshotDirectory("./snapshots");

    SnapshotsQueue snapshots_queue{1};
    int64_t session_without_auth = 1;
    int64_t session_with_auth = 2;
    size_t term = 0;

    auto state_machine = std::make_shared<KeeperStateMachine>(nullptr, snapshots_queue, this->keeper_context, nullptr);
    state_machine->init();

    auto & storage = state_machine->getStorageUnsafe();

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
        ASSERT_TRUE(committedNodeExists(storage, node_without_acl));
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
        ASSERT_TRUE(committedNodeExists(storage, node_with_acl));
    }

    auto set_req_with_acl = std::make_shared<ZooKeeperSetRequest>();
    set_req_with_acl->path = node_with_acl;
    set_req_with_acl->data = "modified";

    auto set_req_without_acl = std::make_shared<ZooKeeperSetRequest>();
    set_req_without_acl->path = node_without_acl;
    set_req_without_acl->data = "modified";

    const auto reset_node_value = [&](std::string_view path)
    { storage.nodes_storage->updateCommittedNode(path, /*new_stats=*/std::nullopt, /*new_data=*/"notmodified", /*out_digest=*/nullptr); };

    auto close_req = std::make_shared<ZooKeeperCloseRequest>();

    {
        SCOPED_TRACE("Session with Auth");

        // test we can modify both nodes
        auto set_entry = getLogEntryFromZKRequest(term, session_with_auth, state_machine->getNextZxid(), set_req_with_acl);
        state_machine->pre_commit(5, set_entry->get_buf());
        state_machine->commit(5, set_entry->get_buf());
        ASSERT_EQ(committedNodeData(storage, node_with_acl), "modified");
        reset_node_value(node_with_acl);

        set_entry = getLogEntryFromZKRequest(term, session_with_auth, state_machine->getNextZxid(), set_req_without_acl);
        state_machine->pre_commit(6, set_entry->get_buf());
        state_machine->commit(6, set_entry->get_buf());
        ASSERT_EQ(committedNodeData(storage, node_without_acl), "modified");
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

        ASSERT_EQ(uncommittedNodeData(storage, node_with_acl), "notmodified");
        ASSERT_EQ(uncommittedNodeData(storage, node_without_acl), "modified");

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

        ASSERT_EQ(uncommittedNodeData(storage, node_with_acl), "notmodified");
        ASSERT_EQ(uncommittedNodeData(storage, node_without_acl), "modified");

        state_machine->commit(8, set_entry_with_acl->get_buf());
        state_machine->commit(9, set_entry_without_acl->get_buf());

        ASSERT_EQ(committedNodeData(storage, node_with_acl), "notmodified");
        ASSERT_EQ(committedNodeData(storage, node_without_acl), "modified");

        reset_node_value(node_without_acl);
    }

    {
        SCOPED_TRACE("Session without Auth");

        // test we can modify only node without acl
        auto set_entry = getLogEntryFromZKRequest(term, session_without_auth, state_machine->getNextZxid(), set_req_with_acl);
        state_machine->pre_commit(10, set_entry->get_buf());
        state_machine->commit(10, set_entry->get_buf());
        ASSERT_EQ(committedNodeData(storage, node_with_acl), "notmodified");

        set_entry = getLogEntryFromZKRequest(term, session_without_auth, state_machine->getNextZxid(), set_req_without_acl);
        state_machine->pre_commit(11, set_entry->get_buf());
        state_machine->commit(11, set_entry->get_buf());
        ASSERT_EQ(committedNodeData(storage, node_without_acl), "modified");
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

        ASSERT_EQ(uncommittedNodeData(storage, node_with_acl), "notmodified");
        ASSERT_EQ(uncommittedNodeData(storage, node_without_acl), "modified");

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

        ASSERT_EQ(uncommittedNodeData(storage, node_with_acl), "notmodified");
        ASSERT_EQ(uncommittedNodeData(storage, node_without_acl), "modified");

        state_machine->commit(13, set_entry_with_acl->get_buf());
        state_machine->commit(14, set_entry_without_acl->get_buf());

        ASSERT_EQ(committedNodeData(storage, node_with_acl), "notmodified");
        ASSERT_EQ(committedNodeData(storage, node_without_acl), "modified");

        reset_node_value(node_without_acl);
    }
}

TEST_P(CoordinationTest, TestMultiRequestWithNoAuth)
{
    using namespace Coordination;
    using namespace DB;

    ChangelogDirTest snapshots("./snapshots");
    this->setSnapshotDirectory("./snapshots");

    SnapshotsQueue snapshots_queue{1};
    int64_t session_without_auth = 1;
    int64_t session_with_auth = 2;
    size_t term = 0;

    auto state_machine = std::make_shared<KeeperStateMachine>(nullptr, snapshots_queue, this->keeper_context, nullptr);
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
        ASSERT_TRUE(committedNodeExists(storage, node_with_acl));
    }
    Requests ops;
    ops.push_back(zkutil::makeSetRequest(node_with_acl, "modified", -1));
    ops.push_back(zkutil::makeCheckRequest("/nonexistentnode", -1));
    auto multi_req = std::make_shared<ZooKeeperMultiRequest>(ops, ACLs{});
    auto multi_entry = getLogEntryFromZKRequest(term, session_without_auth, state_machine->getNextZxid(), multi_req);
    state_machine->pre_commit(4, multi_entry->get_buf());
    state_machine->commit(4, multi_entry->get_buf());

    ASSERT_TRUE(committedNodeExists(storage, node_with_acl));
    ASSERT_EQ(committedNodeData(storage, node_with_acl), "notmodified");
}

TEST_P(CoordinationTest, TestSetACLWithAuthSchemeForAclWhenAuthIsPrecommitted)
{
    using namespace Coordination;
    using namespace DB;

    ChangelogDirTest snapshots("./snapshots");
    this->setSnapshotDirectory("./snapshots");


    SnapshotsQueue snapshots_queue{1};

    auto state_machine = std::make_shared<KeeperStateMachine>(nullptr, snapshots_queue, this->keeper_context, nullptr);
    state_machine->init();

    String user_auth_data = "test_user:test_password";
    String digest = KeeperStorage::generateDigest(user_auth_data);

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

    auto & storage = state_machine->getStorageUnsafe();
    ASSERT_TRUE(storage.nodes_storage->getUncommittedNodeSimple(node_path, /*out_stats=*/nullptr, /*out_data=*/nullptr));

    auto acls = getUncommittedACLs(storage, node_path);
    ASSERT_EQ(acls.size(), 1);
    EXPECT_EQ(acls[0].scheme, "digest");
    EXPECT_EQ(acls[0].id, digest);
    EXPECT_EQ(acls[0].permissions, 31);
}

TEST_P(CoordinationTest, TestSessionExpiryQueue)
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

TEST_P(CoordinationTest, TestDurableState)
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

        /// The attempt that reports the corruption must not consume the evidence. Were the file
        /// deleted here, the restart after a failed start would find no state at all and NuRaft
        /// would silently resume at term 0 with an empty vote, which is worse than not starting.
        ASSERT_TRUE(std::filesystem::exists("./state"));
        reload_state_manager();
#    ifdef NDEBUG
        ASSERT_EQ(state_manager->read_state(), nullptr);
#    else
        ASSERT_THROW(state_manager->read_state(), DB::Exception);
#    endif
        ASSERT_TRUE(std::filesystem::exists("./state"));
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

namespace
{

/// Test disk: throws when the given file is (re)opened for writing, and rejects `moveFile`
/// (as plain and `s3_plain` metadata do).
class ThrowingStateDisk : public DB::DiskLocal
{
public:
    ThrowingStateDisk(const std::string & disk_name, const std::string & disk_path, std::string fail_path_)
        : DB::DiskLocal(disk_name, disk_path), fail_path(std::move(fail_path_))
    {
    }

    void arm() { armed = true; }
    void disarm() { armed = false; }

    void armReads() { reads_armed = true; }
    void disarmReads() { reads_armed = false; }

    std::unique_ptr<DB::WriteBufferFromFileBase>
    writeFile(const String & path, size_t buf_size, DB::WriteMode mode, const DB::WriteSettings & settings) override
    {
        auto inner = DB::DiskLocal::writeFile(path, buf_size, mode, settings);
        /// Only a rewrite truncates, so only a rewrite leaves the torn file these tests are
        /// about. An append open, which `save_state` also does to sync an existing file, must not
        /// be the thing that fails, or the injection would land before the rewrite window.
        if (armed && path == fail_path && mode == DB::WriteMode::Rewrite)
            throw std::runtime_error("Injected state write failure");
        return inner;
    }

    void prepareRead(
        const String & path, const DB::ReadSettings & settings, std::optional<size_t> read_hint, DB::ReadPipeline & pipeline)
        const override
    {
        if (reads_armed && path == fail_path)
            throw std::runtime_error("Injected state read failure");
        DB::DiskLocal::prepareRead(path, settings, read_hint, pipeline);
    }

    void moveFile(const String &, const String &) override
    {
        /// Matches plain (`s3_plain`) metadata, which throws `NOT_IMPLEMENTED` for `moveFile`.
        throw std::runtime_error("moveFile is not implemented for this disk");
    }

private:
    std::string fail_path;
    bool armed = false;
    bool reads_armed = false;
};

using DiskEvents = std::shared_ptr<std::vector<std::string>>;

/// Records `sync` on the file it wraps, so a test can tell a durable write from a write that
/// only reached the page cache.
class RecordingWriteBuffer : public DB::WriteBufferFromFileDecorator
{
public:
    RecordingWriteBuffer(std::unique_ptr<DB::WriteBufferFromFileBase> impl_, std::string path_, DiskEvents events_)
        : DB::WriteBufferFromFileDecorator(std::move(impl_)), path(std::move(path_)), events(std::move(events_))
    {
    }

    void sync() override
    {
        DB::WriteBufferFromFileDecorator::sync();
        events->push_back("sync:" + path);
    }

private:
    std::string path;
    DiskEvents events;
};

/// Records the point at which the wrapped directory sync guard is destroyed, which is when the
/// directory is actually synced.
class RecordingSyncGuard : public DB::ISyncGuard
{
public:
    RecordingSyncGuard(DB::SyncGuardPtr impl_, DiskEvents events_) : impl(std::move(impl_)), events(std::move(events_)) { }

    ~RecordingSyncGuard() override
    {
        impl.reset();
        events->push_back("dirsync");
    }

private:
    DB::SyncGuardPtr impl;
    DiskEvents events;
};

/// Test disk: records the order of the durability-relevant operations, so a test can assert that
/// the backup is durable before the live state file is rewritten, and that the recovery path does
/// not drop the backup.
class RecordingStateDisk : public DB::DiskLocal
{
public:
    RecordingStateDisk(const std::string & disk_name, const std::string & disk_path, DiskEvents events_)
        : DB::DiskLocal(disk_name, disk_path), events(std::move(events_))
    {
    }

    std::unique_ptr<DB::WriteBufferFromFileBase>
    writeFile(const String & path, size_t buf_size, DB::WriteMode mode, const DB::WriteSettings & settings) override
    {
        /// Only `Rewrite` truncates, so `open:` marks the destructive open.
        events->push_back((mode == DB::WriteMode::Append ? "append:" : "open:") + path);
        auto inner = DB::DiskLocal::writeFile(path, buf_size, mode, settings);
        return std::make_unique<RecordingWriteBuffer>(std::move(inner), path, events);
    }

    void removeFile(const String & path) override
    {
        events->push_back("remove:" + path);
        DB::DiskLocal::removeFile(path);
    }

    void removeFileIfExists(const String & path) override
    {
        if (existsFile(path))
            events->push_back("remove:" + path);
        DB::DiskLocal::removeFileIfExists(path);
    }

    DB::SyncGuardPtr getDirectorySyncGuard(const String & path) const override
    {
        auto inner = DB::DiskLocal::getDirectorySyncGuard(path);
        /// The directory sync happens when the guard is destroyed, so the event is recorded then
        /// rather than here. Recording acquisition would also accept a guard that is kept alive
        /// past the operation it is supposed to make durable.
        return std::make_unique<RecordingSyncGuard>(std::move(inner), events);
    }

private:
    DiskEvents events;
};

size_t indexOf(const std::vector<std::string> & events, const std::string & event, size_t from = 0)
{
    if (from >= events.size())
        return std::string::npos;
    const auto it = std::find(events.begin() + from, events.end(), event);
    return it == events.end() ? std::string::npos : static_cast<size_t>(it - events.begin());
}

}

/// Regression test for https://github.com/ClickHouse/ClickHouse/issues/111454.
TEST_P(CoordinationTest, TestDurableStateCrashDuringSave)
{
    ChangelogDirTest logs("./logs");
    this->setLogDirectory("./logs");

    auto disk = std::make_shared<ThrowingStateDisk>("StateFile", ".", "state");
    this->keeper_context->setStateFileDisk(disk);

    std::optional<DB::KeeperStateManager> state_manager;
    const auto reload_state_manager = [&]
    {
        state_manager.emplace(1, "localhost", 9181, this->keeper_context);
        state_manager->loadLogStore(1, 0);
    };

    reload_state_manager();
    ASSERT_EQ(state_manager->read_state(), nullptr);

    auto state = nuraft::cs_new<nuraft::srv_state>();
    state->set_term(1);
    state->set_voted_for(2);
    state->allow_election_timer(true);

    auto new_state = nuraft::cs_new<nuraft::srv_state>();
    new_state->set_term(2);
    new_state->set_voted_for(3);
    new_state->allow_election_timer(true);

    {
        SCOPED_TRACE("Persist term 1 successfully");
        state_manager->save_state(*state);

        auto read_state = state_manager->read_state();
        ASSERT_NE(read_state, nullptr);
        ASSERT_EQ(read_state->get_term(), 1);
        ASSERT_EQ(read_state->get_voted_for(), 2);
    }

    {
        SCOPED_TRACE("Crash while the live state file is being rewritten");
        disk->arm();
        ASSERT_THROW(state_manager->save_state(*new_state), std::exception);
        disk->disarm();

        /// The injection must have landed on the truncating rewrite, or the test would silently
        /// retarget if the failure point ever moved.
        ASSERT_TRUE(std::filesystem::exists("./state"));
        ASSERT_EQ(std::filesystem::file_size("./state"), 0);
        ASSERT_TRUE(std::filesystem::exists("./state-OLD"));
        ASSERT_GT(std::filesystem::file_size("./state-OLD"), 0);
    }

    {
        SCOPED_TRACE("The last durable state is recovered, not lost");
        /// A nullptr here would reset the node to term 0 with an empty vote. The interrupted
        /// term-2 write never became durable, so term 1 is the expected answer.
        reload_state_manager();
        auto recovered = state_manager->read_state();
        ASSERT_NE(recovered, nullptr);
        ASSERT_EQ(recovered->get_term(), 1);
        ASSERT_EQ(recovered->get_voted_for(), 2);
    }

    {
        SCOPED_TRACE("Saving works again after recovery");
        state_manager->save_state(*new_state);
        reload_state_manager();
        auto final_state = state_manager->read_state();
        ASSERT_NE(final_state, nullptr);
        ASSERT_EQ(final_state->get_term(), 2);
        ASSERT_EQ(final_state->get_voted_for(), 3);
    }

    if (std::filesystem::exists("./state"))
        std::filesystem::remove("./state");
    if (std::filesystem::exists("./state-OLD"))
        std::filesystem::remove("./state-OLD");
}

/// The backup only protects the state if it is durable before the live state file is truncated,
/// and the recovery path must not drop it before a durable replacement exists.
/// See https://github.com/ClickHouse/ClickHouse/issues/111454.
TEST_P(CoordinationTest, TestDurableStateBackupIsSynced)
{
    ChangelogDirTest logs("./logs");
    this->setLogDirectory("./logs");

    auto events = std::make_shared<std::vector<std::string>>();
    auto disk = std::make_shared<RecordingStateDisk>("StateFile", ".", events);
    this->keeper_context->setStateFileDisk(disk);

    std::optional<DB::KeeperStateManager> state_manager;
    const auto reload_state_manager = [&]
    {
        state_manager.emplace(1, "localhost", 9181, this->keeper_context);
        state_manager->loadLogStore(1, 0);
    };

    reload_state_manager();

    auto state = nuraft::cs_new<nuraft::srv_state>();
    state->set_term(1);
    state->set_voted_for(2);
    state->allow_election_timer(true);
    state_manager->save_state(*state);

    auto new_state = nuraft::cs_new<nuraft::srv_state>();
    new_state->set_term(2);
    new_state->set_voted_for(3);
    new_state->allow_election_timer(true);

    {
        SCOPED_TRACE("Backup is durable before the live state file is truncated");
        /// Second save: "state" already exists, so it is backed up to "state-OLD" first.
        events->clear();
        state_manager->save_state(*new_state);

        const auto backup_synced = indexOf(*events, "sync:state-OLD");
        const auto live_opened = indexOf(*events, "open:state");
        const auto backup_removed = indexOf(*events, "remove:state-OLD");

        /// Without the sync the backup can be page-cache-only, so a crash in the rewrite window
        /// loses both copies.
        ASSERT_NE(backup_synced, std::string::npos);
        ASSERT_NE(live_opened, std::string::npos);
        ASSERT_LT(backup_synced, live_opened);

        /// Anchored past the backup: an earlier sync of the live file also records a dirsync.
        const auto backup_dirsync = indexOf(*events, "dirsync", backup_synced + 1);
        ASSERT_NE(backup_dirsync, std::string::npos);
        ASSERT_LT(backup_dirsync, live_opened);

        /// The backup is dropped only once the replacement is durable in full, contents and
        /// directory entry. Anchored past the truncating open, or the sync of the old contents counts.
        ASSERT_NE(backup_removed, std::string::npos);
        const auto live_synced = indexOf(*events, "sync:state", live_opened + 1);
        ASSERT_NE(live_synced, std::string::npos);
        ASSERT_LT(live_synced, backup_removed);

        const auto live_dirsync = indexOf(*events, "dirsync", live_synced + 1);
        ASSERT_NE(live_dirsync, std::string::npos);
        ASSERT_LT(live_dirsync, backup_removed);

        /// Three directory syncs in total: the live file before the backup, the new backup, and
        /// the replacement, each distinct and in that order.
        const auto pre_backup_dirsync = indexOf(*events, "dirsync");
        ASSERT_NE(pre_backup_dirsync, std::string::npos);
        ASSERT_LT(pre_backup_dirsync, backup_synced);
        ASSERT_LT(pre_backup_dirsync, backup_dirsync);
        ASSERT_LT(backup_dirsync, live_dirsync);
    }

    {
        SCOPED_TRACE("Recovery from the backup alone does not delete it");
        /// Reconstruct the on-disk state left by a crash inside the rewrite window: the backup is
        /// present and the live state file is lost. Promoting the backup with a copy that is not
        /// yet durable would reopen the same window during startup.
        ASSERT_TRUE(std::filesystem::exists("./state"));
        ASSERT_FALSE(std::filesystem::exists("./state-OLD"));
        std::filesystem::copy_file("./state", "./state-OLD");
        std::filesystem::remove("./state");

        events->clear();
        reload_state_manager();
        auto recovered = state_manager->read_state();
        ASSERT_NE(recovered, nullptr);
        ASSERT_EQ(recovered->get_term(), 2);
        ASSERT_EQ(recovered->get_voted_for(), 3);
        ASSERT_EQ(indexOf(*events, "remove:state-OLD"), std::string::npos);
        ASSERT_TRUE(std::filesystem::exists("./state-OLD"));
    }

    {
        SCOPED_TRACE("A valid live state file is synced on startup, and the backup still kept");
        /// A usable live state file is not proof that its bytes reached the disk, so startup must
        /// keep the backup rather than delete it on the strength of parsing. The previous step
        /// left only the backup, so restore the live file from it.
        std::filesystem::copy_file("./state-OLD", "./state");
        events->clear();
        reload_state_manager();
        auto both_valid = state_manager->read_state();
        ASSERT_NE(both_valid, nullptr);
        ASSERT_EQ(both_valid->get_term(), 2);
        ASSERT_EQ(indexOf(*events, "remove:state-OLD"), std::string::npos);
        ASSERT_TRUE(std::filesystem::exists("./state-OLD"));

        /// Returning a state means the node is about to act on that term, so the live file must be
        /// durable by then, or a power failure would make the node forget a term it voted in. The
        /// sync must be an append: a rewrite would truncate the file it is meant to preserve.
        ASSERT_NE(indexOf(*events, "append:state"), std::string::npos);
        ASSERT_NE(indexOf(*events, "sync:state"), std::string::npos);
        ASSERT_LT(indexOf(*events, "append:state"), indexOf(*events, "sync:state"));
        ASSERT_EQ(indexOf(*events, "open:state"), std::string::npos);

        /// Durable contents alone still allow the directory entry to be lost, which would lose the
        /// whole file.
        ASSERT_NE(indexOf(*events, "dirsync"), std::string::npos);
        ASSERT_LT(indexOf(*events, "sync:state"), indexOf(*events, "dirsync"));

        /// The sync must not have cost any content.
        reload_state_manager();
        auto after_sync = state_manager->read_state();
        ASSERT_NE(after_sync, nullptr);
        ASSERT_EQ(after_sync->get_term(), 2);
        ASSERT_EQ(after_sync->get_voted_for(), 3);
    }

    {
        SCOPED_TRACE("Refreshing an existing backup syncs the live file first");
        /// Refreshing the backup truncates it, so the live file it is replaced from must be durable
        /// first. Otherwise a retry after a failed sync destroys the one proven copy of the term
        /// while the live bytes are still only buffered. Both files are present here, which is
        /// exactly the state such a retry starts from.
        std::filesystem::copy_file("./state", "./state-OLD", std::filesystem::copy_options::overwrite_existing);
        events->clear();
        state_manager->save_state(*new_state);
        const auto live_synced_before_backup = indexOf(*events, "sync:state");
        const auto backup_reopened = indexOf(*events, "open:state-OLD");
        ASSERT_NE(live_synced_before_backup, std::string::npos);
        ASSERT_NE(backup_reopened, std::string::npos);
        ASSERT_LT(live_synced_before_backup, backup_reopened);
    }

    {
        SCOPED_TRACE("The next successful save re-establishes the live file and clears the backup");
        state_manager->save_state(*new_state);
        ASSERT_TRUE(std::filesystem::exists("./state"));
        ASSERT_FALSE(std::filesystem::exists("./state-OLD"));

        reload_state_manager();
        auto final_state = state_manager->read_state();
        ASSERT_NE(final_state, nullptr);
        ASSERT_EQ(final_state->get_term(), 2);
    }

    if (std::filesystem::exists("./state"))
        std::filesystem::remove("./state");
    if (std::filesystem::exists("./state-OLD"))
        std::filesystem::remove("./state-OLD");
}

/// An interrupted rewrite usually leaves some bytes behind rather than an empty file, so the live
/// state file fails its checksum instead of being empty. Recovery from the backup must still
/// happen, in debug builds too, where a checksum mismatch is otherwise reported as corruption.
/// See https://github.com/ClickHouse/ClickHouse/issues/111454.
TEST_P(CoordinationTest, TestDurableStatePartialWriteRecoversFromBackup)
{
    ChangelogDirTest logs("./logs");
    this->setLogDirectory("./logs");

    auto disk = std::make_shared<DB::DiskLocal>("StateFile", ".");
    this->keeper_context->setStateFileDisk(disk);

    std::optional<DB::KeeperStateManager> state_manager;
    const auto reload_state_manager = [&]
    {
        state_manager.emplace(1, "localhost", 9181, this->keeper_context);
        state_manager->loadLogStore(1, 0);
    };

    reload_state_manager();

    auto state = nuraft::cs_new<nuraft::srv_state>();
    state->set_term(1);
    state->set_voted_for(2);
    state->allow_election_timer(true);
    state_manager->save_state(*state);

    {
        SCOPED_TRACE("Live file holds a partial write: non-empty, but fails its checksum");
        /// This is what a crash inside the rewrite usually leaves behind, alongside a valid backup.
        ASSERT_TRUE(std::filesystem::exists("./state"));
        std::filesystem::copy_file("./state", "./state-OLD");
        const auto full_size = std::filesystem::file_size("./state");
        ASSERT_GT(full_size, 10u);
        std::filesystem::resize_file("./state", full_size - 1);
    }

    {
        SCOPED_TRACE("Recovery from the backup happens, in debug builds too");
        reload_state_manager();
        auto recovered = state_manager->read_state();
        ASSERT_NE(recovered, nullptr);
        ASSERT_EQ(recovered->get_term(), 1);
        ASSERT_EQ(recovered->get_voted_for(), 2);
    }

    if (std::filesystem::exists("./state"))
        std::filesystem::remove("./state");
    if (std::filesystem::exists("./state-OLD"))
        std::filesystem::remove("./state-OLD");
}

/// NuRaft keeps the server alive after `save_state` throws, so a peer retry re-enters `save_state`
/// while the live state file is still torn from the failed attempt. The retry must not copy that
/// unusable file over the backup, which at that point holds the only recoverable state.
/// See https://github.com/ClickHouse/ClickHouse/issues/111454.
TEST_P(CoordinationTest, TestDurableStateRetryKeepsValidBackup)
{
    ChangelogDirTest logs("./logs");
    this->setLogDirectory("./logs");

    auto disk = std::make_shared<ThrowingStateDisk>("StateFile", ".", "state");
    this->keeper_context->setStateFileDisk(disk);

    std::optional<DB::KeeperStateManager> state_manager;
    const auto reload_state_manager = [&]
    {
        state_manager.emplace(1, "localhost", 9181, this->keeper_context);
        state_manager->loadLogStore(1, 0);
    };

    reload_state_manager();

    /// Persist term 1 successfully; this is the state that must survive both failed attempts.
    auto state = nuraft::cs_new<nuraft::srv_state>();
    state->set_term(1);
    state->set_voted_for(2);
    state->allow_election_timer(true);
    state_manager->save_state(*state);

    auto new_state = nuraft::cs_new<nuraft::srv_state>();
    new_state->set_term(2);
    new_state->set_voted_for(3);
    new_state->allow_election_timer(true);

    {
        SCOPED_TRACE("First attempt tears the live file, leaving the backup valid");
        disk->arm();
        ASSERT_THROW(state_manager->save_state(*new_state), std::exception);
        ASSERT_EQ(std::filesystem::file_size("./state"), 0);
        ASSERT_GT(std::filesystem::file_size("./state-OLD"), 0);
    }

    {
        SCOPED_TRACE("The retry does not let the torn live file overwrite the backup");
        /// No restart in between, so the retry re-enters `save_state` with the live file worthless.
        ASSERT_THROW(state_manager->save_state(*new_state), std::exception);
        ASSERT_GT(std::filesystem::file_size("./state-OLD"), 0);
        disk->disarm();
    }

    {
        SCOPED_TRACE("Term 1 is still recoverable after both failures");
        reload_state_manager();
        auto recovered = state_manager->read_state();
        ASSERT_NE(recovered, nullptr);
        ASSERT_EQ(recovered->get_term(), 1);
        ASSERT_EQ(recovered->get_voted_for(), 2);
    }

    {
        SCOPED_TRACE("A later successful save works and clears the backup");
        state_manager->save_state(*new_state);
        reload_state_manager();
        auto final_state = state_manager->read_state();
        ASSERT_NE(final_state, nullptr);
        ASSERT_EQ(final_state->get_term(), 2);
        ASSERT_EQ(final_state->get_voted_for(), 3);
    }

    if (std::filesystem::exists("./state"))
        std::filesystem::remove("./state");
    if (std::filesystem::exists("./state-OLD"))
        std::filesystem::remove("./state-OLD");
}

/// Validating the live state file before backing it up must not turn a transient read error into
/// the verdict "this file holds nothing worth keeping": `save_state` truncates the live file right
/// after, so skipping the backup on an unread file would recreate the term/vote-loss window.
/// See https://github.com/ClickHouse/ClickHouse/issues/111454.
TEST_P(CoordinationTest, TestDurableStateReadErrorDoesNotDropBackup)
{
    ChangelogDirTest logs("./logs");
    this->setLogDirectory("./logs");

    auto disk = std::make_shared<ThrowingStateDisk>("StateFile", ".", "state");
    this->keeper_context->setStateFileDisk(disk);

    std::optional<DB::KeeperStateManager> state_manager;
    const auto reload_state_manager = [&]
    {
        state_manager.emplace(1, "localhost", 9181, this->keeper_context);
        state_manager->loadLogStore(1, 0);
    };

    reload_state_manager();

    auto state = nuraft::cs_new<nuraft::srv_state>();
    state->set_term(1);
    state->set_voted_for(2);
    state->allow_election_timer(true);
    state_manager->save_state(*state);

    auto new_state = nuraft::cs_new<nuraft::srv_state>();
    new_state->set_term(2);
    new_state->set_voted_for(3);
    new_state->allow_election_timer(true);

    {
        SCOPED_TRACE("An unreadable live state file fails the save instead of truncating it");
        /// `save_state` cannot know whether the file is worth backing up, so it must not proceed.
        disk->armReads();
        ASSERT_THROW(state_manager->save_state(*new_state), std::exception);
        disk->disarmReads();
    }

    {
        SCOPED_TRACE("A read failure on startup does not delete the state file");
        /// Returning no state makes NuRaft restart from term 0 with an empty vote.
        reload_state_manager();
        disk->armReads();
        ASSERT_THROW(state_manager->read_state(), std::exception);
        disk->disarmReads();
        ASSERT_TRUE(std::filesystem::exists("./state"));
    }

    {
        SCOPED_TRACE("Term 1 is intact: neither failure touched the live file");
        reload_state_manager();
        auto recovered = state_manager->read_state();
        ASSERT_NE(recovered, nullptr);
        ASSERT_EQ(recovered->get_term(), 1);
        ASSERT_EQ(recovered->get_voted_for(), 2);
    }

    {
        SCOPED_TRACE("Once reads work again, saving proceeds normally");
        state_manager->save_state(*new_state);
        reload_state_manager();
        auto final_state = state_manager->read_state();
        ASSERT_NE(final_state, nullptr);
        ASSERT_EQ(final_state->get_term(), 2);
        ASSERT_EQ(final_state->get_voted_for(), 3);
    }

    if (std::filesystem::exists("./state"))
        std::filesystem::remove("./state");
    if (std::filesystem::exists("./state-OLD"))
        std::filesystem::remove("./state-OLD");
}

TEST_P(CoordinationTest, TestFeatureFlags)
{
    using namespace Coordination;

    const auto storage_ptr = DB::KeeperStorage::create(500, "", this->keeper_context);
    DB::KeeperStorage & storage = *storage_ptr;
    auto request = std::make_shared<ZooKeeperGetRequest>();
    request->path = DB::keeper_api_feature_flags_path;
    KeeperRequestsForSessions requests {KeeperRequestForSession {.session_id = 0, .request = request}};
    auto responses = storage.processLocalRequests(requests, true);
    const auto & get_response = getSingleResponse<ZooKeeperGetResponse>(responses);
    DB::KeeperFeatureFlags feature_flags;
    feature_flags.setFeatureFlags(get_response.data);
    ASSERT_TRUE(feature_flags.isEnabled(KeeperFeatureFlag::FILTERED_LIST));
    ASSERT_TRUE(feature_flags.isEnabled(KeeperFeatureFlag::MULTI_READ));
    ASSERT_TRUE(feature_flags.isEnabled(KeeperFeatureFlag::CHECK_NOT_EXISTS));
    ASSERT_TRUE(feature_flags.isEnabled(KeeperFeatureFlag::CREATE_IF_NOT_EXISTS));
    ASSERT_TRUE(feature_flags.isEnabled(KeeperFeatureFlag::REMOVE_RECURSIVE));
    ASSERT_TRUE(feature_flags.isEnabled(KeeperFeatureFlag::MULTI_WATCHES));
    ASSERT_TRUE(feature_flags.isEnabled(KeeperFeatureFlag::CHECK_STAT));
    ASSERT_TRUE(feature_flags.isEnabled(KeeperFeatureFlag::TRY_REMOVE));
    ASSERT_TRUE(feature_flags.isEnabled(KeeperFeatureFlag::LIST_WITH_STAT_AND_DATA));
}

TEST(CoordinationRequestSize, WriteRejectsRequestOverInt32)
{
    // The guard fires on the computed size before serialization, so a fake sizeImpl needs no real data.
    struct HugeRequest final : Coordination::ZooKeeperRequest
    {
        String getPath() const override { return {}; }
        Coordination::OpNum getOpNum() const override { return Coordination::OpNum::Create; }
        void writeImpl(DB::WriteBuffer &) const override {}
        size_t sizeImpl() const override { return std::numeric_limits<int32_t>::max(); }
        void readImpl(DB::ReadBuffer &) override {}
        Coordination::ZooKeeperResponsePtr makeResponse() const override { return nullptr; }
        bool isReadRequest() const override { return false; }
    };

    HugeRequest request;
    DB::WriteBufferFromNuraftBuffer wbuf;
    EXPECT_THROW(request.write(wbuf, false, false), Coordination::Exception);
}

#endif
