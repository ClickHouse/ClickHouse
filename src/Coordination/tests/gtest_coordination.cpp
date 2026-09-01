#include "config.h"

#if USE_NURAFT

#include <Coordination/tests/gtest_coordination_common.h>

#include <Coordination/InMemoryLogStore.h>
#include <Coordination/SummingStateMachine.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/KeeperCommon.h>
#include <Coordination/KeeperDispatcher.h>
#include <Coordination/KeeperRequestDispatcher.h>
#include <Coordination/KeeperRequestDispatcherOld.h>
#include <Coordination/KeeperServer.h>
#include <Coordination/KeeperConstants.h>
#include <Coordination/KeeperSnapshotManager.h>
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

#include <Common/ZooKeeper/KeeperException.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Common/ZooKeeper/ZooKeeperIO.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/scope_guard_safe.h>
#include <Common/Stopwatch.h>

#include <Poco/Util/XMLConfiguration.h>

#include <future>
#include <limits>
#include <sstream>
#include <thread>
#include <vector>

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsUInt64 write_snapshot_version;
}

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

TEST(CoordinationSettingsValidation, WriteSnapshotVersionHotReload)
{
    auto ctx = std::make_shared<DB::KeeperContext>(true, std::make_shared<DB::CoordinationSettings>());
    EXPECT_EQ(ctx->getWriteSnapshotVersion(), DB::SnapshotVersion::V8);

    /// write_snapshot_version is hot-reloadable: a valid update takes effect.
    auto updated = std::make_shared<DB::CoordinationSettings>();
    (*updated)[DB::CoordinationSetting::write_snapshot_version] = 9;
    ctx->updateSettings(updated);
    EXPECT_EQ(ctx->getWriteSnapshotVersion(), DB::SnapshotVersion::V9);

    /// An out-of-range update is rejected and the previous value stays in effect.
    auto too_old = std::make_shared<DB::CoordinationSettings>();
    (*too_old)[DB::CoordinationSetting::write_snapshot_version] = 3;
    EXPECT_THROW(ctx->updateSettings(too_old), DB::Exception);
    EXPECT_EQ(ctx->getWriteSnapshotVersion(), DB::SnapshotVersion::V9);

    auto too_new = std::make_shared<DB::CoordinationSettings>();
    (*too_new)[DB::CoordinationSetting::write_snapshot_version] = DB::MAX_SUPPORTED_SNAPSHOT_VERSION + 1;
    EXPECT_THROW(ctx->updateSettings(too_new), DB::Exception);
    EXPECT_EQ(ctx->getWriteSnapshotVersion(), DB::SnapshotVersion::V9);
}

TEST(CoordinationSettingsParse, NuraftSnapshotSyncCtxTimeout)
{
    auto load = [](const std::string & xml)
    {
        std::istringstream stream(xml); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(stream);
        DB::CoordinationSettings settings;
        settings.loadFromConfig("keeper_server.coordination_settings", *config);
        return settings[DB::CoordinationSetting::nuraft_snapshot_sync_ctx_timeout_ms].totalMilliseconds();
    };

    /// The default must stay 0, which is what `raft_server::get_snapshot_sync_ctx_timeout` treats as
    /// "derive from raft_limits_response_limit * heart_beat_interval_ms". Anything else would change
    /// the snapshot-install budget of every existing installation on upgrade.
    EXPECT_EQ(load("<clickhouse><keeper_server><coordination_settings>"
                   "</coordination_settings></keeper_server></clickhouse>"),
              0);

    /// A bare number in the config is milliseconds, which is the unit
    /// `raft_params::snapshot_sync_ctx_timeout_` expects. Had the setting been declared with a
    /// coarser unit, the same config would mean a budget 1000 times larger.
    EXPECT_EQ(load("<clickhouse><keeper_server><coordination_settings>"
                   "<nuraft_snapshot_sync_ctx_timeout_ms>60000</nuraft_snapshot_sync_ctx_timeout_ms>"
                   "</coordination_settings></keeper_server></clickhouse>"),
              60000);

    /// The setting itself is unbounded, so an operator can configure more milliseconds than the
    /// int32 `raft_params::snapshot_sync_ctx_timeout_` can hold. Such a value survives parsing and
    /// must be narrowed by `buildRaftParams` rather than wrapping - covered by the tests below.
    EXPECT_GT(load("<clickhouse><keeper_server><coordination_settings>"
                   "<nuraft_snapshot_sync_ctx_timeout_ms>3000000000</nuraft_snapshot_sync_ctx_timeout_ms>"
                   "</coordination_settings></keeper_server></clickhouse>"),
              std::numeric_limits<int32_t>::max());
}

/// The composition that actually reaches NuRaft: config text -> setting -> `raft_params` field.
/// Parsing and narrowing are pinned separately below, but only this test would notice the timeout
/// being handed over in the wrong unit or bypassing the narrowing.
TEST(CoordinationSettingsParse, BuildRaftParams)
{
    auto build = [](const std::string & xml)
    {
        std::istringstream stream(xml); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::AutoPtr<Poco::Util::XMLConfiguration> config = new Poco::Util::XMLConfiguration(stream);
        DB::CoordinationSettings settings;
        settings.loadFromConfig("keeper_server.coordination_settings", *config);
        return DB::buildRaftParams(settings, getLogger("CoordinationSettingsParse"));
    };

    /// 0 is what makes `raft_server::get_snapshot_sync_ctx_timeout` fall back to
    /// `raft_limits_response_limit * heart_beat_interval_`, i.e. today's behaviour.
    EXPECT_EQ(build("<clickhouse><keeper_server><coordination_settings>"
                    "</coordination_settings></keeper_server></clickhouse>")
                  .snapshot_sync_ctx_timeout_,
              0);

    /// Milliseconds all the way through: 60000 in the config must be 60000 in `raft_params`, not
    /// 60 and not 60000000.
    EXPECT_EQ(build("<clickhouse><keeper_server><coordination_settings>"
                    "<nuraft_snapshot_sync_ctx_timeout_ms>60000</nuraft_snapshot_sync_ctx_timeout_ms>"
                    "</coordination_settings></keeper_server></clickhouse>")
                  .snapshot_sync_ctx_timeout_,
              60000);

    /// The field is an int32, so an operator value beyond its range must be narrowed here rather
    /// than wrapping to a negative timeout.
    EXPECT_EQ(build("<clickhouse><keeper_server><coordination_settings>"
                    "<nuraft_snapshot_sync_ctx_timeout_ms>3000000000</nuraft_snapshot_sync_ctx_timeout_ms>"
                    "</coordination_settings></keeper_server></clickhouse>")
                  .snapshot_sync_ctx_timeout_,
              std::numeric_limits<int32_t>::max());

    /// Neighbouring millisecond settings go through the same conversion, so pin one of them too.
    EXPECT_EQ(build("<clickhouse><keeper_server><coordination_settings>"
                    "<heart_beat_interval_ms>250</heart_beat_interval_ms>"
                    "</coordination_settings></keeper_server></clickhouse>")
                  .heart_beat_interval_,
              250);
}

/// Every `nuraft::raft_params` field Keeper configures from an unbounded setting is narrowed by this
/// function, so a value that does not fit must be reported and capped instead of wrapping to a
/// negative timeout or gap.
TEST(CoordinationSettingsParse, ValueOrMaxInt32)
{
    auto log = getLogger("CoordinationSettingsParse");

    EXPECT_EQ(DB::getValueOrMaxInt32AndLogWarning(0, "test", log), 0);
    EXPECT_EQ(DB::getValueOrMaxInt32AndLogWarning(60000, "test", log), 60000);
    EXPECT_EQ(
        DB::getValueOrMaxInt32AndLogWarning(std::numeric_limits<int32_t>::max(), "test", log),
        std::numeric_limits<int32_t>::max());

    /// Above the range it caps rather than wrapping negative.
    EXPECT_EQ(
        DB::getValueOrMaxInt32AndLogWarning(
            static_cast<uint64_t>(std::numeric_limits<int32_t>::max()) + 1, "test", log),
        std::numeric_limits<int32_t>::max());
    EXPECT_EQ(DB::getValueOrMaxInt32AndLogWarning(3000000000, "test", log), std::numeric_limits<int32_t>::max());
    EXPECT_EQ(
        DB::getValueOrMaxInt32AndLogWarning(std::numeric_limits<uint64_t>::max(), "test", log),
        std::numeric_limits<int32_t>::max());
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
        DB::ReadAheadSettings{},
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
        DB::ReadAheadSettings{},
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

/// Test disk: throws when the given file is (re)opened for writing, and rejects moveFile
/// (as plain/s3_plain metadata does).
class ThrowingStateDisk : public DB::DiskLocal
{
public:
    ThrowingStateDisk(const std::string & disk_name, const std::string & disk_path, std::string fail_path_)
        : DB::DiskLocal(disk_name, disk_path), fail_path(std::move(fail_path_))
    {
    }

    void arm() { armed = true; }
    void disarm() { armed = false; }

    std::unique_ptr<DB::WriteBufferFromFileBase>
    writeFile(const String & path, size_t buf_size, DB::WriteMode mode, const DB::WriteSettings & settings) override
    {
        auto inner = DB::DiskLocal::writeFile(path, buf_size, mode, settings);
        if (armed && path == fail_path)
            throw std::runtime_error("Injected state write failure");
        return inner;
    }

    void moveFile(const String &, const String &) override
    {
        /// Matches plain (s3_plain) metadata, which throws NOT_IMPLEMENTED for moveFile.
        throw std::runtime_error("moveFile is not implemented for this disk");
    }

private:
    std::string fail_path;
    bool armed = false;
};

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

    /// Persist an initial state (term 1) successfully.
    auto state = nuraft::cs_new<nuraft::srv_state>();
    state->set_term(1);
    state->set_voted_for(2);
    state->allow_election_timer(true);
    state_manager->save_state(*state);

    {
        auto read_state = state_manager->read_state();
        ASSERT_NE(read_state, nullptr);
        ASSERT_EQ(read_state->get_term(), 1);
        ASSERT_EQ(read_state->get_voted_for(), 2);
    }

    /// Now attempt to persist term 2 but crash (throw) while the live "state" file is being
    /// rewritten. The live file is left truncated/torn, exactly the vulnerable window.
    auto new_state = nuraft::cs_new<nuraft::srv_state>();
    new_state->set_term(2);
    new_state->set_voted_for(3);
    new_state->allow_election_timer(true);

    disk->arm();
    ASSERT_THROW(state_manager->save_state(*new_state), std::exception);
    disk->disarm();

    /// After the "crash" the previously committed state must still be recoverable: read_state
    /// must not return nullptr (which would reset the node to term 0 and lose the vote).
    reload_state_manager();
    auto recovered = state_manager->read_state();
    ASSERT_NE(recovered, nullptr);
    /// The recovered state is the last durably persisted one (term 1); the interrupted term-2
    /// write never became durable. The invariant that matters is that state is not lost.
    ASSERT_EQ(recovered->get_term(), 1);
    ASSERT_EQ(recovered->get_voted_for(), 2);

    /// A subsequent successful save must work normally after recovery.
    state_manager->save_state(*new_state);
    reload_state_manager();
    auto final_state = state_manager->read_state();
    ASSERT_NE(final_state, nullptr);
    ASSERT_EQ(final_state->get_term(), 2);
    ASSERT_EQ(final_state->get_voted_for(), 3);

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

/// checkIfRequestIncreaseMem is the memory-soft-limit admission classifier. It is a pure function of
/// the request, so it is tested here rather than through the integration test: reproducing sustained
/// memory pressure is RSS-driven and decays as soon as the load stops, which makes any assertion that
/// depends on Keeper still refusing inherently racy.
namespace
{

Coordination::ZooKeeperRequestPtr makeSetRequest(const std::string & path, const std::string & data)
{
    auto request = std::make_shared<Coordination::ZooKeeperSetRequest>();
    request->path = path;
    request->data = data;
    request->version = -1;
    return request;
}

Coordination::ZooKeeperRequestPtr makeCreateRequest(const std::string & path, const std::string & data)
{
    auto request = std::make_shared<Coordination::ZooKeeperCreateRequest>();
    request->path = path;
    request->data = data;
    return request;
}

Coordination::ZooKeeperRequestPtr makeRemoveRequest(const std::string & path)
{
    auto request = std::make_shared<Coordination::ZooKeeperRemoveRequest>();
    request->path = path;
    request->version = -1;
    return request;
}

Coordination::ZooKeeperRequestPtr makeMultiRequest(const Coordination::Requests & subrequests)
{
    return std::make_shared<Coordination::ZooKeeperMultiRequest>(subrequests, Coordination::ACLs{});
}

}

TEST(KeeperMemorySoftLimitAdmission, EmptySetIsNotMemoryIncreasing)
{
    /// The session-registration write from ZooKeeper::initSession. Refusing it is what locked tables
    /// into readonly for the duration of a Keeper memory event: a Set cannot allocate a znode, and with
    /// empty data the amount of stored data can only shrink.
    ASSERT_FALSE(DB::checkIfRequestIncreaseMem(makeSetRequest("/clickhouse/sessions/zookeeper/uuid", "")));

    /// A Set that actually carries data can grow the store, so it must still be refused - note this is
    /// true even though the path is long, i.e. the decision is on the data and not on the request size.
    ASSERT_TRUE(DB::checkIfRequestIncreaseMem(makeSetRequest("/clickhouse/sessions/zookeeper/uuid", "x")));
}

TEST(KeeperMemorySoftLimitAdmission, CreateIsAlwaysMemoryIncreasing)
{
    /// Unchanged behaviour, asserted so that narrowing the Set branch cannot silently widen this one.
    /// An empty Create still allocates a znode, so unlike Set it is classified increasing.
    ASSERT_TRUE(DB::checkIfRequestIncreaseMem(makeCreateRequest("/a", "")));
    ASSERT_TRUE(DB::checkIfRequestIncreaseMem(makeCreateRequest("/a", "data")));
}

TEST(KeeperMemorySoftLimitAdmission, MultiClassifiedBySumOfDataSizes)
{
    /// A Multi of only empty Sets has a zero delta and must be admitted. Before the fix this returned
    /// true, because the branch summed bytesSize() - which includes the path, the version and the xid -
    /// so an empty Set contributed growth proportional to its path length. `Set(<table>/replicas, "")`
    /// in SharedMergeTree's activateReplica is exactly this shape.
    ASSERT_FALSE(DB::checkIfRequestIncreaseMem(makeMultiRequest({
        makeSetRequest("/some/quite/long/path/that/would/have/dominated/bytesSize", ""),
        makeSetRequest("/another/long/path/replicas", ""),
    })));

    /// Data in any subrequest still makes the Multi increasing.
    ASSERT_TRUE(DB::checkIfRequestIncreaseMem(makeMultiRequest({
        makeSetRequest("/a", ""),
        makeSetRequest("/b", "data"),
    })));

    /// So does a Create, which is the gate-2 shape from activateReplica: an ephemeral is_active node
    /// plus Sets. This one genuinely allocates and is expected to stay refused.
    ASSERT_TRUE(DB::checkIfRequestIncreaseMem(makeMultiRequest({
        makeCreateRequest("/table/replicas/r1/is_active", ""),
        makeSetRequest("/table/replicas/r1/host", "hostname"),
        makeSetRequest("/table/replicas", ""),
    })));
}

TEST(KeeperMemorySoftLimitAdmission, ReadsAndRemovesAreNotMemoryIncreasing)
{
    /// Reads fall through to the final `return false`, which is why a saturated Keeper still serves
    /// them - the property the end-to-end test relies on.
    ASSERT_FALSE(DB::checkIfRequestIncreaseMem(std::make_shared<Coordination::ZooKeeperGetRequest>()));
    ASSERT_FALSE(DB::checkIfRequestIncreaseMem(std::make_shared<Coordination::ZooKeeperListRequest>()));

    /// A standalone Remove is not classified increasing. Deliberately unchanged by this fix.
    ASSERT_FALSE(DB::checkIfRequestIncreaseMem(makeRemoveRequest("/a")));
}

namespace DB
{

/// The in-flight batch queue and the SessionID error path are private, so the tests below reach them
/// through these friend accessors.
class KeeperRequestDispatcherTestAccessor
{
public:
    /// Puts one request in a fresh in-flight batch, the way dispatchThread would, and returns its index.
    static size_t seedInFlightBatch(KeeperRequestDispatcher & dispatcher, const KeeperRequestForSession & request)
    {
        size_t batch_idx = dispatcher.tail_idx.load();
        auto & batch = dispatcher.in_flight_batches[batch_idx % dispatcher.in_flight_batches.size()];
        batch.requests = {request};
        batch.activate({});
        dispatcher.tail_idx.store(batch_idx + 1);
        return batch_idx;
    }

    static size_t committedRequests(KeeperRequestDispatcher & dispatcher, size_t batch_idx)
    {
        return dispatcher.in_flight_batches[batch_idx % dispatcher.in_flight_batches.size()].committed_requests;
    }

    static size_t headIdx(const KeeperRequestDispatcher & dispatcher) { return dispatcher.head_idx.load(); }

    static void dropInFlightRequests(KeeperRequestDispatcher & dispatcher) { dispatcher.dropInFlightRequests(); }
};

class KeeperRequestDispatcherOldTestAccessor
{
public:
    static void addErrorResponses(
        KeeperRequestDispatcherOld & dispatcher,
        const KeeperRequestsForSessions & requests_for_sessions,
        Coordination::Error error,
        bool may_have_dependent_reads)
    {
        dispatcher.addErrorResponses(requests_for_sessions, error, may_have_dependent_reads);
    }
};

class KeeperDispatcherTestAccessor
{
public:
    static void setKeeperContext(KeeperDispatcher & dispatcher, KeeperContextPtr keeper_context)
    {
        dispatcher.keeper_context = std::move(keeper_context);
    }

    static void setServer(KeeperDispatcher & dispatcher, std::unique_ptr<KeeperServer> server)
    {
        dispatcher.server = std::move(server);
    }

    static KeeperServer * server(KeeperDispatcher & dispatcher) { return dispatcher.server.get(); }

    static KeeperSpecialResponseRouter router(KeeperDispatcher & dispatcher)
    {
        return [&dispatcher](const KeeperResponseForSession & response)
        { return dispatcher.tryRouteSpecialResponse(response); };
    }

    /// Registers a waiter the way getSessionID does, and returns the future a client blocks on.
    /// Empty if the internal id was already registered.
    static std::optional<std::future<int64_t>> registerSessionIDWaiter(KeeperDispatcher & dispatcher, int64_t internal_id)
    {
        std::lock_guard lock(dispatcher.new_session_id_mutex);
        auto [it, inserted] = dispatcher.new_session_id_requests.try_emplace(internal_id);
        if (!inserted)
            return {};
        return it->second.get_future();
    }

    static size_t sessionIDWaiterCount(KeeperDispatcher & dispatcher, int64_t internal_id)
    {
        std::lock_guard lock(dispatcher.new_session_id_mutex);
        return dispatcher.new_session_id_requests.count(internal_id);
    }

    static void interruptibleSleep(KeeperDispatcher & dispatcher, std::chrono::milliseconds period)
    {
        dispatcher.interruptibleSleep(period);
    }
};

}

namespace
{

/// A server without a started Raft instance. Enough for onCommit and the error paths, which only
/// touch in_flight_batches and the response routing.
struct DispatcherFixture
{
    ChangelogDirTest dir{"./session_id_routing_logs"};
    DB::KeeperContextPtr keeper_context;
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config;
    DB::SnapshotsQueue snapshots_queue{1};
    DB::KeeperSnapshotManagerS3 snapshot_s3;
    std::unique_ptr<DB::KeeperServer> server;
    std::unique_ptr<DB::KeeperRequestDispatcher> dispatcher;

    /// Responses the router took, i.e. that did not go to the per-session response queue.
    std::vector<DB::KeeperResponseForSession> routed;

    DB::KeeperSpecialResponseRouter router()
    {
        return [this](const DB::KeeperResponseForSession & response)
        {
            if (response.response->getOpNum() != Coordination::OpNum::SessionID)
                return false;
            routed.push_back(response);
            return true;
        };
    }

    DispatcherFixture()
    {
        std::string xml = R"(<clickhouse><keeper_server>
            <server_id>1</server_id>
            <tcp_port>0</tcp_port>
            <raft_configuration><server>
                <id>1</id><hostname>localhost</hostname><port>44444</port>
            </server></raft_configuration>
        </keeper_server></clickhouse>)";
        std::stringstream stream(xml); // NOLINT(readability-isolate-declaration)
        config = new Poco::Util::XMLConfiguration(stream);

        keeper_context = ::makeKeeperContext(false, nullptr);
        keeper_context->setLogDisk(std::make_shared<DB::DiskLocal>("LogDisk", dir.path));
        keeper_context->setSnapshotDisk(std::make_shared<DB::DiskLocal>("SnapshotDisk", dir.path));
        keeper_context->setStateFileDisk(std::make_shared<DB::DiskLocal>("StateFile", dir.path));
        keeper_context->setLocalLogsPreprocessed();

        server = std::make_unique<DB::KeeperServer>(
            DB::KeeperConfiguration::loadFromConfig(*config, true),
            *config,
            [](DB::KeeperResponseForSession) {},
            snapshots_queue,
            keeper_context,
            snapshot_s3,
            [](uint64_t, const DB::KeeperRequestForSession &) {});

        dispatcher = std::make_unique<DB::KeeperRequestDispatcher>(server.get(), router());
    }
};

DB::KeeperRequestForSession makeSessionIDRequest(int32_t server_id, int64_t internal_id)
{
    auto request = std::make_shared<Coordination::ZooKeeperSessionIDRequest>();
    request->server_id = server_id;
    request->internal_id = internal_id;
    request->session_timeout_ms = 10000;
    /// KeeperDispatcher::getSessionID leaves xid at its default and uses session id -1, so every
    /// SessionID request in the cluster carries the same (session_id, xid).
    DB::KeeperRequestForSession request_for_session;
    request_for_session.request = request;
    request_for_session.session_id = DB::keeper_internal_get_session_id;
    return request_for_session;
}

using RequestDispatcherAccessor = DB::KeeperRequestDispatcherTestAccessor;
using RequestDispatcherOldAccessor = DB::KeeperRequestDispatcherOldTestAccessor;
using DispatcherAccessor = DB::KeeperDispatcherTestAccessor;

}

/// A SessionID commit from another server must not retire our in-flight SessionID request, and our
/// own must still retire it.
TEST(KeeperDispatcher, SessionIDCommitCorrelation)
{
    DispatcherFixture fixture;
    auto & dispatcher = *fixture.dispatcher;

    size_t batch_idx = RequestDispatcherAccessor::seedInFlightBatch(
        dispatcher, makeSessionIDRequest(/*server_id=*/ 1, /*internal_id=*/ 7));

    ASSERT_EQ(RequestDispatcherAccessor::committedRequests(dispatcher, batch_idx), 0u);

    /// Same degenerate (session_id, xid), different origin.
    dispatcher.onCommit(makeSessionIDRequest(/*server_id=*/ 2, /*internal_id=*/ 7));
    EXPECT_EQ(RequestDispatcherAccessor::committedRequests(dispatcher, batch_idx), 0u)
        << "a foreign server's SessionID commit retired our request";

    /// Same server, different client.
    dispatcher.onCommit(makeSessionIDRequest(/*server_id=*/ 1, /*internal_id=*/ 8));
    EXPECT_EQ(RequestDispatcherAccessor::committedRequests(dispatcher, batch_idx), 0u)
        << "another client's SessionID commit retired our request";

    /// Ours: correlation must still work, otherwise every session request would stall.
    dispatcher.onCommit(makeSessionIDRequest(/*server_id=*/ 1, /*internal_id=*/ 7));
    EXPECT_EQ(RequestDispatcherAccessor::committedRequests(dispatcher, batch_idx), 1u)
        << "our own SessionID commit did not retire our request";
    EXPECT_EQ(RequestDispatcherAccessor::headIdx(dispatcher), batch_idx + 1) << "the fully committed batch was not popped";
}

/// A dropped SessionID request must reach its waiter instead of the per-session response queue,
/// where session id -1 has no callback and the response is discarded.
TEST(KeeperDispatcher, SessionIDErrorReachesWaiter)
{
    DispatcherFixture fixture;
    auto & dispatcher = *fixture.dispatcher;

    size_t batch_idx = RequestDispatcherAccessor::seedInFlightBatch(
        dispatcher, makeSessionIDRequest(/*server_id=*/ 1, /*internal_id=*/ 11));

    RequestDispatcherAccessor::dropInFlightRequests(dispatcher);

    ASSERT_EQ(fixture.routed.size(), 1u) << "the dropped SessionID error did not reach its waiter";
    const auto & response = fixture.routed.front();
    EXPECT_EQ(response.response->error, Coordination::Error::ZCONNECTIONLOSS);

    /// The identifiers the waiter is keyed by must survive makeResponse().
    const auto & session_id_response = dynamic_cast<const Coordination::ZooKeeperSessionIDResponse &>(*response.response);
    EXPECT_EQ(session_id_response.server_id, 1);
    EXPECT_EQ(session_id_response.internal_id, 11);

    EXPECT_EQ(RequestDispatcherAccessor::headIdx(dispatcher), batch_idx + 1) << "the dropped batch was not popped";
}

/// use_new_dispatcher is a setting, so the old dispatcher is a live carrier of the same defect. It
/// has no in-flight batch tracking, so it synthesizes the error straight from addErrorResponses.
TEST(KeeperDispatcherOld, SessionIDErrorReachesWaiter)
{
    DispatcherFixture fixture;
    DB::KeeperRequestDispatcherOld dispatcher_old(fixture.server.get(), fixture.router());
    /// Its threads loop until the context says shutdown, the way KeeperDispatcher::shutdown does it.
    SCOPE_EXIT({
        fixture.keeper_context->setShutdownCalled();
        dispatcher_old.shutdown();
    });

    RequestDispatcherOldAccessor::addErrorResponses(
        dispatcher_old,
        {makeSessionIDRequest(/*server_id=*/ 1, /*internal_id=*/ 13)},
        Coordination::Error::ZCONNECTIONLOSS,
        /*may_have_dependent_reads=*/ false);

    ASSERT_EQ(fixture.routed.size(), 1u) << "the dropped SessionID error did not reach its waiter";
    const auto & response = fixture.routed.front();
    EXPECT_EQ(response.response->error, Coordination::Error::ZCONNECTIONLOSS);
    const auto & session_id_response = dynamic_cast<const Coordination::ZooKeeperSessionIDResponse &>(*response.response);
    EXPECT_EQ(session_id_response.server_id, 1);
    EXPECT_EQ(session_id_response.internal_id, 13);
}

/// Unlike the arms above, which stop at the router seam, this one drives the production router and
/// asserts on the getSessionID waiter a client actually blocks on.
TEST(KeeperDispatcher, SessionIDErrorReachesRealWaiter)
{
    DispatcherFixture fixture;

    /// onSessionIDResponse reads only server->getServerID(), set by the KeeperServer constructor, so
    /// an un-started server suffices here.
    DB::KeeperDispatcher keeper_dispatcher;
    /// Holds a raw pointer to the server keeper_dispatcher takes over, and would outlive it.
    fixture.dispatcher.reset();
    DispatcherAccessor::setServer(keeper_dispatcher, std::move(fixture.server));

    DB::KeeperRequestDispatcher dispatcher(
        DispatcherAccessor::server(keeper_dispatcher), DispatcherAccessor::router(keeper_dispatcher));

    constexpr int64_t internal_id = 17;
    auto waiter = DispatcherAccessor::registerSessionIDWaiter(keeper_dispatcher, internal_id);
    ASSERT_TRUE(waiter.has_value());
    auto & future = *waiter;

    /// A response for a different client must not wake our waiter.
    RequestDispatcherAccessor::seedInFlightBatch(dispatcher, makeSessionIDRequest(/*server_id=*/ 1, /*internal_id=*/ 18));
    RequestDispatcherAccessor::dropInFlightRequests(dispatcher);
    ASSERT_EQ(future.wait_for(std::chrono::seconds(0)), std::future_status::timeout)
        << "another client's SessionID error woke our waiter";
    EXPECT_EQ(DispatcherAccessor::sessionIDWaiterCount(keeper_dispatcher, internal_id), 1u);

    RequestDispatcherAccessor::seedInFlightBatch(dispatcher, makeSessionIDRequest(/*server_id=*/ 1, internal_id));
    RequestDispatcherAccessor::dropInFlightRequests(dispatcher);

    /// Ready without waiting: the client does not sit out the session timeout.
    ASSERT_EQ(future.wait_for(std::chrono::seconds(0)), std::future_status::ready)
        << "the dropped SessionID error did not reach the getSessionID waiter";

    try
    {
        FAIL() << "getSessionID returned session id " << future.get() << " instead of the error";
    }
    catch (const Coordination::Exception & e)
    {
        EXPECT_EQ(e.code, Coordination::Error::ZCONNECTIONLOSS);
    }

    EXPECT_EQ(DispatcherAccessor::sessionIDWaiterCount(keeper_dispatcher, internal_id), 0u) << "the waiter entry leaked";
}

/// A request accepted before the shutdown flag was set is discarded without a response: the drains
/// do not synthesize one, and the old dispatcher's requestThread abandons what it already popped.
/// Routing cannot cover that, so the waiter is completed once no dispatcher can produce a response.
TEST(KeeperDispatcher, PendingSessionIDRequestsFailOnShutdown)
{
    DispatcherFixture fixture;
    fixture.dispatcher.reset();

    /// Drives the real KeeperDispatcher::shutdown, so removing its call reddens this arm. Neither
    /// dispatcher is constructed and `server` is left null: shutdown skips both, and the waiter
    /// must still be completed.
    DB::KeeperDispatcher keeper_dispatcher;
    DispatcherAccessor::setKeeperContext(keeper_dispatcher, fixture.keeper_context);

    constexpr int64_t internal_id = 31;
    auto waiter = DispatcherAccessor::registerSessionIDWaiter(keeper_dispatcher, internal_id);
    ASSERT_TRUE(waiter.has_value());
    auto & future = *waiter;

    ASSERT_EQ(future.wait_for(std::chrono::seconds(0)), std::future_status::timeout)
        << "the waiter completed before shutdown";

    keeper_dispatcher.shutdown(/*closed_all_connections=*/ true);

    ASSERT_EQ(future.wait_for(std::chrono::seconds(0)), std::future_status::ready)
        << "the waiter was left to time out across shutdown";
    try
    {
        FAIL() << "getSessionID returned session id " << future.get() << " instead of the error";
    }
    catch (const Coordination::Exception & e)
    {
        EXPECT_EQ(e.code, Coordination::Error::ZSESSIONEXPIRED);
    }

    EXPECT_EQ(DispatcherAccessor::sessionIDWaiterCount(keeper_dispatcher, internal_id), 0u) << "the waiter entry leaked";
}

/// setShutdownCalled is one-shot, so a shutdown step that throws before the normal cleanup point
/// is the waiters' only chance to be completed.
TEST(KeeperDispatcher, PendingSessionIDRequestsFailOnThrowingShutdown)
{
    DispatcherFixture fixture;
    fixture.dispatcher.reset();

    DB::KeeperDispatcher keeper_dispatcher;
    DispatcherAccessor::setKeeperContext(keeper_dispatcher, fixture.keeper_context);

    constexpr int64_t internal_id = 37;
    auto waiter = DispatcherAccessor::registerSessionIDWaiter(keeper_dispatcher, internal_id);
    ASSERT_TRUE(waiter.has_value());
    auto & future = *waiter;

    DB::FailPointInjection::enableFailPoint("keeper_shutdown_throw_after_flag");
    SCOPE_EXIT({ DB::FailPointInjection::disableFailPoint("keeper_shutdown_throw_after_flag"); });

    keeper_dispatcher.shutdown(/*closed_all_connections=*/ true);

    ASSERT_EQ(future.wait_for(std::chrono::seconds(0)), std::future_status::ready)
        << "a shutdown that threw left the waiter to time out";
    try
    {
        FAIL() << "getSessionID returned session id " << future.get() << " instead of the error";
    }
    catch (const Coordination::Exception & e)
    {
        EXPECT_EQ(e.code, Coordination::Error::ZSESSIONEXPIRED);
    }

    EXPECT_EQ(DispatcherAccessor::sessionIDWaiterCount(keeper_dispatcher, internal_id), 0u) << "the waiter entry leaked";
}

namespace
{

/// Millisecond counts a coordination wait must survive. The first is representable as nanoseconds
/// but leaves less than a millisecond below Int64::max, so `steady_clock::now() + duration` wraps;
/// the rest overflow the milliseconds to nanoseconds product itself.
const std::vector<Int64> huge_timeouts_ms = {
    9'223'372'036'854LL,
    9'223'372'036'855LL,
    9'223'372'036'854'775LL,
    std::numeric_limits<Int64>::max(),
};

/// The predicate becomes true after this long, so a wait that kept its duration returns because the
/// predicate fired, while a wait whose duration was lost returns immediately instead.
constexpr Int64 notify_after_ms = 300;

}

/// A very long timeout must remain a very long timeout: with the raw conversion the deadline wraps
/// into the past, so the wait gives up at once and reports that the log was not committed.
TEST(KeeperContext, WaitCommittedUptoKeepsHugeTimeout)
{
    /// The parameter is unsigned, so a negative count arrives here as a huge positive one.
    std::vector<UInt64> timeouts;
    for (Int64 ms : huge_timeouts_ms)
        timeouts.push_back(static_cast<UInt64>(ms));
    timeouts.push_back(std::numeric_limits<UInt64>::max());

    for (UInt64 timeout_ms : timeouts)
    {
        SCOPED_TRACE(timeout_ms);

        auto keeper_context = std::make_shared<DB::KeeperContext>(true, std::make_shared<DB::CoordinationSettings>());
        keeper_context->setLastCommitIndex(1);

        std::thread committer(
            [&]
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(notify_after_ms));
                keeper_context->setLastCommitIndex(10);
            });

        Stopwatch watch;
        const bool committed = keeper_context->waitCommittedUpto(10, timeout_ms);
        const auto elapsed_ms = watch.elapsedMilliseconds();
        committer.join();

        EXPECT_TRUE(committed);
        EXPECT_GE(elapsed_ms, static_cast<UInt64>(notify_after_ms) / 2);
    }
}

/// All three callers of interruptibleSleep build the period from a coordination setting, so this
/// covers each of them. The period arrives typed as std::chrono::milliseconds, whose representation
/// is signed, so the reachable extremes are the signed ones.
TEST(KeeperDispatcher, InterruptibleSleepKeepsHugePeriod)
{
    for (Int64 period_ms : huge_timeouts_ms)
    {
        SCOPED_TRACE(period_ms);

        DB::KeeperDispatcher dispatcher;

        std::thread shutdown_signaller(
            [&]
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(notify_after_ms));
                dispatcher.signalShutdown();
            });

        Stopwatch watch;
        DispatcherAccessor::interruptibleSleep(dispatcher, std::chrono::milliseconds(period_ms));
        const auto elapsed_ms = watch.elapsedMilliseconds();
        /// Sampled before the join: the signaller sets the flag unconditionally, so a reading taken
        /// afterwards would be true whatever ended the wait and would assert nothing.
        const bool signalled_when_the_wait_returned = dispatcher.isShuttingDown();
        shutdown_signaller.join();

        EXPECT_GE(elapsed_ms, static_cast<UInt64>(notify_after_ms) / 2);
        /// The elapsed bound alone would also accept a period silently shortened to anything above
        /// 150 ms, which times out rather than keeping the requested period. This pins why the wait
        /// ended: the predicate became true.
        EXPECT_TRUE(signalled_when_the_wait_returned);
    }
}

/// The opposite direction: a non-positive period must still return immediately, otherwise a
/// shutdown path passing a wrapped negative count would hang instead of expiring at once.
TEST(KeeperDispatcher, InterruptibleSleepReturnsAtOnceForNonPositivePeriod)
{
    /// Below the shortest spurious wait worth catching, so the ordering oracle can observe one.
    constexpr Int64 signal_after_ms = 100;

    for (Int64 period_ms : {Int64{0}, Int64{-1}, Int64{-9'223'372'036'854'775}})
    {
        SCOPED_TRACE(period_ms);

        /// A fresh dispatcher per period: the flag latches once signalled, so a shared one would
        /// already be shutting down after the first iteration and the oracle would read true
        /// without any wait having happened.
        DB::KeeperDispatcher dispatcher;

        std::thread shutdown_signaller(
            [&]
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(signal_after_ms));
                dispatcher.signalShutdown();
            });

        Stopwatch watch;
        DispatcherAccessor::interruptibleSleep(dispatcher, std::chrono::milliseconds(period_ms));
        const auto elapsed_ms = watch.elapsedMilliseconds();
        const bool signalled_when_the_wait_returned = dispatcher.isShuttingDown();
        shutdown_signaller.join();

        /// An elapsed bound accepts any wait shorter than the signal delay, so it cannot say the
        /// wait did not happen. This pins the ordering: the call returned while the predicate was
        /// still false.
        EXPECT_FALSE(signalled_when_the_wait_returned);
        EXPECT_LT(elapsed_ms, static_cast<UInt64>(notify_after_ms));
    }
}

#endif
