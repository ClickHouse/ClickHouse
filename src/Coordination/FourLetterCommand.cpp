#include <Coordination/FourLetterCommand.h>

#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperDispatcher.h>
#include <Server/KeeperTCPHandler.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/logger_useful.h>
#include <Poco/Environment.h>
#include <Poco/Path.h>
#include <Common/getCurrentProcessFDCount.h>
#include <Common/getMaxFileDescriptorCount.h>
#include <Common/StringUtils.h>
#include <Common/config_version.h>
#include "Coordination/KeeperFeatureFlags.h"
#include <Coordination/Keeper4LWInfo.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <boost/algorithm/string.hpp>

#include <unistd.h>
#include <bit>

#if USE_JEMALLOC
#include <Common/Jemalloc.h>
#include <jemalloc/jemalloc.h>
#endif

namespace
{

String formatZxid(int64_t zxid)
{
    /// ZooKeeper print zxid in hex and
    String hex = getHexUIntLowercase(zxid);
    /// without leading zeros
    trimLeft(hex, '0');
    return "0x" + hex;
}

}

#if USE_NURAFT
namespace ProfileEvents
{
    extern const std::vector<Event> keeper_profile_events;
}
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IFourLetterCommand::IFourLetterCommand(KeeperDispatcher & keeper_dispatcher_)
    : keeper_dispatcher(keeper_dispatcher_)
{
}

int32_t IFourLetterCommand::code()
{
    return toCode(name());
}

String IFourLetterCommand::toName(int32_t code)
{
    int reverted_code = std::byteswap(code);
    return String(reinterpret_cast<char *>(&reverted_code), 4);
}

int32_t IFourLetterCommand::toCode(const String & name)
{
    int32_t res = *reinterpret_cast<const int32_t *>(name.data());
    /// keep consistent with Coordination::read method by changing big endian to little endian.
    return std::byteswap(res);
}

IFourLetterCommand::~IFourLetterCommand() = default;

FourLetterCommandFactory & FourLetterCommandFactory::instance()
{
    static FourLetterCommandFactory factory;
    return factory;
}

void FourLetterCommandFactory::checkInitialization() const
{
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Four letter command not initialized");
}

bool FourLetterCommandFactory::isKnown(int32_t code)
{
    checkInitialization();
    return commands.contains(code);
}

FourLetterCommandPtr FourLetterCommandFactory::get(int32_t code)
{
    checkInitialization();
    return commands.at(code);
}

void FourLetterCommandFactory::registerCommand(FourLetterCommandPtr & command)
{
    if (commands.contains(command->code()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Four letter command {} already registered", command->name());

    commands.emplace(command->code(), std::move(command));
}

void FourLetterCommandFactory::registerCommands(KeeperDispatcher & keeper_dispatcher)
{
    FourLetterCommandFactory & factory = FourLetterCommandFactory::instance();

    if (!factory.isInitialized())
    {
        FourLetterCommandPtr ruok_command = std::make_shared<RuokCommand>(keeper_dispatcher);
        factory.registerCommand(ruok_command);

        FourLetterCommandPtr mntr_command = std::make_shared<MonitorCommand>(keeper_dispatcher);
        factory.registerCommand(mntr_command);

        FourLetterCommandPtr conf_command = std::make_shared<ConfCommand>(keeper_dispatcher);
        factory.registerCommand(conf_command);

        FourLetterCommandPtr cons_command = std::make_shared<ConsCommand>(keeper_dispatcher);
        factory.registerCommand(cons_command);

        FourLetterCommandPtr brief_watch_command = std::make_shared<BriefWatchCommand>(keeper_dispatcher);
        factory.registerCommand(brief_watch_command);

        FourLetterCommandPtr data_size_command = std::make_shared<DataSizeCommand>(keeper_dispatcher);
        factory.registerCommand(data_size_command);

        FourLetterCommandPtr dump_command = std::make_shared<DumpCommand>(keeper_dispatcher);
        factory.registerCommand(dump_command);

        FourLetterCommandPtr envi_command = std::make_shared<EnviCommand>(keeper_dispatcher);
        factory.registerCommand(envi_command);

        FourLetterCommandPtr is_rad_only_command = std::make_shared<IsReadOnlyCommand>(keeper_dispatcher);
        factory.registerCommand(is_rad_only_command);

        FourLetterCommandPtr rest_conn_stats_command = std::make_shared<RestConnStatsCommand>(keeper_dispatcher);
        factory.registerCommand(rest_conn_stats_command);

        FourLetterCommandPtr server_stat_command = std::make_shared<ServerStatCommand>(keeper_dispatcher);
        factory.registerCommand(server_stat_command);

        FourLetterCommandPtr stat_command = std::make_shared<StatCommand>(keeper_dispatcher);
        factory.registerCommand(stat_command);

        FourLetterCommandPtr stat_reset_command = std::make_shared<StatResetCommand>(keeper_dispatcher);
        factory.registerCommand(stat_reset_command);

        FourLetterCommandPtr watch_by_path_command = std::make_shared<WatchByPathCommand>(keeper_dispatcher);
        factory.registerCommand(watch_by_path_command);

        FourLetterCommandPtr watch_command = std::make_shared<WatchCommand>(keeper_dispatcher);
        factory.registerCommand(watch_command);

        FourLetterCommandPtr recovery_command = std::make_shared<RecoveryCommand>(keeper_dispatcher);
        factory.registerCommand(recovery_command);

        FourLetterCommandPtr api_version_command = std::make_shared<ApiVersionCommand>(keeper_dispatcher);
        factory.registerCommand(api_version_command);

        FourLetterCommandPtr create_snapshot_command = std::make_shared<CreateSnapshotCommand>(keeper_dispatcher);
        factory.registerCommand(create_snapshot_command);

        FourLetterCommandPtr log_info_command = std::make_shared<LogInfoCommand>(keeper_dispatcher);
        factory.registerCommand(log_info_command);

        FourLetterCommandPtr request_leader_command = std::make_shared<RequestLeaderCommand>(keeper_dispatcher);
        factory.registerCommand(request_leader_command);

        FourLetterCommandPtr recalculate_command = std::make_shared<RecalculateCommand>(keeper_dispatcher);
        factory.registerCommand(recalculate_command);

        FourLetterCommandPtr clean_resources_command = std::make_shared<CleanResourcesCommand>(keeper_dispatcher);
        factory.registerCommand(clean_resources_command);

        FourLetterCommandPtr feature_flags_command = std::make_shared<FeatureFlagsCommand>(keeper_dispatcher);
        factory.registerCommand(feature_flags_command);

        FourLetterCommandPtr yield_leadership_command = std::make_shared<YieldLeadershipCommand>(keeper_dispatcher);
        factory.registerCommand(yield_leadership_command);

#if USE_JEMALLOC
        FourLetterCommandPtr jemalloc_dump_stats = std::make_shared<JemallocDumpStats>(keeper_dispatcher);
        factory.registerCommand(jemalloc_dump_stats);

        FourLetterCommandPtr jemalloc_flush_profile = std::make_shared<JemallocFlushProfile>(keeper_dispatcher);
        factory.registerCommand(jemalloc_flush_profile);

        FourLetterCommandPtr jemalloc_enable_profile = std::make_shared<JemallocEnableProfile>(keeper_dispatcher);
        factory.registerCommand(jemalloc_enable_profile);

        FourLetterCommandPtr jemalloc_disable_profile = std::make_shared<JemallocDisableProfile>(keeper_dispatcher);
        factory.registerCommand(jemalloc_disable_profile);
#endif
        FourLetterCommandPtr profile_events_command = std::make_shared<ProfileEventsCommand>(keeper_dispatcher);
        factory.registerCommand(profile_events_command);

        factory.initializeAllowList(keeper_dispatcher);
        factory.setInitialize(true);
    }
}

bool FourLetterCommandFactory::isEnabled(int32_t code)
{
    checkInitialization();
    if (!allow_list.empty() && *allow_list.cbegin() == ALLOW_LIST_ALL)
        return true;

    return std::find(allow_list.begin(), allow_list.end(), code) != allow_list.end();
}

void FourLetterCommandFactory::initializeAllowList(KeeperDispatcher & keeper_dispatcher)
{
    const auto & keeper_settings = keeper_dispatcher.getKeeperConfigurationAndSettings();

    String list_str = keeper_settings->four_letter_word_allow_list;
    Strings tokens;
    splitInto<','>(tokens, list_str);

    for (String token: tokens)
    {
        trim(token);

        if (token == "*")
        {
            allow_list.clear();
            allow_list.push_back(ALLOW_LIST_ALL);
            return;
        }
        else
        {
            if (commands.contains(IFourLetterCommand::toCode(token)))
            {
                allow_list.push_back(IFourLetterCommand::toCode(token));
            }
            else
            {
                auto log = getLogger("FourLetterCommandFactory");
                LOG_WARNING(log, "Find invalid keeper 4lw command {} when initializing, ignore it.", token);
            }
        }
    }
}

String RuokCommand::run()
{
    return "imok";
}

namespace
{

void print(IFourLetterCommand::StringBuffer & buf, const String & key, const String & value)
{
    writeText("zk_", buf);
    writeText(key, buf);
    writeText('\t', buf);
    writeText(value, buf);
    writeText('\n', buf);
}

void print(IFourLetterCommand::StringBuffer & buf, const String & key, uint64_t value)
{
    print(buf, key, toString(value));
}

constexpr auto * SERVER_NOT_ACTIVE_MSG = "This instance is not currently serving requests";

}

String MonitorCommand::run()
{
    if (!keeper_dispatcher.isServerActive())
        return SERVER_NOT_ACTIVE_MSG;

    auto & stats = keeper_dispatcher.getKeeperConnectionStats();
    Keeper4LWInfo keeper_info = keeper_dispatcher.getKeeper4LWInfo();

    const auto & state_machine = keeper_dispatcher.getStateMachine();

    StringBuffer ret;
    print(ret, "version", String(VERSION_DESCRIBE) + "-" + VERSION_GITHASH);

    print(ret, "avg_latency", stats.getAvgLatency());
    print(ret, "max_latency", stats.getMaxLatency());
    print(ret, "min_latency", stats.getMinLatency());
    print(ret, "packets_received", stats.getPacketsReceived());
    print(ret, "packets_sent", stats.getPacketsSent());

    print(ret, "num_alive_connections", keeper_info.alive_connections_count);
    print(ret, "outstanding_requests", keeper_info.outstanding_requests_count);

    print(ret, "server_state", keeper_info.getRole());

    const auto & storage_stats = state_machine.getStorageStats();

    print(ret, "znode_count", storage_stats.nodes_count.load(std::memory_order_relaxed));
    print(ret, "watch_count", storage_stats.total_watches_count.load(std::memory_order_relaxed));
    print(ret, "ephemerals_count", storage_stats.total_emphemeral_nodes_count.load(std::memory_order_relaxed));
    print(ret, "approximate_data_size", storage_stats.approximate_data_size.load(std::memory_order_relaxed));
    print(ret, "key_arena_size", 0);
    print(ret, "latest_snapshot_size", state_machine.getLatestSnapshotSize());

#if defined(OS_LINUX) || defined(OS_DARWIN)
    print(ret, "open_file_descriptor_count", getCurrentProcessFDCount());
    auto max_file_descriptor_count = getMaxFileDescriptorCount();
    if (max_file_descriptor_count.has_value())
        print(ret, "max_file_descriptor_count", *max_file_descriptor_count);
    else
        print(ret, "max_file_descriptor_count", -1);
#endif

    if (keeper_info.is_leader)
    {
        print(ret, "followers", keeper_info.follower_count);
        print(ret, "synced_followers", keeper_info.synced_follower_count);
    }

    return ret.str();
}

String StatResetCommand::run()
{
    if (!keeper_dispatcher.isServerActive())
        return SERVER_NOT_ACTIVE_MSG;

    keeper_dispatcher.resetConnectionStats();
    return "Server stats reset.\n";
}

String NopCommand::run()
{
    return "";
}

String ConfCommand::run()
{
    if (!keeper_dispatcher.isServerActive())
        return SERVER_NOT_ACTIVE_MSG;

    StringBuffer buf;
    keeper_dispatcher.getKeeperConfigurationAndSettings()->dump(buf);
    keeper_dispatcher.getKeeperContext()->dumpConfiguration(buf);
    return buf.str();
}

String ConsCommand::run()
{
    if (!keeper_dispatcher.isServerActive())
        return SERVER_NOT_ACTIVE_MSG;

    StringBuffer buf;
    KeeperTCPHandler::dumpConnections(buf, false);
    return buf.str();
}

String RestConnStatsCommand::run()
{
    if (!keeper_dispatcher.isServerActive())
        return SERVER_NOT_ACTIVE_MSG;

    KeeperTCPHandler::resetConnsStats();
    return "Connection stats reset.\n";
}

String ServerStatCommand::run()
{
    if (!keeper_dispatcher.isServerActive())
        return SERVER_NOT_ACTIVE_MSG;

    StringBuffer buf;

    auto write = [&buf](const String & key, const String & value)
    {
        writeText(key, buf);
        writeText(": ", buf);
        writeText(value, buf);
        writeText('\n', buf);
    };

    auto & stats = keeper_dispatcher.getKeeperConnectionStats();
    Keeper4LWInfo keeper_info = keeper_dispatcher.getKeeper4LWInfo();
    const auto & storage_stats = keeper_dispatcher.getStateMachine().getStorageStats();

    write("ClickHouse Keeper version", String(VERSION_DESCRIBE) + "-" + VERSION_GITHASH);

    StringBuffer latency;
    latency << stats.getMinLatency() << "/" << stats.getAvgLatency() << "/" << stats.getMaxLatency();
    write("Latency min/avg/max", latency.str());

    write("Received", toString(stats.getPacketsReceived()));
    write("Sent", toString(stats.getPacketsSent()));
    write("Connections", toString(keeper_info.alive_connections_count));
    write("Outstanding", toString(keeper_info.outstanding_requests_count));
    write("Zxid", formatZxid(storage_stats.last_zxid.load(std::memory_order_relaxed)));
    write("Mode", keeper_info.getRole());
    write("Node count", toString(storage_stats.nodes_count.load(std::memory_order_relaxed)));

    return buf.str();
}

String StatCommand::run()
{
    if (!keeper_dispatcher.isServerActive())
        return SERVER_NOT_ACTIVE_MSG;

    StringBuffer buf;

    auto write = [&buf] (const String & key, const String & value) { buf << key << ": " << value << '\n'; };

    auto & stats = keeper_dispatcher.getKeeperConnectionStats();
    Keeper4LWInfo keeper_info = keeper_dispatcher.getKeeper4LWInfo();
    const auto & storage_stats = keeper_dispatcher.getStateMachine().getStorageStats();

    write("ClickHouse Keeper version", String(VERSION_DESCRIBE) + "-" + VERSION_GITHASH);

    buf << "Clients:\n";
    KeeperTCPHandler::dumpConnections(buf, true);
    buf << '\n';

    StringBuffer latency;
    latency << stats.getMinLatency() << "/" << stats.getAvgLatency() << "/" << stats.getMaxLatency();
    write("Latency min/avg/max", latency.str());

    write("Received", toString(stats.getPacketsReceived()));
    write("Sent", toString(stats.getPacketsSent()));
    write("Connections", toString(keeper_info.alive_connections_count));
    write("Outstanding", toString(keeper_info.outstanding_requests_count));
    write("Zxid", formatZxid(storage_stats.last_zxid.load(std::memory_order_relaxed)));
    write("Mode", keeper_info.getRole());
    write("Node count", toString(storage_stats.nodes_count.load(std::memory_order_relaxed)));

    return buf.str();
}

String BriefWatchCommand::run()
{
    if (!keeper_dispatcher.isServerActive())
        return SERVER_NOT_ACTIVE_MSG;

    StringBuffer buf;
    const auto & state_machine = keeper_dispatcher.getStateMachine();
    buf << state_machine.getSessionsWithWatchesCount() << " connections watching "
        << state_machine.getWatchedPathsCount() << " paths\n";
    buf << "Total watches:" << state_machine.getTotalWatchesCount() << "\n";
    return buf.str();
}

String WatchCommand::run()
{
    if (!keeper_dispatcher.isServerActive())
        return SERVER_NOT_ACTIVE_MSG;

    StringBuffer buf;
    const auto & state_machine = keeper_dispatcher.getStateMachine();
    state_machine.dumpWatches(buf);
    return buf.str();
}

String WatchByPathCommand::run()
{
    if (!keeper_dispatcher.isServerActive())
        return SERVER_NOT_ACTIVE_MSG;

    StringBuffer buf;
    const auto & state_machine = keeper_dispatcher.getStateMachine();
    state_machine.dumpWatchesByPath(buf);
    return buf.str();
}

String DataSizeCommand::run()
{
    if (!keeper_dispatcher.isServerActive())
        return SERVER_NOT_ACTIVE_MSG;

    StringBuffer buf;
    buf << "snapshot_dir_size: " << keeper_dispatcher.getSnapDirSize() << '\n';
    buf << "log_dir_size: " << keeper_dispatcher.getLogDirSize() << '\n';
    return buf.str();
}

String DumpCommand::run()
{
    if (!keeper_dispatcher.isServerActive())
        return SERVER_NOT_ACTIVE_MSG;

    StringBuffer buf;
    const auto & state_machine = keeper_dispatcher.getStateMachine();
    state_machine.dumpSessionsAndEphemerals(buf);
    return buf.str();
}

String EnviCommand::run()
{
    using Poco::Environment;
    using Poco::Path;

    StringBuffer buf;
    buf << "Environment:\n";
    buf << "clickhouse.keeper.version=" << VERSION_DESCRIBE << '-' << VERSION_GITHASH << '\n';

    buf << "host.name=" << Environment::nodeName() << '\n';
    buf << "os.name=" << Environment::osDisplayName() << '\n';
    buf << "os.arch=" << Environment::osArchitecture() << '\n';
    buf << "os.version=" << Environment::osVersion() << '\n';
    buf << "cpu.count=" << Environment::processorCount() << '\n';

    String os_user;
    os_user.resize(256, '\0');
    if (0 == getlogin_r(os_user.data(), os_user.size() - 1))
        os_user.resize(strlen(os_user.c_str()));
    else
        os_user.clear();    /// Don't mind if we cannot determine user login.

    buf << "user.name=" << os_user << '\n';

    buf << "user.home=" << Path::home() << '\n';
    buf << "user.dir=" << Path::current() << '\n';
    buf << "user.tmp=" << Path::temp() << '\n';

    return buf.str();
}

String IsReadOnlyCommand::run()
{
    if (keeper_dispatcher.isObserver())
        return "ro";
    else
        return "rw";
}

String RecoveryCommand::run()
{
    keeper_dispatcher.forceRecovery();
    return "ok";
}

String ApiVersionCommand::run()
{
    return toString(static_cast<uint8_t>(KeeperApiVersion::WITH_MULTI_READ));
}

String CreateSnapshotCommand::run()
{
    auto log_index = keeper_dispatcher.createSnapshot();
    return log_index > 0 ? std::to_string(log_index) : "Failed to schedule snapshot creation task.";
}

String LogInfoCommand::run()
{
    KeeperLogInfo log_info = keeper_dispatcher.getKeeperLogInfo();
    StringBuffer ret;

    auto append = [&ret] (String key, uint64_t value) -> void
    {
        writeText(key, ret);
        writeText('\t', ret);
        writeText(std::to_string(value), ret);
        writeText('\n', ret);
    };
    append("first_log_idx", log_info.first_log_idx);
    append("first_log_term", log_info.first_log_idx);
    append("last_log_idx", log_info.last_log_idx);
    append("last_log_term", log_info.last_log_term);
    append("last_committed_log_idx", log_info.last_committed_log_idx);
    append("leader_committed_log_idx", log_info.leader_committed_log_idx);
    append("target_committed_log_idx", log_info.target_committed_log_idx);
    append("last_snapshot_idx", log_info.last_snapshot_idx);

    append("latest_logs_cache_entries", log_info.latest_logs_cache_entries);
    append("latest_logs_cache_size", log_info.latest_logs_cache_size);

    append("commit_logs_cache_entries", log_info.commit_logs_cache_entries);
    append("commit_logs_cache_size", log_info.commit_logs_cache_size);
    return ret.str();
}

String RequestLeaderCommand::run()
{
    return keeper_dispatcher.requestLeader() ? "Sent leadership request to leader." : "Failed to send leadership request to leader.";
}

String RecalculateCommand::run()
{
    keeper_dispatcher.recalculateStorageStats();
    return "ok";
}

String CleanResourcesCommand::run()
{
    KeeperDispatcher::cleanResources();
    return "ok";
}

String FeatureFlagsCommand::run()
{
    const auto & feature_flags = keeper_dispatcher.getKeeperContext()->getFeatureFlags();

    StringBuffer ret;

    auto append = [&ret] (const String & key, uint8_t value) -> void
    {
        writeText(key, ret);
        writeText('\t', ret);
        writeText(std::to_string(value), ret);
        writeText('\n', ret);
    };

    for (const auto & [feature_flag, name] : magic_enum::enum_entries<KeeperFeatureFlag>())
    {
        std::string feature_flag_string(name);
        boost::to_lower(feature_flag_string);
        append(feature_flag_string, feature_flags.isEnabled(feature_flag));
    }

    return ret.str();
}

String YieldLeadershipCommand::run()
{
    keeper_dispatcher.yieldLeadership();
    return "Sent yield leadership request to leader.";
}

#if USE_JEMALLOC

void printToString(void * output, const char * data)
{
    std::string * output_data = reinterpret_cast<std::string *>(output);
    *output_data += std::string(data);
}

String JemallocDumpStats::run()
{
    std::string output;
    malloc_stats_print(printToString, &output, nullptr);
    return output;
}

String JemallocFlushProfile::run()
{
    return flushJemallocProfile("/tmp/jemalloc_keeper");
}

String JemallocEnableProfile::run()
{
    setJemallocProfileActive(true);
    return "ok";
}

String JemallocDisableProfile::run()
{
    setJemallocProfileActive(false);
    return "ok";
}
#endif

String ProfileEventsCommand::run()
{
    StringBuffer ret;

#if USE_NURAFT
    auto append = [&ret] (const String & metric, uint64_t value, const String & docs) -> void
    {
        writeText(metric, ret);
        writeText('\t', ret);
        writeText(std::to_string(value), ret);
        writeText('\t', ret);
        writeText(docs, ret);
        writeText('\n', ret);
    };

    for (auto i : ProfileEvents::keeper_profile_events)
    {
        const auto counter = ProfileEvents::global_counters[i].load(std::memory_order_relaxed);
        std::string metric_name{ProfileEvents::getName(static_cast<ProfileEvents::Event>(i))};
        std::string metric_doc{ProfileEvents::getDocumentation(static_cast<ProfileEvents::Event>(i))};
        append(metric_name, counter, metric_doc);
    }
#endif

    return ret.str();
}

}
