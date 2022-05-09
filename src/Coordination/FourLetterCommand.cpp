#include <Coordination/FourLetterCommand.h>

#include <Coordination/KeeperDispatcher.h>
#include <Server/KeeperTCPHandler.h>
#include <Common/logger_useful.h>
#include <Poco/Environment.h>
#include <Poco/Path.h>
#include <Common/getCurrentProcessFDCount.h>
#include <Common/getMaxFileDescriptorCount.h>
#include <Common/StringUtils/StringUtils.h>
#include <Coordination/Keeper4LWInfo.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <unistd.h>

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
    int reverted_code = __builtin_bswap32(code);
    return String(reinterpret_cast<char *>(&reverted_code), 4);
}

int32_t IFourLetterCommand::toCode(const String & name)
{
    int32_t res = *reinterpret_cast<const int32_t *>(name.data());
    /// keep consistent with Coordination::read method by changing big endian to little endian.
    return __builtin_bswap32(res);
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
        throw Exception("Four letter command  not initialized", ErrorCodes::LOGICAL_ERROR);
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
        throw Exception("Four letter command " + command->name() + " already registered", ErrorCodes::LOGICAL_ERROR);

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
                auto * log = &Poco::Logger::get("FourLetterCommandFactory");
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

    print(ret, "znode_count", state_machine.getNodesCount());
    print(ret, "watch_count", state_machine.getTotalWatchesCount());
    print(ret, "ephemerals_count", state_machine.getTotalEphemeralNodesCount());
    print(ret, "approximate_data_size", state_machine.getApproximateDataSize());
    print(ret, "key_arena_size", state_machine.getKeyArenaSize());
    print(ret, "latest_snapshot_size", state_machine.getLatestSnapshotBufSize());

#if defined(__linux__) || defined(__APPLE__)
    print(ret, "open_file_descriptor_count", getCurrentProcessFDCount());
    print(ret, "max_file_descriptor_count", getMaxFileDescriptorCount());
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

    write("ClickHouse Keeper version", String(VERSION_DESCRIBE) + "-" + VERSION_GITHASH);

    StringBuffer latency;
    latency << stats.getMinLatency() << "/" << stats.getAvgLatency() << "/" << stats.getMaxLatency();
    write("Latency min/avg/max", latency.str());

    write("Received", toString(stats.getPacketsReceived()));
    write("Sent", toString(stats.getPacketsSent()));
    write("Connections", toString(keeper_info.alive_connections_count));
    write("Outstanding", toString(keeper_info.outstanding_requests_count));
    write("Zxid", toString(keeper_info.last_zxid));
    write("Mode", keeper_info.getRole());
    write("Node count", toString(keeper_info.total_nodes_count));

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
    write("Zxid", toString(keeper_info.last_zxid));
    write("Mode", keeper_info.getRole());
    write("Node count", toString(keeper_info.total_nodes_count));

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
    buf << "clickhouse.keeper.version=" << (String(VERSION_DESCRIBE) + "-" + VERSION_GITHASH) << '\n';

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

}
