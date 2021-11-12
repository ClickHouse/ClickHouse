#include <unistd.h>
#include <Coordination/FourLetterCommand.h>
#include <Coordination/KeeperDispatcher.h>
#include <Server/KeeperTCPHandler.h>
#include <base/logger_useful.h>
#include <Poco/Environment.h>
#include <Poco/Path.h>
#include <Poco/StringTokenizer.h>
#include <Common/getCurrentProcessFDCount.h>
#include <Common/getMaxFileDescriptorCount.h>
#include <Common/hex.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IFourLetterCommand::IFourLetterCommand(const KeeperDispatcher & keeper_dispatcher_) : keeper_dispatcher(keeper_dispatcher_)
{
}

Int32 IFourLetterCommand::code()
{
    return toCode(name());
}

String IFourLetterCommand::toName(Int32 code)
{
    return String(reinterpret_cast<char *>(__builtin_bswap32(code)));
}

Int32 IFourLetterCommand::toCode(const String & name)
{
    Int32 res = *reinterpret_cast<const Int32 *>(name.data());
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
    {
        throw Exception("Four letter command  not initialized", ErrorCodes::LOGICAL_ERROR);
    }
}

bool FourLetterCommandFactory::isKnown(Int32 code)
{
    checkInitialization();
    return commands.contains(code);
}

FourLetterCommandPtr FourLetterCommandFactory::get(Int32 code)
{
    checkInitialization();
    return commands.at(code);
}

void FourLetterCommandFactory::registerCommand(FourLetterCommandPtr & command)
{
    if (commands.contains(command->code()))
    {
        throw Exception("Four letter command " + command->name() + " already registered", ErrorCodes::LOGICAL_ERROR);
    }
    commands.emplace(command->code(), std::move(command));
}

void FourLetterCommandFactory::registerCommands(const KeeperDispatcher & keeper_dispatcher)
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

        factory.initializeWhiteList(keeper_dispatcher);
        factory.setInitialize(true);
    }
}

bool FourLetterCommandFactory::isEnabled(Int32 code)
{
    checkInitialization();
    if (!white_list.empty() && *white_list.cbegin() == WHITE_LIST_ALL)
    {
        return true;
    }
    return std::find(white_list.begin(), white_list.end(), code) != white_list.end();
}

void FourLetterCommandFactory::initializeWhiteList(const KeeperDispatcher & keeper_dispatcher)
{
    using Poco::StringTokenizer;
    const auto & keeper_settings = keeper_dispatcher.getKeeperSettings();

    String list_str = keeper_settings->four_letter_word_white_list;
    StringTokenizer tokenizer(list_str, ",", 2);

    for (const String & token : tokenizer)
    {
        if (token == "*")
        {
            white_list.clear();
            white_list.resize(1);
            white_list.push_back(WHITE_LIST_ALL);
            return;
        }
        else
        {
            if (commands.contains(IFourLetterCommand::toCode(token)))
            {
                white_list.push_back(IFourLetterCommand::toCode(token));
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

String MonitorCommand::run()
{
    KeeperStatsPtr stats = keeper_dispatcher.getKeeperStats();
    const IKeeperInfo & keeper_info = keeper_dispatcher.getKeeperInfo();

    if (!keeper_info.hasLeader())
    {
        return "This instance is not currently serving requests";
    }

    const IRaftInfo & raft_info = keeper_dispatcher.getRaftInfo();
    const IStateMachineInfo & state_machine = keeper_dispatcher.getStateMachineInfo();

    StringBuffer ret;
    print(ret, "version", String(VERSION_DESCRIBE) + "-" + VERSION_GITHASH);

    print(ret, "avg_latency", stats->getAvgLatency());
    print(ret, "max_latency", stats->getMaxLatency());
    print(ret, "min_latency", stats->getMinLatency());
    print(ret, "packets_received", stats->getPacketsReceived());
    print(ret, "packets_sent", stats->getPacketsSent());

    print(ret, "num_alive_connections", keeper_info.getNumAliveConnections());
    print(ret, "outstanding_requests", keeper_info.getOutstandingRequests());
    print(ret, "server_state", keeper_info.getRole());

    print(ret, "znode_count", state_machine.getNodeCount());
    print(ret, "watch_count", state_machine.getWatchCount());
    print(ret, "ephemerals_count", state_machine.getEphemeralNodeCount());
    print(ret, "approximate_data_size", state_machine.getApproximateDataSize());

#if defined(__linux__) || defined(__APPLE__)
    print(ret, "open_file_descriptor_count", getCurrentProcessFDCount());
    print(ret, "max_file_descriptor_count", getMaxFileDescriptorCount());
#endif

    if (raft_info.isLeader())
    {
        print(ret, "followers", raft_info.getFollowerCount());
        print(ret, "synced_followers", raft_info.getSyncedFollowerCount());
    }

    return ret.str();
}

void MonitorCommand::print(IFourLetterCommand::StringBuffer & buf, const String & key, const String & value)
{
    writeText("zk_", buf);
    writeText(key, buf);
    writeText('\t', buf);
    writeText(value, buf);
    writeText('\n', buf);
}

void MonitorCommand::print(IFourLetterCommand::StringBuffer & buf, const String & key, UInt64 value)
{
    print(buf, key, toString(value));
}

String StatResetCommand::run()
{
    KeeperStatsPtr stats = keeper_dispatcher.getKeeperStats();
    stats->reset();
    return "Server stats reset.";
}

String NopCommand::run()
{
    return "";
}

String ConfCommand::run()
{
    StringBuffer buf;
    keeper_dispatcher.dumpConf(buf);
    return buf.str();
}

String ConsCommand::run()
{
    StringBuffer buf;
    KeeperTCPHandler::dumpConnections(buf, false);
    return buf.str();
}

String RestConnStatsCommand::run()
{
    KeeperTCPHandler::resetConnsStats();
    return "Connection stats reset.";
}

String ServerStatCommand::run()
{
    StringBuffer buf;

    auto write = [&buf](const String & key, const String & value)
    {
        writeText(key, buf);
        writeText(": ", buf);
        writeText(value, buf);
        writeText('\n', buf);
    };

    KeeperStatsPtr stats = keeper_dispatcher.getKeeperStats();
    const IKeeperInfo & keeper_info = keeper_dispatcher.getKeeperInfo();
    const IStateMachineInfo & state_machine = keeper_dispatcher.getStateMachineInfo();

    write("ClickHouse Keeper version", String(VERSION_DESCRIBE) + "-" + VERSION_GITHASH);

    StringBuffer latency;
    latency << stats->getMinLatency() << "/" << stats->getAvgLatency() << "/" << stats->getMaxLatency() << "\n";
    write("Latency min/avg/max", latency.str());

    write("Received", toString(stats->getPacketsReceived()));
    write("Sent ", toString(stats->getPacketsSent()));
    write("Connections", toString(keeper_info.getNumAliveConnections()));
    write("Outstanding", toString(keeper_info.getOutstandingRequests()));
    write("Zxid", toString(state_machine.getLastProcessedZxid()));
    write("Mode", keeper_info.getRole());
    write("Node count", toString(state_machine.getNodeCount()));

    return buf.str();
}

String StatCommand::run()
{
    StringBuffer buf;

    auto write = [&buf](const String & key, const String & value) { buf << key << ": " << value << '\n'; };

    KeeperStatsPtr stats = keeper_dispatcher.getKeeperStats();
    const IKeeperInfo & keeper_info = keeper_dispatcher.getKeeperInfo();
    const IStateMachineInfo & state_machine = keeper_dispatcher.getStateMachineInfo();

    write("ClickHouse Keeper version", String(VERSION_DESCRIBE) + "-" + VERSION_GITHASH);

    buf << "Clients:\n";
    KeeperTCPHandler::dumpConnections(buf, true);
    buf << '\n';

    StringBuffer latency;
    latency << stats->getMinLatency() << "/" << stats->getAvgLatency() << "/" << stats->getMaxLatency() << "\n";
    write("Latency min/avg/max", latency.str());

    write("Received", toString(stats->getPacketsReceived()));
    write("Sent ", toString(stats->getPacketsSent()));
    write("Connections", toString(keeper_info.getNumAliveConnections()));
    write("Outstanding", toString(keeper_info.getOutstandingRequests()));
    write("Zxid", toString(state_machine.getLastProcessedZxid()));
    write("Mode", keeper_info.getRole());
    write("Node count", toString(state_machine.getNodeCount()));

    return buf.str();
}

String BriefWatchCommand::run()
{
    StringBuffer buf;
    const IStateMachineInfo & state_machine = keeper_dispatcher.getStateMachineInfo();
    buf << keeper_dispatcher.getNumAliveConnections() << " connections watching " << state_machine.getWatchPathCount() << " paths\n";
    buf << "Total watches:" << state_machine.getWatchCount();
    return buf.str();
}

String WatchCommand::run()
{
    StringBuffer buf;
    const IStateMachineInfo & state_machine = keeper_dispatcher.getStateMachineInfo();
    state_machine.dumpWatches(buf);
    return buf.str();
}

String WatchByPathCommand::run()
{
    StringBuffer buf;
    const IStateMachineInfo & state_machine = keeper_dispatcher.getStateMachineInfo();
    state_machine.dumpWatchesByPath(buf);
    return buf.str();
}

String DataSizeCommand::run()
{
    StringBuffer buf;
    buf << "snapshot_dir_size: " << keeper_dispatcher.getSnapDirSize() << '\n';
    buf << "log_dir_size: " << keeper_dispatcher.getDataDirSize() << '\n';
    return buf.str();
}

String DumpCommand::run()
{
    StringBuffer buf;
    const IStateMachineInfo & state_machine = keeper_dispatcher.getStateMachineInfo();
    keeper_dispatcher.dumpSessions(buf);
    state_machine.dumpEphemerals(buf);
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

    char user_name[128];
    getlogin_r(user_name, 128);
    buf << "user.name=" << user_name << '\n';

    buf << "user.home=" << Path::home() << '\n';
    buf << "user.dir=" << Path::current() << '\n';
    buf << "user.tmp=" << Path::temp() << '\n';

    return buf.str();
}

String IsReadOnlyCommand::run()
{
    if (keeper_dispatcher.getRole() == "observer")
        return "ro";
    else
        return "rw";
}

}
