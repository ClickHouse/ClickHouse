#include <Coordination/FourLetterCommand.h>
#include <Coordination/KeeperDispatcher.h>
#include <Poco/StringTokenizer.h>
#include <Common/getCurrentProcessFDCount.h>
#include <Common/getMaxFileDescriptorCount.h>

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

void IFourLetterCommand::printSet(IFourLetterCommand::StringBuffer & buffer, std::unordered_set<String> & set, String && prefix)
{
    for (const auto & str : set)
    {
        buffer.write(prefix.data(), prefix.size());
        buffer.write(str.data(), str.size());
        buffer.write('\n');
    }
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
    auto * log = &Poco::Logger::get("FourLetterCommandFactory");
    LOG_INFO(log, "Register four letter command {}, code {}", command->name(), std::to_string(command->code()));
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

        FourLetterCommandPtr srst_command = std::make_shared<StatResetCommand>(keeper_dispatcher);
        factory.registerCommand(srst_command);

        FourLetterCommandPtr conf_command = std::make_shared<ConfCommand>(keeper_dispatcher);
        factory.registerCommand(conf_command);

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

String RuokCommand::name()
{
    return "ruok";
}

String RuokCommand::run()
{
    return "imok";
}

RuokCommand::~RuokCommand() = default;

String MonitorCommand::name()
{
    return "mntr";
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
    print(ret, "ephemerals_count", state_machine.getEphemeralCount());
    print(ret, "approximate_data_size", state_machine.getApproximateDataSize());

#if defined(__linux__) || defined(__APPLE__)
    print(ret, "open_file_descriptor_count", getCurrentProcessFDCount());
    print(ret, "max_file_descriptor_count", getMaxFileDescriptorCount());
#endif

    if (raft_info.isLeader())
    {
        print(ret, "followers", raft_info.getFollowerCount());
        print(ret, "synced_followers", raft_info.getSyncedFollowerCount());
        /// TODO implementation
        /// print(ret, "pending_syncs", 0);
    }

    /// TODO Maybe the next 3 metrics are useless.
    /// print(ret, "last_proposal_size", -1);
    /// print(ret, "max_proposal_size", -1);
    /// print(ret, "min_proposal_size", -1);

    return ret.str();
}

void MonitorCommand::print(IFourLetterCommand::StringBuffer & buf, const String & key, const String & value)
{
    const static String prefix = "zk_";
    const int prefix_len = prefix.size();

    buf.write(prefix.data(), prefix_len);
    buf.write(key.data(), key.size());

    buf.write('\t');
    buf.write(value.data(), value.size());
    buf.write('\n');
}

void MonitorCommand::print(IFourLetterCommand::StringBuffer & buf, const String & key, UInt64 value)
{
    print(buf, key, std::to_string(value));
}

MonitorCommand::~MonitorCommand() = default;

String StatResetCommand::name()
{
    return "srst";
}

String StatResetCommand::run()
{
    KeeperStatsPtr stats = keeper_dispatcher.getKeeperStats();
    stats->reset();
    return "Server stats reset.";
}

StatResetCommand::~StatResetCommand() = default;

String NopCommand::name()
{
    return "nopc";
}

String NopCommand::run()
{
    return DB::String();
}

NopCommand::~NopCommand() = default;

String ConfCommand::name()
{
    return "conf";
}

String ConfCommand::run()
{
    StringBuffer buf;
    keeper_dispatcher.dumpConf(buf);
    return buf.str();
}

ConfCommand::~ConfCommand() = default;

}
