#include <Loggers/Loggers.h>
#include <Loggers/OwnSplitChannel.h>

#include <gtest/gtest.h>

#include <Poco/Exception.h>
#include <Poco/Logger.h>
#include <Poco/Net/DatagramSocket.h>
#include <Poco/Net/SocketAddress.h>
#include <Poco/NullChannel.h>
#include <Poco/Timespan.h>
#include <Poco/Util/MapConfiguration.h>

#include <array>
#include <atomic>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace
{

std::string nextTestLoggerName()
{
    static std::atomic<size_t> counter{0};
    return "SyslogProgramNameTest_" + std::to_string(counter.fetch_add(1, std::memory_order_relaxed));
}

class LoggerStateGuard
{
public:
    LoggerStateGuard()
    {
        auto & root = Poco::Logger::root();
        root_level = root.getLevel();
        root_channel = root.getChannel();

        std::vector<std::string> names;
        root.names(names);
        logger_states.reserve(names.size());
        for (const auto & name : names)
        {
            auto * logger = Poco::Logger::has(name);
            if (!logger)
                continue;

            LoggerState state;
            state.name = name;
            state.level = logger->getLevel();
            logger_states.push_back(std::move(state));
        }
    }

    ~LoggerStateGuard()
    {
        auto & root = Poco::Logger::root();
        root.setChannel(root_channel.get());
        root.setLevel(root_level);

        for (const auto & state : logger_states)
        {
            auto * logger = Poco::Logger::has(state.name);
            if (!logger)
                continue;

            /// Do not restore per-logger channels: some tests may leave channels with shorter lifetime,
            /// and reattaching those stale pointers can destabilize subsequent tests.
            logger->setChannel(root_channel.get());
            logger->setLevel(state.level);
        }
    }

private:
    struct LoggerState
    {
        std::string name;
        int level = 0;
    };

    int root_level = 0;
    Poco::AutoPtr<Poco::Channel> root_channel;
    std::vector<LoggerState> logger_states;
};

std::string receiveDatagram(Poco::Net::DatagramSocket & socket)
{
    std::array<char, 4096> buffer{};
    Poco::Net::SocketAddress sender;

    try
    {
        int received = socket.receiveFrom(buffer.data(), static_cast<int>(buffer.size()), sender);
        if (received > 0)
            return std::string(buffer.data(), static_cast<size_t>(received));
    }
    catch (const Poco::TimeoutException &)
    {
    }

    return {};
}

std::string extractRFC5424AppName(const std::string & packet)
{
    std::istringstream stream(packet); // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    std::string token;

    /// RFC5424: "<PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID ..."
    for (size_t i = 0; i < 3; ++i)
    {
        if (!(stream >> token))
            return {};
    }

    if (!(stream >> token))
        return {};

    return token;
}

std::string sendAndReceiveRemoteSyslogPacket(
    const std::string & cmd_name,
    const std::optional<std::string> & program_name,
    const std::string & format)
{
    LoggerStateGuard logger_state_guard;
    Loggers loggers;

    Poco::Net::DatagramSocket receiver(Poco::Net::SocketAddress("127.0.0.1", 0));
    receiver.setReceiveTimeout(Poco::Timespan(2, 0));

    auto config = Poco::AutoPtr<Poco::Util::MapConfiguration>(new Poco::Util::MapConfiguration);
    config->setString("logger", nextTestLoggerName());
    config->setString("logger.async", "false");
    config->setString("logger.level", "trace");
    config->setString("logger.use_syslog", "true");
    config->setString("logger.syslog_level", "trace");
    config->setString("logger.syslog.address", "127.0.0.1:" + std::to_string(receiver.address().port()));
    config->setString("logger.syslog.format", format);

    if (program_name)
        config->setString("logger.syslog.programname", *program_name);

    Poco::Logger & logger = Poco::Logger::create(nextTestLoggerName(), new Poco::NullChannel, Poco::Message::PRIO_TRACE);
    loggers.buildLoggers(*config, logger, cmd_name);
    logger.information("syslog programname test message");

    auto packet = receiveDatagram(receiver);

    /// Avoid leaving a channel owned by `loggers` attached to this dedicated logger.
    logger.setChannel(new Poco::NullChannel);

    return packet;
}

}

TEST(Loggers, RemoteSyslogProgramNameCanBeConfigured)
{
    constexpr auto expected_program_name = "clickhouse-production-scc";
    const auto packet = sendAndReceiveRemoteSyslogPacket("clickhouse-server", expected_program_name, "syslog");
    ASSERT_FALSE(packet.empty());
    EXPECT_EQ(extractRFC5424AppName(packet), expected_program_name);
}

TEST(Loggers, RemoteSyslogProgramNameDefaultsToCommandName)
{
    constexpr auto expected_program_name = "clickhouse-server";
    const auto packet = sendAndReceiveRemoteSyslogPacket(expected_program_name, std::nullopt, "syslog");
    ASSERT_FALSE(packet.empty());
    EXPECT_EQ(extractRFC5424AppName(packet), expected_program_name);
}

TEST(Loggers, RemoteSyslogProgramNameIgnoredForBSDFormat)
{
    constexpr auto configured_program_name = "clickhouse-syslog-custom-tag";
    const auto packet = sendAndReceiveRemoteSyslogPacket("clickhouse-server", configured_program_name, "bsd");
    ASSERT_FALSE(packet.empty());
    EXPECT_EQ(packet.find(configured_program_name), std::string::npos);
}
