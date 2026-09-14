#include <Loggers/Loggers.h>
#include <Loggers/OwnSplitChannel.h>

#include <Common/Exception.h>
#include <base/scope_guard.h>

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
        /// `getChannel()` returns a borrowed pointer, and `AutoPtr(C *)` adopts a reference without adding one.
        /// Take a shared reference instead, otherwise the guard releases a reference it never acquired and the
        /// channel can be destroyed while it is still attached to the root logger.
        root_channel.assign(root.getChannel(), /*shared=*/true);

        std::vector<std::string> names;
        Poco::Logger::names(names);
        logger_states.reserve(names.size());
        for (const auto & name : names)
        {
            auto * logger = Poco::Logger::has(name);
            if (!logger)
                continue;

            LoggerState state;
            state.name = name;
            state.level = logger->getLevel();
            /// Hold a shared reference for the same reason as the root channel above, and so that the channel
            /// survives `buildLoggers` reassigning it below.
            state.channel.assign(logger->getChannel(), /*shared=*/true);
            logger_states.push_back(std::move(state));
        }
    }

    ~LoggerStateGuard()
    {
        auto & root = Poco::Logger::root();
        root.setChannel(root_channel.get());
        root.setLevel(root_level);

        /// Not `const auto &`: `Poco::AutoPtr::get() const` yields a `const Channel *`, which `setChannel` does
        /// not accept.
        for (auto & state : logger_states)
        {
            auto * logger = Poco::Logger::has(state.name);
            if (!logger)
                continue;

            /// `buildLoggers` repoints every already-created logger at a channel owned by the `Loggers`
            /// instance under test, which dies with the test body. Restore the exact channel each logger had
            /// instead of the root channel: the root channel is null in a fresh process, so falling back to it
            /// would silently mute loggers created by earlier tests in the same binary.
            logger->setChannel(state.channel.get());
            logger->setLevel(state.level);
        }
    }

private:
    struct LoggerState
    {
        std::string name;
        int level = 0;
        Poco::AutoPtr<Poco::Channel> channel;
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
    catch (const Poco::TimeoutException &) /// NOLINT(bugprone-empty-catch)
    {
        /// No packet arrived within the timeout, the caller handles the empty result.
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

    /// `Poco::Logger` and `setChannel` both add their own reference, so the channel is held by an `AutoPtr`
    /// here rather than handing over a raw `new` expression, which would leak a reference.
    Poco::AutoPtr<Poco::Channel> null_channel(new Poco::NullChannel);
    Poco::Logger & logger = Poco::Logger::create(nextTestLoggerName(), null_channel.get(), Poco::Message::PRIO_TRACE);
    SCOPE_EXIT({
        /// Avoid leaving a channel owned by `loggers` attached to this dedicated logger, on the throwing path too.
        logger.setChannel(null_channel.get());
    });

    loggers.buildLoggers(*config, logger, cmd_name);
    logger.information("syslog programname test message");

    return receiveDatagram(receiver);
}

/// Builds loggers with the given `programname`, without sending anything, so that configuration
/// validation can be exercised on its own.
void buildLoggersWithProgramName(const std::string & program_name, bool remote)
{
    LoggerStateGuard logger_state_guard;
    Loggers loggers;

    auto config = Poco::AutoPtr<Poco::Util::MapConfiguration>(new Poco::Util::MapConfiguration);
    config->setString("logger", nextTestLoggerName());
    config->setString("logger.async", "false");
    config->setString("logger.level", "trace");
    config->setString("logger.use_syslog", "true");
    config->setString("logger.syslog_level", "trace");
    config->setString("logger.syslog.programname", program_name);
    if (remote)
    {
        config->setString("logger.syslog.address", "127.0.0.1:1514");
        config->setString("logger.syslog.format", "syslog");
    }

    Poco::AutoPtr<Poco::Channel> null_channel(new Poco::NullChannel);
    Poco::Logger & logger = Poco::Logger::create(nextTestLoggerName(), null_channel.get(), Poco::Message::PRIO_TRACE);
    SCOPE_EXIT({
        /// Avoid leaving a channel owned by `loggers` attached to this dedicated logger.
        logger.setChannel(null_channel.get());
    });

    loggers.buildLoggers(*config, logger, "clickhouse-server");
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

TEST(Loggers, RemoteSyslogInvalidCommandNameFallsBackToNilAppName)
{
    /// The `cmd_name` fallback is derived from argv[0], so a renamed binary or symlink can produce a value that
    /// is not a valid RFC 5424 APP-NAME. Before `programname` existed this path sent Poco's `-` default, and a
    /// deployment that never configured the setting must keep getting a well-formed header rather than a
    /// malformed one built from the binary name.
    const auto overlong = sendAndReceiveRemoteSyslogPacket(std::string(49, 'a'), std::nullopt, "syslog");
    ASSERT_FALSE(overlong.empty());
    EXPECT_EQ(extractRFC5424AppName(overlong), "-");

    const auto with_space = sendAndReceiveRemoteSyslogPacket("prod server", std::nullopt, "syslog");
    ASSERT_FALSE(with_space.empty());
    EXPECT_EQ(extractRFC5424AppName(with_space), "-");
}

TEST(Loggers, RemoteSyslogProgramNameIgnoredForBSDFormat)
{
    constexpr auto configured_program_name = "clickhouse-syslog-custom-tag";
    const auto packet = sendAndReceiveRemoteSyslogPacket("clickhouse-server", configured_program_name, "bsd");
    ASSERT_FALSE(packet.empty());
    EXPECT_EQ(packet.find(configured_program_name), std::string::npos);
}

TEST(Loggers, SyslogProgramNameWithWhitespaceIsRejected)
{
    /// A space would be emitted verbatim as RFC 5424 APP-NAME and shift the rest of the message header.
    EXPECT_THROW(buildLoggersWithProgramName("prod server", /*remote=*/true), DB::Exception);
    EXPECT_THROW(buildLoggersWithProgramName("prod\tserver", /*remote=*/true), DB::Exception);
    EXPECT_THROW(buildLoggersWithProgramName("prod server", /*remote=*/false), DB::Exception);
}

TEST(Loggers, SyslogProgramNameWithNonPrintableCharactersIsRejected)
{
    EXPECT_THROW(buildLoggersWithProgramName("clickhouse\nserver", /*remote=*/true), DB::Exception);
    /// UTF-8 encoding of "clickhouse-ü": bytes outside of the printable ASCII range.
    EXPECT_THROW(buildLoggersWithProgramName("clickhouse-\xC3\xBC", /*remote=*/true), DB::Exception);
}

TEST(Loggers, SyslogProgramNameWithInvalidLengthIsRejected)
{
    EXPECT_THROW(buildLoggersWithProgramName("", /*remote=*/true), DB::Exception);
    /// RFC 5424 limits APP-NAME to 48 characters.
    EXPECT_THROW(buildLoggersWithProgramName(std::string(49, 'a'), /*remote=*/true), DB::Exception);
    EXPECT_NO_THROW(buildLoggersWithProgramName(std::string(48, 'a'), /*remote=*/true));
}
