#include "IBridge.h"

#include <boost/program_options.hpp>
#include <Poco/Net/NetException.h>
#include <Poco/Util/HelpFormatter.h>

#include <base/logger_useful.h>
#include <base/range.h>

#include <Common/StringUtils/StringUtils.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/config.h>
#include <base/errnoToString.h>
#include <IO/ReadHelpers.h>
#include <Formats/registerFormats.h>
#include <Server/HTTP/HTTPServer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <sys/time.h>
#include <sys/resource.h>

#if USE_ODBC
#    include <Poco/Data/ODBC/Connector.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
}

namespace
{
    Poco::Net::SocketAddress makeSocketAddress(const std::string & host, UInt16 port, Poco::Logger * log)
    {
        Poco::Net::SocketAddress socket_address;
        try
        {
            socket_address = Poco::Net::SocketAddress(host, port);
        }
        catch (const Poco::Net::DNSException & e)
        {
            const auto code = e.code();
            if (code == EAI_FAMILY
#if defined(EAI_ADDRFAMILY)
                || code == EAI_ADDRFAMILY
#endif
            )
            {
                LOG_ERROR(log, "Cannot resolve listen_host ({}), error {}: {}. If it is an IPv6 address and your host has disabled IPv6, then consider to specify IPv4 address to listen in <listen_host> element of configuration file. Example: <listen_host>0.0.0.0</listen_host>", host, e.code(), e.message());
            }

            throw;
        }
        return socket_address;
    }

    Poco::Net::SocketAddress socketBindListen(Poco::Net::ServerSocket & socket, const std::string & host, UInt16 port, Poco::Logger * log)
    {
        auto address = makeSocketAddress(host, port, log);
#if POCO_VERSION < 0x01080000
        socket.bind(address, /* reuseAddress = */ true);
#else
        socket.bind(address, /* reuseAddress = */ true, /* reusePort = */ false);
#endif

        socket.listen(/* backlog = */ 64);

        return address;
    }
}


void IBridge::handleHelp(const std::string &, const std::string &)
{
    Poco::Util::HelpFormatter help_formatter(options());
    help_formatter.setCommand(commandName());
    help_formatter.setHeader("HTTP-proxy for odbc requests");
    help_formatter.setUsage("--http-port <port>");
    help_formatter.format(std::cerr);

    stopOptionsProcessing();
}


void IBridge::defineOptions(Poco::Util::OptionSet & options)
{
    options.addOption(
        Poco::Util::Option("http-port", "", "port to listen").argument("http-port", true) .binding("http-port"));

    options.addOption(
        Poco::Util::Option("listen-host", "", "hostname or address to listen, default 127.0.0.1").argument("listen-host").binding("listen-host"));

    options.addOption(
        Poco::Util::Option("http-timeout", "", "http timeout for socket, default 1800").argument("http-timeout").binding("http-timeout"));

    options.addOption(
        Poco::Util::Option("max-server-connections", "", "max connections to server, default 1024").argument("max-server-connections").binding("max-server-connections"));

    options.addOption(
        Poco::Util::Option("keep-alive-timeout", "", "keepalive timeout, default 10").argument("keep-alive-timeout").binding("keep-alive-timeout"));

    options.addOption(
        Poco::Util::Option("log-level", "", "sets log level, default info") .argument("log-level").binding("logger.level"));

    options.addOption(
        Poco::Util::Option("log-path", "", "log path for all logs, default console").argument("log-path").binding("logger.log"));

    options.addOption(
        Poco::Util::Option("err-log-path", "", "err log path for all logs, default no").argument("err-log-path").binding("logger.errorlog"));

    options.addOption(
        Poco::Util::Option("stdout-path", "", "stdout log path, default console").argument("stdout-path").binding("logger.stdout"));

    options.addOption(
        Poco::Util::Option("stderr-path", "", "stderr log path, default console").argument("stderr-path").binding("logger.stderr"));

    using Me = std::decay_t<decltype(*this)>;

    options.addOption(
        Poco::Util::Option("help", "", "produce this help message").binding("help").callback(Poco::Util::OptionCallback<Me>(this, &Me::handleHelp)));

    ServerApplication::defineOptions(options); // NOLINT Don't need complex BaseDaemon's .xml config
}


void IBridge::initialize(Application & self)
{
    BaseDaemon::closeFDs();
    is_help = config().has("help");

    if (is_help)
        return;

    config().setString("logger", bridgeName());

    /// Redirect stdout, stderr to specified files.
    /// Some libraries and sanitizers write to stderr in case of errors.
    const auto stdout_path = config().getString("logger.stdout", "");
    if (!stdout_path.empty())
    {
        if (!freopen(stdout_path.c_str(), "a+", stdout))
            throw Poco::OpenFileException("Cannot attach stdout to " + stdout_path);

        /// Disable buffering for stdout.
        setbuf(stdout, nullptr);
    }
    const auto stderr_path = config().getString("logger.stderr", "");
    if (!stderr_path.empty())
    {
        if (!freopen(stderr_path.c_str(), "a+", stderr))
            throw Poco::OpenFileException("Cannot attach stderr to " + stderr_path);

        /// Disable buffering for stderr.
        setbuf(stderr, nullptr);
    }

    buildLoggers(config(), logger(), self.commandName());

    BaseDaemon::logRevision();

    log = &logger();
    hostname = config().getString("listen-host", "127.0.0.1");
    port = config().getUInt("http-port");
    if (port > 0xFFFF)
        throw Exception("Out of range 'http-port': " + std::to_string(port), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    http_timeout = config().getUInt64("http-timeout", DEFAULT_HTTP_READ_BUFFER_TIMEOUT);
    max_server_connections = config().getUInt("max-server-connections", 1024);
    keep_alive_timeout = config().getUInt64("keep-alive-timeout", 10);

    struct rlimit limit;
    const UInt64 gb = 1024 * 1024 * 1024;

    /// Set maximum RSS to 1 GiB.
    limit.rlim_max = limit.rlim_cur = gb;
    if (setrlimit(RLIMIT_RSS, &limit))
        LOG_WARNING(log, "Unable to set maximum RSS to 1GB: {} (current rlim_cur={}, rlim_max={})",
                    errnoToString(errno), limit.rlim_cur, limit.rlim_max);

    if (!getrlimit(RLIMIT_RSS, &limit))
        LOG_INFO(log, "RSS limit: cur={}, max={}", limit.rlim_cur, limit.rlim_max);

    try
    {
        const auto oom_score = toString(config().getUInt64("bridge_oom_score", 500));
        WriteBufferFromFile buf("/proc/self/oom_score_adj");
        buf.write(oom_score.data(), oom_score.size());
        buf.close();
        LOG_INFO(log, "OOM score is set to {}", oom_score);
    }
    catch (const Exception & e)
    {
        LOG_WARNING(log, "Failed to set OOM score, error: {}", e.what());
    }

    initializeTerminationAndSignalProcessing();

    ServerApplication::initialize(self); // NOLINT
}


void IBridge::uninitialize()
{
    BaseDaemon::uninitialize();
}


int IBridge::main(const std::vector<std::string> & /*args*/)
{
    if (is_help)
        return Application::EXIT_OK;

    registerFormats();
    LOG_INFO(log, "Starting up {} on host: {}, port: {}", bridgeName(), hostname, port);

    Poco::Net::ServerSocket socket;
    auto address = socketBindListen(socket, hostname, port, log);
    socket.setReceiveTimeout(http_timeout);
    socket.setSendTimeout(http_timeout);

    Poco::ThreadPool server_pool(3, max_server_connections);

    Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
    http_params->setTimeout(http_timeout);
    http_params->setKeepAliveTimeout(keep_alive_timeout);

    auto shared_context = Context::createShared();
    auto context = Context::createGlobal(shared_context.get());
    context->makeGlobalContext();

    if (config().has("query_masking_rules"))
        SensitiveDataMasker::setInstance(std::make_unique<SensitiveDataMasker>(config(), "query_masking_rules"));

    auto server = HTTPServer(
        context,
        getHandlerFactoryPtr(context),
        server_pool,
        socket,
        http_params);

    SCOPE_EXIT({
        LOG_DEBUG(log, "Received termination signal.");
        LOG_DEBUG(log, "Waiting for current connections to close.");

        server.stop();

        for (size_t count : collections::range(1, 6))
        {
            if (server.currentConnections() == 0)
                break;
            LOG_DEBUG(log, "Waiting for {} connections, try {}", server.currentConnections(), count);
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    });

    server.start();
    LOG_INFO(log, "Listening http://{}", address.toString());

    waitForTerminationRequest();
    return Application::EXIT_OK;
}

}
