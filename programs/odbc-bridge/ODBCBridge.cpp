#include "ODBCBridge.h"
#include "HandlerFactory.h"

#include <string>
#include <errno.h>
#include <IO/ReadHelpers.h>
#include <boost/program_options.hpp>

#if USE_ODBC
// It doesn't make much sense to build this bridge without ODBC, but we still do this.
#    include <Poco/Data/ODBC/Connector.h>
#endif

#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/NetException.h>
#include <Poco/String.h>
#include <Poco/Util/HelpFormatter.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/config.h>
#include <common/logger_useful.h>
#include <ext/scope_guard.h>
#include <ext/range.h>
#include <Common/SensitiveDataMasker.h>

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

void ODBCBridge::handleHelp(const std::string &, const std::string &)
{
    Poco::Util::HelpFormatter help_formatter(options());
    help_formatter.setCommand(commandName());
    help_formatter.setHeader("HTTP-proxy for odbc requests");
    help_formatter.setUsage("--http-port <port>");
    help_formatter.format(std::cerr);

    stopOptionsProcessing();
}


void ODBCBridge::defineOptions(Poco::Util::OptionSet & options)
{
    options.addOption(Poco::Util::Option("http-port", "", "port to listen").argument("http-port", true).binding("http-port"));
    options.addOption(
        Poco::Util::Option("listen-host", "", "hostname to listen, default localhost").argument("listen-host").binding("listen-host"));
    options.addOption(
        Poco::Util::Option("http-timeout", "", "http timeout for socket, default 1800").argument("http-timeout").binding("http-timeout"));

    options.addOption(Poco::Util::Option("max-server-connections", "", "max connections to server, default 1024")
                          .argument("max-server-connections")
                          .binding("max-server-connections"));
    options.addOption(Poco::Util::Option("keep-alive-timeout", "", "keepalive timeout, default 10")
                          .argument("keep-alive-timeout")
                          .binding("keep-alive-timeout"));

    options.addOption(Poco::Util::Option("log-level", "", "sets log level, default info").argument("log-level").binding("logger.level"));

    options.addOption(
        Poco::Util::Option("log-path", "", "log path for all logs, default console").argument("log-path").binding("logger.log"));

    options.addOption(Poco::Util::Option("err-log-path", "", "err log path for all logs, default no")
                          .argument("err-log-path")
                          .binding("logger.errorlog"));

    using Me = std::decay_t<decltype(*this)>;
    options.addOption(Poco::Util::Option("help", "", "produce this help message")
                          .binding("help")
                          .callback(Poco::Util::OptionCallback<Me>(this, &Me::handleHelp)));

    ServerApplication::defineOptions(options); // NOLINT Don't need complex BaseDaemon's .xml config
}

void ODBCBridge::initialize(Application & self)
{
    BaseDaemon::closeFDs();
    is_help = config().has("help");

    if (is_help)
        return;

    config().setString("logger", "ODBCBridge");

    buildLoggers(config(), logger(), self.commandName());

    BaseDaemon::logRevision();

    log = &logger();
    hostname = config().getString("listen-host", "localhost");
    port = config().getUInt("http-port");
    if (port > 0xFFFF)
        throw Exception("Out of range 'http-port': " + std::to_string(port), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

    http_timeout = config().getUInt("http-timeout", DEFAULT_HTTP_READ_BUFFER_TIMEOUT);
    max_server_connections = config().getUInt("max-server-connections", 1024);
    keep_alive_timeout = config().getUInt("keep-alive-timeout", 10);

    initializeTerminationAndSignalProcessing();

#if USE_ODBC
    // It doesn't make much sense to build this bridge without ODBC, but we
    // still do this.
    Poco::Data::ODBC::Connector::registerConnector();
#endif

    ServerApplication::initialize(self); // NOLINT
}

void ODBCBridge::uninitialize()
{
    BaseDaemon::uninitialize();
}

int ODBCBridge::main(const std::vector<std::string> & /*args*/)
{
    if (is_help)
        return Application::EXIT_OK;

    LOG_INFO(log, "Starting up");
    Poco::Net::ServerSocket socket;
    auto address = socketBindListen(socket, hostname, port, log);
    socket.setReceiveTimeout(http_timeout);
    socket.setSendTimeout(http_timeout);
    Poco::ThreadPool server_pool(3, max_server_connections);
    Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
    http_params->setTimeout(http_timeout);
    http_params->setKeepAliveTimeout(keep_alive_timeout);

    auto shared_context = Context::createShared();
    Context context(Context::createGlobal(shared_context.get()));
    context.makeGlobalContext();

    if (config().has("query_masking_rules"))
    {
        SensitiveDataMasker::setInstance(std::make_unique<SensitiveDataMasker>(config(), "query_masking_rules"));
    }

    auto server = Poco::Net::HTTPServer(
        new HandlerFactory("ODBCRequestHandlerFactory-factory", keep_alive_timeout, context), server_pool, socket, http_params);
    server.start();

    LOG_INFO(log, "Listening http://{}", address.toString());

    SCOPE_EXIT({
        LOG_DEBUG(log, "Received termination signal.");
        LOG_DEBUG(log, "Waiting for current connections to close.");
        server.stop();
        for (size_t count : ext::range(1, 6))
        {
            if (server.currentConnections() == 0)
                break;
            LOG_DEBUG(log, "Waiting for {} connections, try {}", server.currentConnections(), count);
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    });

    waitForTerminationRequest();
    return Application::EXIT_OK;
}
}

#pragma GCC diagnostic ignored "-Wmissing-declarations"
int mainEntryClickHouseODBCBridge(int argc, char ** argv)
{
    DB::ODBCBridge app;
    try
    {
        return app.run(argc, argv);
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        auto code = DB::getCurrentExceptionCode();
        return code ? code : 1;
    }
}
