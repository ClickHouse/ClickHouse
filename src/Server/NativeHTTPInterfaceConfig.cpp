#include <Server/NativeHTTPInterfaceConfig.h>
#include <Server/InterfaceConfigUtil.h>
#include <Server/IServer.h>
#include <Server/ProtocolServerAdapter.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/HTTP/HTTPServer.h>
#include <Common/Exception.h>
#include <base/logger_useful.h>

#if USE_SSL
#   include <Poco/Net/Context.h>
#   include <Poco/Net/SecureServerSocket.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

NativeHTTPInterfaceConfig::NativeHTTPInterfaceConfig(const std::string & name_)
    : HTTPInterfaceConfigBase(name_, "native_http")
{
}

void NativeHTTPInterfaceConfig::createSingleServer(ProtocolServerAdapter & adapter, const std::string & host, IServer & server, Poco::ThreadPool & pool, AsynchronousMetrics * async_metrics)
{
    if (async_metrics == nullptr)
        throw Exception("AsynchronousMetrics instance not provided", ErrorCodes::LOGICAL_ERROR);

    Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
    http_params->setTimeout(Util::toTimespan(http_receive_timeout));
    http_params->setKeepAliveTimeout(Util::toTimespan(http_keep_alive_timeout));

    if (secure)
    {
#if USE_SSL
        Poco::Net::SecureServerSocket socket;
        auto address = Util::socketBindListen(socket, host, port, secure, reuse_port, backlog, &server.logger());
        socket.setReceiveTimeout(Util::toTimespan(http_receive_timeout));
        socket.setSendTimeout(Util::toTimespan(http_send_timeout));

        adapter.add(std::make_unique<HTTPServer>(
            server.context(), createHandlerFactory(server, *async_metrics, "HTTPSHandler-factory"), pool, socket, http_params, *this));

        LOG_INFO(&server.logger(), "Listening for connections with Native HTTPS protocol ({}): https://{}", name, address.toString());
#else
        throw Exception{"Unable to listen for Native HTTPS connections: SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.", ErrorCodes::SUPPORT_IS_DISABLED};
#endif
    }
    else
    {
        Poco::Net::ServerSocket socket;
        auto address = Util::socketBindListen(socket, host, port, secure, reuse_port, backlog, &server.logger());
        socket.setReceiveTimeout(Util::toTimespan(http_receive_timeout));
        socket.setSendTimeout(Util::toTimespan(http_send_timeout));

        adapter.add(std::make_unique<HTTPServer>(
            server.context(), createHandlerFactory(server, *async_metrics, "HTTPHandler-factory"), pool, socket, http_params, *this));

        LOG_INFO(&server.logger(), "Listening for connections with Native HTTP protocol ({}): http://{}", name, address.toString());
    }
}

std::unique_ptr<NativeHTTPInterfaceConfig> NativeHTTPInterfaceConfig::tryParseLegacyInterface(
    const bool secure_,
    const LegacyGlobalConfigOverrides & global_overrides,
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings
)
{
    return Util::tryParseLegacyInterfaceHelper<NativeHTTPInterfaceConfig>(
        secure_,
        (secure_ ? "https_port" : "http_port"),
        (secure_ ? "LegacyNativeHTTPS" : "LegacyNativeHTTP"),
        global_overrides,
        config,
        settings
    );
}

}
