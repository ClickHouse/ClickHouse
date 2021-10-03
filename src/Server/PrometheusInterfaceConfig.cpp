#include <Server/PrometheusInterfaceConfig.h>
#include <Server/InterfaceConfigUtil.h>
#include <Server/ProtocolServerAdapter.h>
#include <Server/IServer.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/HTTP/HTTPServer.h>
#include <Common/Exception.h>
#include <base/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

PrometheusInterfaceConfig::PrometheusInterfaceConfig(const std::string & name_)
    : HTTPInterfaceConfigBase(name_, "prometheus")
{
}

void PrometheusInterfaceConfig::createSingleServer(ProtocolServerAdapter & adapter, const std::string & host, IServer & server, Poco::ThreadPool & pool, AsynchronousMetrics * async_metrics)
{
    if (async_metrics == nullptr)
        throw Exception("AsynchronousMetrics instance not provided", ErrorCodes::LOGICAL_ERROR);

    Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
    http_params->setTimeout(Util::toTimespan(http_receive_timeout));
    http_params->setKeepAliveTimeout(Util::toTimespan(http_keep_alive_timeout));

    Poco::Net::ServerSocket socket;
    auto address = Util::socketBindListen(socket, host, port, secure, reuse_port, backlog, &server.logger());
    socket.setReceiveTimeout(Util::toTimespan(http_receive_timeout));
    socket.setSendTimeout(Util::toTimespan(http_send_timeout));

    adapter.add(std::make_unique<HTTPServer>(
        server.context(), createHandlerFactory(server, *async_metrics, "PrometheusHandler-factory"), pool, socket, http_params, *this));

    LOG_INFO(&server.logger(), "Listening for connections with Prometheus protocol ({}): http://{}", name, address.toString());
}

std::unique_ptr<PrometheusInterfaceConfig> PrometheusInterfaceConfig::tryParseLegacyInterface(
    const LegacyGlobalConfigOverrides & global_overrides,
    const Poco::Util::AbstractConfiguration & config,
    const Settings & settings
)
{
    return Util::tryParseLegacyInterfaceHelper<PrometheusInterfaceConfig>(
        false,
        "prometheus.port",
        "LegacyPrometheus",
        global_overrides,
        config,
        settings
    );
}

}
