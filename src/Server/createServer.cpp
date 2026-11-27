#include <Server/createServer.h>

#include <Server/ProtocolServerAdapter.h>

#include <Common/CurrentThread.h>
#include <Common/ErrorCodes.h>
#include <Common/Exception.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
}

bool createServer(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & listen_host,
    const char * port_name,
    bool listen_try,
    bool start_server,
    std::vector<ProtocolServerAdapter> & servers,
    CreateServerFunc && func,
    LoggerRawPtr log)
{
    /// For testing purposes, user may omit tcp_port or http_port or https_port in configuration file.
    if (config.getString(port_name, "").empty())
        return false;

    /// If we already have an active server for this listen_host/port_name, don't create it again.
    for (const auto & server : servers)
    {
        if (!server.isStopping() && server.getListenHost() == listen_host && server.getPortName() == port_name)
            return false;
    }

    auto port = config.getInt(port_name);
    try
    {
        servers.push_back(func(static_cast<UInt16>(port)));
        try
        {
            if (start_server)
            {
                servers.back().start();
                LOG_INFO(log, "Listening for {}", servers.back().getDescription());
            }
            return true;
        }
        catch (...)
        {
            /// Roll back the just-pushed adapter so its bound socket is released
            /// and a half-initialized listener does not linger in `servers`. We must
            /// catch all exception types — not only `Poco::Exception` — so the outer
            /// `Poco::Exception` handler below still wraps `Poco`-class errors into
            /// `NETWORK_ERROR` (or logs them when `listen_try`) while non-`Poco`
            /// exceptions propagate to the caller after the rollback.
            servers.pop_back();
            throw;
        }
    }
    catch (const Poco::Exception &)
    {
        if (listen_try)
        {
            LOG_WARNING(log, "Listen [{}]:{} failed: {}. If it is an IPv6 or IPv4 address and your host has disabled IPv6 or IPv4, "
                "then consider to "
                "specify not disabled IPv4 or IPv6 address to listen in <listen_host> element of configuration "
                "file. Example for disabled IPv6: <listen_host>0.0.0.0</listen_host> ."
                " Example for disabled IPv4: <listen_host>::</listen_host>",
                listen_host, port, getCurrentExceptionMessage(false));
        }
        else
        {
            throw Exception(ErrorCodes::NETWORK_ERROR, "Listen [{}]:{} failed: {}", listen_host, port, getCurrentExceptionMessage(false));
        }
    }
    return false;
}

}
