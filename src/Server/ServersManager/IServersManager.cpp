#include <Server/ServersManager/IServersManager.h>

#include <Interpreters/Context.h>
#include <Server/waitServersToFinish.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Config/AbstractConfigurationComparison.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/logger_useful.h>
#include <Common/makeSocketAddress.h>

namespace DB
{

namespace ErrorCodes
{
extern const int NETWORK_ERROR;
extern const int INVALID_CONFIG_PARAMETER;
}

IServersManager::IServersManager(ContextMutablePtr global_context_, Poco::Logger * logger_)
    : global_context(global_context_), logger(logger_)
{
}


bool IServersManager::empty() const
{
    return servers.empty();
}

std::vector<ProtocolServerMetrics> IServersManager::getMetrics() const
{
    std::vector<ProtocolServerMetrics> metrics;
    metrics.reserve(servers.size());
    for (const auto & server : servers)
        metrics.emplace_back(ProtocolServerMetrics{server.getPortName(), server.currentThreads(), server.refusedConnections()});
    return metrics;
}

void IServersManager::startServers()
{
    for (auto & server : servers)
    {
        server.start();
        LOG_INFO(logger, "Listening for {}", server.getDescription());
    }
}

void IServersManager::stopServers(const ServerType & server_type)
{
    /// Remove servers once all their connections are closed
    auto check_server = [&](const char prefix[], auto & server)
    {
        if (!server.isStopping())
            return false;
        size_t current_connections = server.currentConnections();
        LOG_DEBUG(
            logger,
            "Server {}{}: {} ({} connections)",
            server.getDescription(),
            prefix,
            !current_connections ? "finished" : "waiting",
            current_connections);
        return !current_connections;
    };

    std::erase_if(servers, std::bind_front(check_server, " (from one of previous remove)"));

    for (auto & server : servers)
    {
        if (!server.isStopping() && server_type.shouldStop(server.getPortName()))
            server.stop();
    }

    std::erase_if(servers, std::bind_front(check_server, ""));
}

void IServersManager::updateServers(
    const Poco::Util::AbstractConfiguration & config,
    IServer & iserver,
    std::mutex & servers_lock,
    Poco::ThreadPool & server_pool,
    AsynchronousMetrics & async_metrics,
    ConfigurationPtr latest_config)
{
    stopServersForUpdate(config, latest_config);
    createServers(config, iserver, servers_lock, server_pool, async_metrics, true, ServerType(ServerType::Type::QUERIES_ALL));
}

Poco::Net::SocketAddress IServersManager::socketBindListen(
    const Poco::Util::AbstractConfiguration & config, Poco::Net::ServerSocket & socket, const std::string & host, UInt16 port) const
{
    auto address = makeSocketAddress(host, port, logger);
    socket.bind(address, /* reuseAddress = */ true, /* reusePort = */ config.getBool("listen_reuse_port", false));
    /// If caller requests any available port from the OS, discover it after binding.
    if (port == 0)
    {
        address = socket.address();
        LOG_DEBUG(logger, "Requested any available port (port == 0), actual port is {:d}", address.port());
    }

    socket.listen(/* backlog = */ config.getUInt("listen_backlog", 4096));
    return address;
}

void IServersManager::createServer(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & listen_host,
    const char * port_name,
    bool start_server,
    CreateServerFunc && func)
{
    /// For testing purposes, user may omit tcp_port or http_port or https_port in configuration file.
    if (config.getString(port_name, "").empty())
        return;

    /// If we already have an active server for this listen_host/port_name, don't create it again
    for (const auto & server : servers)
    {
        if (!server.isStopping() && server.getListenHost() == listen_host && server.getPortName() == port_name)
            return;
    }

    auto port = config.getInt(port_name);
    try
    {
        servers.push_back(func(port));
        if (start_server)
        {
            servers.back().start();
            LOG_INFO(logger, "Listening for {}", servers.back().getDescription());
        }
        global_context->registerServerPort(port_name, port);
    }
    catch (const Poco::Exception &)
    {
        if (!getListenTry(config))
        {
            throw Exception(ErrorCodes::NETWORK_ERROR, "Listen [{}]:{} failed: {}", listen_host, port, getCurrentExceptionMessage(false));
        }
        LOG_WARNING(
            logger,
            "Listen [{}]:{} failed: {}. If it is an IPv6 or IPv4 address and your host has disabled IPv6 or IPv4, "
            "then consider to "
            "specify not disabled IPv4 or IPv6 address to listen in <listen_host> element of configuration "
            "file. Example for disabled IPv6: <listen_host>0.0.0.0</listen_host> ."
            " Example for disabled IPv4: <listen_host>::</listen_host>",
            listen_host,
            port,
            getCurrentExceptionMessage(false));
    }
}

void IServersManager::stopServersForUpdate(const Poco::Util::AbstractConfiguration & config, ConfigurationPtr latest_config)
{
    /// Remove servers once all their connections are closed
    auto check_server = [&](const char prefix[], auto & server)
    {
        if (!server.isStopping())
            return false;
        size_t current_connections = server.currentConnections();
        LOG_DEBUG(
            logger,
            "Server {}{}: {} ({} connections)",
            server.getDescription(),
            prefix,
            !current_connections ? "finished" : "waiting",
            current_connections);
        return !current_connections;
    };

    std::erase_if(servers, std::bind_front(check_server, " (from one of previous reload)"));

    const auto listen_hosts = getListenHosts(config);
    const Poco::Util::AbstractConfiguration & previous_config = latest_config ? *latest_config : config;

    for (auto & server : servers)
    {
        if (server.isStopping())
            return;
        std::string port_name = server.getPortName();
        bool has_host = false;
        bool is_http = false;
        if (port_name.starts_with("protocols."))
        {
            std::string protocol = port_name.substr(0, port_name.find_last_of('.'));
            has_host = config.has(protocol + ".host");

            std::string conf_name = protocol;
            std::string prefix = protocol + ".";
            std::unordered_set<std::string> pset{conf_name};
            while (true)
            {
                if (config.has(prefix + "type"))
                {
                    std::string type = config.getString(prefix + "type");
                    if (type == "http")
                    {
                        is_http = true;
                        break;
                    }
                }

                if (!config.has(prefix + "impl"))
                    break;

                conf_name = "protocols." + config.getString(prefix + "impl");
                prefix = conf_name + ".";

                if (!pset.insert(conf_name).second)
                    throw Exception(
                        ErrorCodes::INVALID_CONFIG_PARAMETER, "Protocol '{}' configuration contains a loop on '{}'", protocol, conf_name);
            }
        }
        else
        {
            /// NOTE: better to compare using getPortName() over using
            /// dynamic_cast<> since HTTPServer is also used for prometheus and
            /// internal replication communications.
            is_http = server.getPortName() == "http_port" || server.getPortName() == "https_port";
        }

        if (!has_host)
            has_host = std::find(listen_hosts.begin(), listen_hosts.end(), server.getListenHost()) != listen_hosts.end();
        bool has_port = !config.getString(port_name, "").empty();
        bool force_restart = is_http && !isSameConfiguration(previous_config, config, "http_handlers");
        if (force_restart)
            LOG_TRACE(logger, "<http_handlers> had been changed, will reload {}", server.getDescription());

        if (!has_host || !has_port || config.getInt(server.getPortName()) != server.portNumber() || force_restart)
        {
            server.stop();
            LOG_INFO(logger, "Stopped listening for {}", server.getDescription());
        }
    }

    std::erase_if(servers, std::bind_front(check_server, ""));
}

Strings IServersManager::getListenHosts(const Poco::Util::AbstractConfiguration & config) const
{
    auto listen_hosts = DB::getMultipleValuesFromConfig(config, "", "listen_host");
    if (listen_hosts.empty())
    {
        listen_hosts.emplace_back("::1");
        listen_hosts.emplace_back("127.0.0.1");
    }
    return listen_hosts;
}

bool IServersManager::getListenTry(const Poco::Util::AbstractConfiguration & config) const
{
    bool listen_try = config.getBool("listen_try", false);
    if (!listen_try)
    {
        Poco::Util::AbstractConfiguration::Keys protocols;
        config.keys("protocols", protocols);
        listen_try = DB::getMultipleValuesFromConfig(config, "", "listen_host").empty()
            && std::none_of(
                         protocols.begin(),
                         protocols.end(),
                         [&](const auto & protocol)
                         { return config.has("protocols." + protocol + ".host") && config.has("protocols." + protocol + ".port"); });
    }
    return listen_try;
}

}
