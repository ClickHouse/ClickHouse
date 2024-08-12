#pragma once

#include <Server/IServer.h>

#include <Daemon/BaseDaemon.h>
#include <Server/HTTP/HTTPContext.h>
#include <Server/TCPProtocolStackFactory.h>
#include <Server/ServerType.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Interpreters/ContextShared_fwd.h>

/** Server provides three interfaces:
  * 1. HTTP - simple interface for any applications.
  * 2. TCP - interface for native clickhouse-client and for server to server internal communications.
  *    More rich and efficient, but less compatible
  *     - data is transferred by columns;
  *     - data is transferred compressed;
  *    Allows to get more information in response.
  * 3. Interserver HTTP - for replication.
  */

namespace Poco
{
    namespace Net
    {
        class ServerSocket;
    }
}

namespace DB
{
class AsynchronousMetrics;
class ProtocolServerAdapter;

class Server : public BaseDaemon, public IServer
{
public:
    using ServerApplication::run;

    Poco::Util::LayeredConfiguration & config() const override
    {
        return BaseDaemon::config();
    }

    Poco::Logger & logger() const override
    {
        return BaseDaemon::logger();
    }

    ContextMutablePtr context() const override
    {
        return global_context;
    }

    bool isCancelled() const override
    {
        return BaseDaemon::isCancelled();
    }

    void defineOptions(Poco::Util::OptionSet & _options) override;

protected:
    int run() override;

    void initialize(Application & self) override;

    void uninitialize() override;

    int main(const std::vector<std::string> & args) override;

    std::string getDefaultCorePath() const override;

private:
    SharedContextHolder shared_context_holder;
    ContextMutablePtr global_context;
    /// Updated/recent config, to compare http_handlers
    ConfigurationPtr latest_config;

    HTTPContextPtr httpContext() const;

    Poco::Net::SocketAddress socketBindListen(
        const Poco::Util::AbstractConfiguration & config,
        Poco::Net::ServerSocket & socket,
        const std::string & host,
        UInt16 port,
        [[maybe_unused]] bool secure = false) const;

    std::unique_ptr<TCPProtocolStackFactory> buildProtocolStackFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & protocol,
        Poco::Net::HTTPServerParams::Ptr http_params,
        AsynchronousMetrics & async_metrics,
        bool & is_secure);

    using CreateServerFunc = std::function<ProtocolServerAdapter(UInt16)>;
    void createServer(
        Poco::Util::AbstractConfiguration & config,
        const std::string & listen_host,
        const char * port_name,
        bool listen_try,
        bool start_server,
        std::vector<ProtocolServerAdapter> & servers,
        CreateServerFunc && func) const;

    void createServers(
        Poco::Util::AbstractConfiguration & config,
        const Strings & listen_hosts,
        bool listen_try,
        Poco::ThreadPool & server_pool,
        AsynchronousMetrics & async_metrics,
        std::vector<ProtocolServerAdapter> & servers,
        bool start_servers = false,
        const ServerType & server_type = ServerType(ServerType::Type::QUERIES_ALL));

    void createInterserverServers(
        Poco::Util::AbstractConfiguration & config,
        const Strings & interserver_listen_hosts,
        bool listen_try,
        Poco::ThreadPool & server_pool,
        AsynchronousMetrics & async_metrics,
        std::vector<ProtocolServerAdapter> & servers,
        bool start_servers = false,
        const ServerType & server_type = ServerType(ServerType::Type::QUERIES_ALL));

    void updateServers(
        Poco::Util::AbstractConfiguration & config,
        Poco::ThreadPool & server_pool,
        AsynchronousMetrics & async_metrics,
        std::vector<ProtocolServerAdapter> & servers,
        std::vector<ProtocolServerAdapter> & servers_to_start_before_tables);

    void stopServers(
        std::vector<ProtocolServerAdapter> & servers,
        const ServerType & server_type) const;
};

}
