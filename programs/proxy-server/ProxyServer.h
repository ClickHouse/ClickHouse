#pragma once

#include <Server/ServerType.h>
#include <Server/TCPProtocolStackFactory.h>
#include <Daemon/BaseDaemon.h>

#include <ProxyServer/IProxyServer.h>
#include <ProxyServer/Router.h>

namespace Poco
{
namespace Net
{
class ServerSocket;
}
}

namespace DB
{
class ProtocolServerAdapter;
}

namespace Proxy
{

class ProxyServer : public BaseDaemon, public IProxyServer
{
public:
    using ServerApplication::run;

    Poco::Util::LayeredConfiguration & config() const override { return BaseDaemon::config(); }

    Poco::Logger & logger() const override { return BaseDaemon::logger(); }

    bool isCancelled() const override { return BaseDaemon::isCancelled(); }

    void defineOptions(Poco::Util::OptionSet & _options) override;

protected:
    int run() override;

    void initialize(Application & self) override;

    void uninitialize() override;

    int main(const std::vector<std::string> & args) override;

    std::string getDefaultCorePath() const override;

private:
    /// Updated/recent config, to compare http_handlers
    DB::ConfigurationPtr latest_config;

    Poco::Net::SocketAddress socketBindListen(
        const Poco::Util::AbstractConfiguration & config,
        Poco::Net::ServerSocket & socket,
        const std::string & host,
        UInt16 port,
        [[maybe_unused]] bool secure = false) const;

    using CreateServerFunc = std::function<DB::ProtocolServerAdapter(UInt16)>;
    void createServer(
        Poco::Util::AbstractConfiguration & config,
        const std::string & listen_host,
        const char * port_name,
        bool start_server,
        std::vector<DB::ProtocolServerAdapter> & servers,
        CreateServerFunc && func) const;

    void createServers(
        Poco::Util::AbstractConfiguration & config,
        RouterPtr router,
        const std::vector<std::string> & listen_hosts,
        Poco::ThreadPool & server_pool,
        std::vector<DB::ProtocolServerAdapter> & servers,
        bool start_servers = false,
        const DB::ServerType & server_type = DB::ServerType(DB::ServerType::Type::QUERIES_ALL));
};

}
