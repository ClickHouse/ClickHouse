#pragma once

#include <Server/IServer.h>
#include <Daemon/BaseDaemon.h>
#include "TinyContext.h"

namespace Poco
{
    namespace Net
    {
        class ServerSocket;
    }
}

namespace DB
{

/// standalone clickhouse-keeper server (replacement for ZooKeeper). Uses the same
/// config as clickhouse-server. Serves requests on TCP ports with or without
/// SSL using ZooKeeper protocol.
class Keeper : public BaseDaemon
{
public:
    using ServerApplication::run;

    Poco::Util::LayeredConfiguration & config() const
    {
        return BaseDaemon::config();
    }

    Poco::Logger & logger() const
    {
        return BaseDaemon::logger();
    }

    bool isCancelled() const
    {
        return BaseDaemon::isCancelled();
    }

    void defineOptions(Poco::Util::OptionSet & _options) override;

protected:
    void logRevision() const override;

    void handleCustomArguments(const std::string & arg, const std::string & value);

    int run() override;

    void initialize(Application & self) override;

    void uninitialize() override;

    int main(const std::vector<std::string> & args) override;

    std::string getDefaultConfigFileName() const override;

private:
    TinyContext tiny_context;

    Poco::Net::SocketAddress socketBindListen(Poco::Net::ServerSocket & socket, const std::string & host, UInt16 port, [[maybe_unused]] bool secure = false) const;

    using CreateServerFunc = std::function<void(UInt16)>;
    void createServer(const std::string & listen_host, const char * port_name, bool listen_try, CreateServerFunc && func) const;
};

}
