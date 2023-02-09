#pragma once

#include <Interpreters/Context.h>
#include <Server/HTTP/HTTPRequestHandlerFactory.h>
#include <Daemon/BaseDaemon.h>

#include <Poco/Logger.h>
#include <Poco/Util/ServerApplication.h>


namespace DB
{

/// Class represents base for clickhouse-odbc-bridge and clickhouse-library-bridge servers.
/// Listens to incoming HTTP POST and GET requests on specified port and host.
/// Has two handlers '/' for all incoming POST requests and /ping for GET request about service status.
class IBridge : public BaseDaemon
{

public:
    /// Define command line arguments
    void defineOptions(Poco::Util::OptionSet & options) override;

protected:
    using HandlerFactoryPtr = std::shared_ptr<HTTPRequestHandlerFactory>;

    void initialize(Application & self) override;

    void uninitialize() override;

    int main(const std::vector<std::string> & args) override;

    virtual std::string bridgeName() const = 0;

    virtual HandlerFactoryPtr getHandlerFactoryPtr(ContextPtr context) const = 0;

    size_t keep_alive_timeout;

private:
    void handleHelp(const std::string &, const std::string &);

    bool is_help;
    std::string hostname;
    size_t port;
    std::string log_level;
    size_t max_server_connections;
    size_t http_timeout;

    Poco::Logger * log;
};
}
