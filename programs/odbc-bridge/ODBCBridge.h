#pragma once

#include <Interpreters/Context.h>
#include <Poco/Logger.h>
#include <daemon/BaseDaemon.h>

namespace DB
{
/** Class represents clickhouse-odbc-bridge server, which listen
  * incoming HTTP POST and GET requests on specified port and host.
  * Has two handlers '/' for all incoming POST requests to ODBC driver
  * and /ping for GET request about service status
  */
class ODBCBridge : public BaseDaemon
{
public:
    void defineOptions(Poco::Util::OptionSet & options) override;

protected:
    void initialize(Application & self) override;

    void uninitialize() override;

    int main(const std::vector<std::string> & args) override;

private:
    void handleHelp(const std::string &, const std::string &);

    bool is_help;
    std::string hostname;
    size_t port;
    size_t http_timeout;
    std::string log_level;
    size_t max_server_connections;
    size_t keep_alive_timeout;

    Poco::Logger * log;
};
}
