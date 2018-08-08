#pragma once

#include <daemon/BaseDaemon.h>
#include <Poco/Logger.h>
#include <Interpreters/Context.h>

namespace DB
{
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
    UInt16 port;
    size_t http_timeout;
    std::string log_level;
    size_t max_server_connections;
    size_t keep_alive_timeout;

    Poco::Logger * log;

    std::shared_ptr<Context> context; /// need for settings only
};

}
