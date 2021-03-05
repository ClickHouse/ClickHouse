#pragma once

#include <Interpreters/Context.h>
#include <daemon/BaseDaemon.h>
#include <common/logger_useful.h>


namespace DB
{

class LibraryBridge : public BaseDaemon
{

public:
    void defineOptions(Poco::Util::OptionSet & options) override;

protected:
    void initialize(Application & self) override;

    void uninitialize() override;

    int main(const std::vector<std::string> & args) override;

private:
    void handleHelp(const std::string &, const std::string &);

    std::string hostname;
    size_t port;

    size_t http_timeout;
    std::string log_level;
    size_t max_server_connections;
    size_t keep_alive_timeout;

    Poco::Logger * log;
};

}

