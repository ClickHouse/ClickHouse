#pragma once

#include <libnuraft/nuraft.hxx>
#include <common/logger_useful.h>

namespace DB
{

class LoggerWrapper : public nuraft::logger
{
public:
    LoggerWrapper(const std::string & name)
        : log(&Poco::Logger::get(name))
    {}

    void put_details(
        int level,
        const char * /* source_file */,
        const char * /* func_name */,
        size_t /* line_number */,
        const std::string & msg) override
    {
        LOG_IMPL(log, static_cast<DB::LogsLevel>(level), static_cast<Poco::Message::Priority>(level), msg);
    }

    void set_level(int level) override
    {
        level = std::max(6, std::min(1, level));
        log->setLevel(level);
    }

    int get_level() override
    {
        return log->getLevel();
    }

private:
    Poco::Logger * log;
};

}
