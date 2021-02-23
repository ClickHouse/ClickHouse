#pragma once

#include <libnuraft/nuraft.hxx> // Y_IGNORE
#include <common/logger_useful.h>
#include <Core/SettingsEnums.h>

namespace DB
{

class LoggerWrapper : public nuraft::logger
{
public:
    LoggerWrapper(const std::string & name, LogsLevel level_)
        : log(&Poco::Logger::get(name))
        , level(static_cast<int>(level_))
    {
        log->setLevel(level);
    }

    void put_details(
        int level_,
        const char * /* source_file */,
        const char * /* func_name */,
        size_t /* line_number */,
        const std::string & msg) override
    {
        LOG_IMPL(log, static_cast<DB::LogsLevel>(level_), static_cast<Poco::Message::Priority>(level_), msg);
    }

    void set_level(int level_) override
    {
        level_ = std::min(6, std::max(1, level_));
        log->setLevel(level_);
        level = level_;
    }

    int get_level() override
    {
        return level;
    }

private:
    Poco::Logger * log;
    std::atomic<int> level;
};

}
