#pragma once

#include <Core/LogsLevel.h>
#include <libnuraft/nuraft.hxx>
#include <Common/logger_useful.h>

namespace DB
{

class LoggerWrapper : public nuraft::logger
{
private:

    static inline const std::unordered_map<LogsLevel, quill::LogLevel> LEVELS =
    {
        {LogsLevel::test, quill::LogLevel::TraceL2},
        {LogsLevel::trace, quill::LogLevel::TraceL1},
        {LogsLevel::debug, quill::LogLevel::Debug},
        {LogsLevel::information, quill::LogLevel::Info},
        {LogsLevel::warning, quill::LogLevel::Warning},
        {LogsLevel::error, quill::LogLevel::Error},
        {LogsLevel::fatal, quill::LogLevel::Critical}
    };
    static inline const int LEVEL_MAX = static_cast<int>(LogsLevel::trace);
    static inline const int LEVEL_MIN = static_cast<int>(LogsLevel::none);

public:
    LoggerWrapper(const std::string & name, LogsLevel level_)
        : log(getLogger(name, "RaftInstance"))
        , level(level_)
    {
        log->getQuillLogger()->set_log_level(LEVELS.at(level));
    }

    void put_details(
        [[maybe_unused]] int level_,
        const char * /* source_file */,
        const char * /* func_name */,
        size_t /* line_number */,
        const std::string & msg) override
    {
        LogsLevel db_level = static_cast<LogsLevel>(level_);
        LOG_IMPL(log, db_level, fmt::runtime(msg));
    }

    void set_level(int level_) override
    {
        level_ = std::min(LEVEL_MAX, std::max(LEVEL_MIN, level_));
        level = static_cast<LogsLevel>(level_);
        log->getQuillLogger()->set_log_level(LEVELS.at(level));
    }

    int get_level() override
    {
        LogsLevel lvl = level;
        return static_cast<int>(lvl);
    }

private:
    LoggerPtr log;
    std::atomic<LogsLevel> level;
};

}
