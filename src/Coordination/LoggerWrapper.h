#pragma once

#include <Core/LogsLevel.h>
#include <libnuraft/nuraft.hxx>
#include <Common/logger_useful.h>

namespace DB
{

class LoggerWrapper : public nuraft::logger
{
private:

    static inline const std::unordered_map<LogsLevel, Poco::Message::Priority> LEVELS =
    {
        {LogsLevel::test, Poco::Message::Priority::PRIO_TEST},
        {LogsLevel::trace, Poco::Message::Priority::PRIO_TRACE},
        {LogsLevel::debug, Poco::Message::Priority::PRIO_DEBUG},
        {LogsLevel::information, Poco::Message::PRIO_INFORMATION},
        {LogsLevel::warning, Poco::Message::PRIO_WARNING},
        {LogsLevel::error, Poco::Message::PRIO_ERROR},
        {LogsLevel::fatal, Poco::Message::PRIO_FATAL}
    };
    static inline const int LEVEL_MAX = static_cast<int>(LogsLevel::trace);
    static inline const int LEVEL_MIN = static_cast<int>(LogsLevel::none);

public:
    LoggerWrapper(const std::string & name, LogsLevel level_)
        : log(getLogger(name))
        , level(level_)
    {
        log->setLevel(static_cast<int>(LEVELS.at(level)));
    }

    void put_details(
        int level_,
        const char * /* source_file */,
        const char * /* func_name */,
        size_t /* line_number */,
        const std::string & msg) override
    {
        LogsLevel db_level = static_cast<LogsLevel>(level_);
        LOG_IMPL(log, db_level, LEVELS.at(db_level), fmt::runtime(msg));
    }

    void set_level(int level_) override
    {
        level_ = std::min(LEVEL_MAX, std::max(LEVEL_MIN, level_));
        level = static_cast<LogsLevel>(level_);
        log->setLevel(static_cast<int>(LEVELS.at(level)));
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
