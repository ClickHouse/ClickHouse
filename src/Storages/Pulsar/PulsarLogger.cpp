#include <Storages/Pulsar/PulsarLogger.h>

#include <Common/logger_useful.h>

namespace DB
{

namespace
{

Poco::Message::Priority toPocoPriority(pulsar::Logger::Level level)
{
    switch (level)
    {
        case pulsar::Logger::LEVEL_DEBUG:
            return Poco::Message::PRIO_DEBUG;
        case pulsar::Logger::LEVEL_INFO:
            return Poco::Message::PRIO_INFORMATION;
        case pulsar::Logger::LEVEL_WARN:
            return Poco::Message::PRIO_WARNING;
        case pulsar::Logger::LEVEL_ERROR:
            return Poco::Message::PRIO_ERROR;
    }
    return Poco::Message::PRIO_ERROR;
}

class PulsarLogger : public pulsar::Logger
{
public:
    PulsarLogger(LoggerPtr log_, std::string file_name_)
        : logger(std::move(log_))
        , file_name(std::move(file_name_))
    {
    }

    bool isEnabled(Level level) override { return logger->is(toPocoPriority(level)); }

    void log(Level level, int line, const std::string & message) override
    {
        auto priority = toPocoPriority(level);
        if (logger->is(priority))
            logger->log(Poco::Message(logger->name(), fmt::format("{}:{} {}", file_name, line, message), priority));
    }

private:
    LoggerPtr logger;
    std::string file_name;
};

}

pulsar::Logger * PulsarLoggerFactory::getLogger(const std::string & file_name)
{
    return new PulsarLogger(::getLogger("PulsarClient"), file_name);
}

}
