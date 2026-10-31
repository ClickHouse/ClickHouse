#include <Storages/Pulsar/PulsarLogger.h>

#include <Common/StringUtils.h>
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

/// The client library may print raw pointers (`0x7f...`), and the messages end up in `system.text_log`
/// and in the client log queue. Mask every hexadecimal literal so runtime addresses are never disclosed.
std::string redactAddresses(const std::string & message)
{
    std::string result;
    result.reserve(message.size());

    size_t pos = 0;
    while (pos < message.size())
    {
        if (message[pos] == '0' && pos + 2 < message.size() && (message[pos + 1] == 'x' || message[pos + 1] == 'X')
            && isHexDigit(message[pos + 2]) && (pos == 0 || !isWordCharASCII(message[pos - 1])))
        {
            pos += 2;
            while (pos < message.size() && isHexDigit(message[pos]))
                ++pos;
            result += "0x<hidden>";
        }
        else
        {
            result += message[pos];
            ++pos;
        }
    }

    return result;
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
            logger->log(Poco::Message(logger->name(), fmt::format("{}:{} {}", file_name, line, redactAddresses(message)), priority));
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
