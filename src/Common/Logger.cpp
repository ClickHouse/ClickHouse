#include <Poco/Logger.h>
#include <Common/Logger.h>

LoggerPtr getLogger(const std::string & name)
{
    return Poco::Logger::getShared(name);
}

LoggerPtr createLogger(const std::string & name, Poco::Channel * channel, Poco::Message::Priority level)
{
    return Poco::Logger::createShared(name, channel, level);
}

LoggerRawPtr getRawLogger(const std::string & name)
{
    return &Poco::Logger::get(name);
}

LoggerRawPtr createRawLogger(const std::string & name, Poco::Channel * channel, Poco::Message::Priority level)
{
    return &Poco::Logger::create(name, channel, level);
}

bool hasLogger(const std::string & name)
{
    return Poco::Logger::has(name);
}
