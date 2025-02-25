#include <Poco/Logger.h>
#include <Common/Logger.h>

#include <quill/Frontend.h>


LoggerPtr getLogger(const std::string & name)
{
    auto * root = quill::Frontend::get_logger("root");

    if (!root)
        return nullptr;

    return quill::Frontend::create_or_get_logger(name, root);
}

PocoLoggerPtr getPocoLogger(const std::string & name)
{
    return Poco::Logger::getShared(name);
}

PocoLoggerPtr createLogger(const std::string & name, Poco::Channel * channel, Poco::Message::Priority level)
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

static constinit std::atomic<bool> allow_logging{true};

bool isLoggingEnabled()
{
    return allow_logging;
}

void disableLogging()
{
    allow_logging = false;
}
