#include <Poco/Logger.h>
#include <Common/Logger.h>
#include <Common/Exception.h>
#include "Loggers/OwnPatternFormatter.h"
#include "Loggers/OwnSplitChannel.h"

#include <quill/Frontend.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}
namespace
{
std::unique_ptr<OwnPatternFormatter> formatter;
std::atomic<OwnPatternFormatter *> formatter_ptr = nullptr;
}

Logger::Logger(std::string_view name_, QuillLoggerPtr logger_)
    : name(name_)
    , logger(logger_)
{
}

Logger::Logger(std::string name_, QuillLoggerPtr logger_)
{
    name_holder = std::move(name_);
    name = name_holder;
    logger = logger_;
}

OwnPatternFormatter * Logger::getFormatter()
{
    return formatter_ptr.load(std::memory_order_relaxed);
}

void Logger::setFormatter(std::unique_ptr<OwnPatternFormatter> formatter_)
{
    formatter = std::move(formatter_);
    formatter_ptr.store(formatter.get(), std::memory_order_relaxed);
}

QuillLoggerPtr Logger::getQuillLogger()
{
    if (!logger)
        logger = quill::Frontend::get_logger("root");

    return logger;
}

DB::OwnSplitChannel & Logger::getTextLogChannel()
{
    static DB::OwnSplitChannel split_channel;
    return split_channel;
}

LoggerPtr getLogger(const char * name, const char * component_name)
{
    return getLogger(std::string_view{name}, component_name);
}

LoggerPtr getLogger(std::string_view name, const char * component_name)
{
    auto * logger = quill::Frontend::get_logger("root");
    if (component_name)
    {
        if (!logger)
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Cannot create logger for component '{}' because root logger is not initialized",
                component_name);
        logger = quill::Frontend::create_or_get_logger(component_name, logger);
    }

    return std::make_shared<Logger>(name, logger);
}

LoggerPtr getLogger(std::string name, const char * component_name)
{
    auto * logger = quill::Frontend::get_logger("root");
    if (component_name)
    {
        if (!logger)
            throw DB::Exception(
                DB::ErrorCodes::LOGICAL_ERROR,
                "Cannot create logger for component '{}' because root logger is not initialized",
                component_name);
        logger = quill::Frontend::create_or_get_logger(component_name, logger);
    }

    return std::make_shared<Logger>(std::move(name), logger);

}

PocoLoggerPtr getPocoLogger(const std::string & name)
{
    return Poco::Logger::getShared(name);
}

PocoLoggerPtr createLogger(const std::string & name, Poco::Channel * channel, Poco::Message::Priority level)
{
    return Poco::Logger::createShared(name, channel, level);
}

QuillLoggerPtr getQuillLogger(const std::string & name)
{
    auto * root = quill::Frontend::get_logger("root");

    if (!root)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Root logger is not initialized");

    return quill::Frontend::create_or_get_logger(name, root);
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
