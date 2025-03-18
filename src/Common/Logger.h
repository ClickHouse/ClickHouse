#pragma once

#include <base/defines.h>

#include <memory>

#include <Common/Logger_fwd.h>
#include <Common/QuillLogger_fwd.h>
#include <quill/core/LogLevel.h>

#include <Poco/Logger.h>
#include <Poco/Message.h>


namespace Poco
{
class Channel;
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;
}

namespace DB
{
class TextLogSink;
}

using PocoLoggerPtr = std::shared_ptr<Poco::Logger>;

class OwnPatternFormatter;

class Logger
{
public:
    Logger(std::string_view name_, DB::QuillLoggerPtr logger_);
    Logger(std::string name_, DB::QuillLoggerPtr logger_);

    DB::QuillLoggerPtr getQuillLogger();

    void setLogLevel(quill::LogLevel level);
    void flushLogs();

    std::string_view getName()
    {
        return name;
    }

    static DB::TextLogSink & getTextLogSink();
    static OwnPatternFormatter * getFormatter();
    static void setFormatter(std::unique_ptr<OwnPatternFormatter> formatter);
private:
    std::string_view name;
    std::string name_holder;
    DB::QuillLoggerPtr logger;
};

using LoggerPtr = std::shared_ptr<Logger>;
using LoggerRawPtr = Logger *;

enum class LoggerComponent
{
    Root = 0,
    RaftInstance
};

LoggerPtr getLogger(LoggerComponent component);
LoggerPtr getLogger(const char * name, LoggerComponent component = LoggerComponent::Root);
LoggerPtr getLogger(std::string_view name, LoggerComponent component = LoggerComponent::Root);
LoggerPtr getLogger(std::string name, LoggerComponent component = LoggerComponent::Root);

template <size_t n>
LoggerPtr getLogger(const char (&name)[n])
{
    return getLogger(std::string_view{name, n});
}

DB::QuillLoggerPtr getQuillLogger(LoggerComponent component);
DB::QuillLoggerPtr getQuillLogger(const std::string & name);

LoggerPtr createLogger(const std::string & name, std::vector<std::shared_ptr<quill::Sink>> sinks);
LoggerPtr createRootLogger(std::vector<std::shared_ptr<quill::Sink>> sinks);

LoggerPtr getRootLogger();

void disableLogging();

bool isLoggingEnabled();
