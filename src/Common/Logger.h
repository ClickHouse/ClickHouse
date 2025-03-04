#pragma once

#include <base/defines.h>

#include <memory>

#include <Common/Logger_fwd.h>

#include <Poco/Logger.h>
#include <Poco/Message.h>


namespace Poco
{
class Channel;
class Logger;
using LoggerPtr = std::shared_ptr<Logger>;
}

namespace quill
{
inline namespace v8
{
class FrontendOptions;
class Sink;

template <typename TFrontendOptions>
class LoggerImpl;
}
}

using QuillLoggerPtr =  quill::v8::LoggerImpl<quill::v8::FrontendOptions> *;

namespace DB
{
class OwnSplitChannel;
}

using PocoLoggerPtr = std::shared_ptr<Poco::Logger>;

class OwnPatternFormatter;

class Logger
{
public:
    Logger(std::string_view name_, QuillLoggerPtr logger_);
    Logger(std::string name_, QuillLoggerPtr logger_);

    QuillLoggerPtr getQuillLogger();

    std::string_view getName()
    {
        return name;
    }

    static DB::OwnSplitChannel & getTextLogChannel();
    static OwnPatternFormatter * getFormatter();
    static void setFormatter(std::unique_ptr<OwnPatternFormatter> formatter);
private:
    std::string_view name;
    std::string name_holder;
    QuillLoggerPtr logger;
};

using LoggerPtr = std::shared_ptr<Logger>;
using LoggerRawPtr = Logger *;


LoggerPtr getLogger(const char * name, const char * component_name = nullptr);
LoggerPtr getLogger(std::string_view name, const char * component_name = nullptr);
LoggerPtr getLogger(std::string name, const char * component_name = nullptr);

template <size_t n>
LoggerPtr getLogger(const char (&name)[n])
{
    return getLogger(std::string_view{name, n});
}

QuillLoggerPtr getQuillLogger(const std::string & name);

LoggerPtr createLogger(const std::string & name, std::vector<std::shared_ptr<quill::Sink>> sinks);

void disableLogging();

bool isLoggingEnabled();
