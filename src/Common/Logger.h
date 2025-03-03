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

    QuillLoggerPtr getLogger();

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


LoggerPtr getLogger(const char * name);
LoggerPtr getLogger(std::string_view name);
LoggerPtr getLogger(std::string name);

template <size_t n>
LoggerPtr getLogger(const char (&name)[n])
{
    return getLogger(std::string_view{name, n});
}

QuillLoggerPtr getQuillLogger(const std::string & name);

PocoLoggerPtr getPocoLogger(const std::string & name);

/** Create Logger with specified name, channel and logging level.
  * If Logger already exists, throws exception.
  * Logger is destroyed, when last shared ptr that refers to Logger with specified name is destroyed.
  */
PocoLoggerPtr createLogger(const std::string & name, Poco::Channel * channel, Poco::Message::Priority level = Poco::Message::PRIO_INFORMATION);

/** Returns true, if currently Logger with specified name is created.
  * Otherwise, returns false.
  */
bool hasLogger(const std::string & name);

void disableLogging();

bool isLoggingEnabled();
