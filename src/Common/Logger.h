#pragma once

#include <base/defines.h>

#include <memory>

#include <Common/Logger_fwd.h>

#include <quill/core/Common.h>

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

struct CustomFrontendOptions
{

  static constexpr quill::QueueType queue_type = quill::QueueType::UnboundedBlocking;

  /**
   * Initial capacity of the queue. Used for UnboundedBlocking, UnboundedDropping, and
   * UnboundedUnlimited. Also serves as the capacity for BoundedBlocking and BoundedDropping.
   */
  static uint32_t initial_queue_capacity; // 128 KiB

  /**
   * Interval for retrying when using BoundedBlocking or UnboundedBlocking.
   * Applicable only when using BoundedBlocking or UnboundedBlocking.
   */
  static constexpr uint32_t blocking_queue_retry_interval_ns = 800;

  /**
   * Enables huge pages on the frontend queues to reduce TLB misses. Available only for Linux.
   */
  static constexpr bool huge_pages_enabled = false;
};

using QuillLoggerPtr =  quill::v8::LoggerImpl<CustomFrontendOptions> *;

namespace DB
{
class TextLogSink;
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

    static DB::TextLogSink & getTextLogSink();
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
