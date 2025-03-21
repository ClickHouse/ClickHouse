#pragma once

#include <base/types.h>
#include <base/defines.h>

#include <memory>
#include <mutex>
#include <unordered_map>

#include <Common/Logger_fwd.h>
#include <Common/QuillLogger_fwd.h>
#include <quill/core/LogLevel.h>

#include <Poco/Message.h>

namespace DB
{
class TextLogSink;
}

class OwnPatternFormatter;

class Logger
{
public:
    Logger(std::string_view name_, DB::QuillLoggerPtr logger_);
    Logger(std::string name_, DB::QuillLoggerPtr logger_);

    DB::QuillLoggerPtr getQuillLogger();

    void setLogLevel(const std::string & level);
    void setLogLevel(quill::LogLevel level);
    void flushLogs();

    std::string_view getName()
    {
        return name;
    }

    static DB::TextLogSink & getTextLogSink();
    static OwnPatternFormatter * getFormatter();
    static void setFormatter(std::unique_ptr<OwnPatternFormatter> formatter);

    static void enableSyncLogging();
    static bool shouldSyncLog();
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
    RaftInstance,
    ZooKeeperClient,
    Max
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


/// This wrapper helps to avoid too frequent and noisy log messages.
/// For each pair (logger_name, format_string) it remembers when such a message was logged the last time.
/// The message will not be logged again if less than min_interval_s seconds passed since the previously logged message.
class LogFrequencyLimiterImpl
{
    /// Hash(logger_name, format_string) -> (last_logged_time_s, skipped_messages_count)
    static std::unordered_map<UInt64, std::pair<time_t, size_t>> logged_messages;
    static time_t last_cleanup;
    static std::mutex mutex;

    LoggerPtr logger;
    time_t min_interval_s;
public:
    LogFrequencyLimiterImpl(LoggerPtr logger_, time_t min_interval_s_) : logger(std::move(logger_)), min_interval_s(min_interval_s_) {}

    std::string_view getName() const { return logger->getName(); }

    bool shouldLogMessage(Poco::Message & message);

    /// Clears messages that were logged last time more than too_old_threshold_s seconds ago
    static void cleanup(time_t too_old_threshold_s = 600);

    LoggerPtr getLogger() const { return logger; }
};

/// This wrapper helps to avoid too noisy log messages from similar objects.
/// Once an instance of LogSeriesLimiter type is created the decision is done
/// All followed messages which use this instance are either printed or muted altogether.
/// LogSeriesLimiter differs from LogFrequencyLimiterImpl in a way that
/// LogSeriesLimiter is useful for accept or mute series of logs when LogFrequencyLimiterImpl works for each line independently.
class LogSeriesLimiter
{
    static std::mutex mutex;
    static time_t last_cleanup;

    /// Hash(logger_name) -> (last_logged_time_s, accepted, muted)
    using SeriesRecords = std::unordered_map<UInt64, std::tuple<time_t, size_t, size_t>>;

    static SeriesRecords & getSeriesRecords() TSA_REQUIRES(mutex)
    {
        static SeriesRecords records;
        return records;
    }

    LoggerPtr logger = nullptr;
    bool accepted = false;
    String debug_message;
public:
    LogSeriesLimiter(LoggerPtr logger_, size_t allowed_count_, time_t interval_s_);

    bool shouldLogMessage(Poco::Message & message);
    std::string_view getName() const { return logger->getName(); }

    LoggerPtr getLogger() const { return logger; }
};

/// This wrapper is useful to save formatted message into a String before sending it to a logger
class LogToStrImpl
{
    String & out_str;
    LoggerPtr logger;
    std::optional<LogFrequencyLimiterImpl> maybe_nested;
    bool propagate_to_actual_log = true;
public:
    LogToStrImpl(String & out_str_, LoggerPtr logger_) : out_str(out_str_), logger(std::move(logger_)) {}
    LogToStrImpl(String & out_str_, LogFrequencyLimiterImpl && maybe_nested_)
        : out_str(out_str_), logger(maybe_nested_.getLogger()), maybe_nested(std::move(maybe_nested_)) {}

    std::string_view getName() const { return logger->getName(); }

    LoggerPtr getLogger() const { return logger; }

    bool shouldLogMessage(Poco::Message & message);
};
