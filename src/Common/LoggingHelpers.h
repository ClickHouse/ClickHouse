#pragma once

#include <base/types.h>
#include <Common/Logger.h>

#include <mutex>
#include <optional>
#include <unordered_map>

#include <Poco/Message.h>

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
    LogFrequencyLimiterImpl(LoggerPtr logger_, time_t min_interval_s_)
        : logger(std::move(logger_))
        , min_interval_s(min_interval_s_)
    {
    }

    LogFrequencyLimiterImpl * operator->() { return this; }
    bool is(Poco::Message::Priority priority) { return logger->is(priority); }
    LogFrequencyLimiterImpl * getChannel() { return this; }
    const String & name() const { return logger->name(); }

    void log(Poco::Message && msg);

    /// Clears messages that were logged last time more than too_old_threshold_s seconds ago
    static void cleanup(time_t too_old_threshold_s = 600);

    LoggerPtr getLogger() { return logger; }
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

    LogSeriesLimiter * operator->() { return this; }
    bool is(Poco::Message::Priority priority) { return logger->is(priority); }
    const String & name() const { return logger->name(); }

    LogSeriesLimiter * getChannel();
    void log(Poco::Message && message);

    LoggerPtr getLogger() { return logger; }
};

/// This wrapper is useful to save formatted message into a String before sending it to a logger
class LogToStrImpl
{
    String & out_str;
    LoggerPtr logger;
    std::optional<LogFrequencyLimiterImpl> maybe_nested;
    bool propagate_to_actual_log = true;

public:
    LogToStrImpl(String & out_str_, LoggerPtr logger_)
        : out_str(out_str_)
        , logger(std::move(logger_))
    {
    }
    LogToStrImpl(String & out_str_, LogFrequencyLimiterImpl && maybe_nested_)
        : out_str(out_str_)
        , logger(maybe_nested_->getLogger())
        , maybe_nested(std::move(maybe_nested_))
    {
    }

    LogToStrImpl * operator->() { return this; }
    bool is(Poco::Message::Priority priority)
    {
        propagate_to_actual_log &= logger->is(priority);
        return true;
    }
    LogToStrImpl * getChannel() { return this; }
    const String & name() const { return logger->name(); }

    void log(Poco::Message && message)
    {
        out_str = message.getText();
        if (!propagate_to_actual_log)
            return;
        if (maybe_nested)
            maybe_nested->log(std::move(message));
        else if (auto * channel = logger->getChannel())
            channel->log(std::move(message));
    }
};
