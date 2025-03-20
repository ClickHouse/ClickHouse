#include <atomic>
#include <Loggers/OwnPatternFormatter.h>
#include <Loggers/TextLogSink.h>
#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/Exception.h>
#include <Common/Logger.h>
#include <Common/LoggingFormatStringHelpers.h>
#include <Common/QuillLogger.h>
#include <Common/SipHash.h>
#include <Common/thread_local_rng.h>

#include <quill/Frontend.h>
#include <quill/sinks/Sink.h>

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

const std::string root_logger_name = "Application";
std::atomic<DB::QuillLoggerPtr> root_logger = nullptr;

std::atomic<bool> sync_logging = false;

DB::QuillLoggerPtr createQuillLogger(const std::string & name, std::vector<std::shared_ptr<quill::Sink>> sinks)
{
    return DB::QuillFrontend::create_or_get_logger(
        name,
        sinks,
        quill::PatternFormatterOptions{
            "%(message)",
            /*timestamp_pattern=*/"",
            /*timestamp_timezone=*/quill::Timezone::LocalTime,
            /*add_metadata_to_multi_line_logs=*/false});
}

}

Logger::Logger(std::string_view name_, DB::QuillLoggerPtr logger_)
    : name(name_)
    , logger(logger_)
{
}

Logger::Logger(std::string name_, DB::QuillLoggerPtr logger_)
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

void Logger::enableSyncLogging()
{
    sync_logging.store(true, std::memory_order_relaxed);
}

bool Logger::shouldSyncLog()
{
    return sync_logging.load(std::memory_order_relaxed);
}

DB::QuillLoggerPtr Logger::getQuillLogger()
{
    if (!logger)
        logger = root_logger.load(std::memory_order_relaxed);

    return logger;
}

void Logger::setLogLevel(const std::string & level)
{
    setLogLevel(DB::parseQuillLogLevel(level));
}

void Logger::setLogLevel(quill::LogLevel level)
{
    auto * quill_logger = getQuillLogger();
    if (!quill_logger)
        return;

    quill_logger->set_log_level(level);
}

void Logger::flushLogs()
{
    auto * quill_logger = getQuillLogger();
    if (!quill_logger)
        return;

    quill_logger->flush_log();
}

DB::TextLogSink & Logger::getTextLogSink()
{
    static DB::TextLogSink text_log_sink;
    return text_log_sink;
}

namespace
{

const std::string & componentToString(LoggerComponent component)
{
    using enum LoggerComponent;
    switch (component)
    {
        case Root: {
            return root_logger_name;
        }
        case RaftInstance: {
            static const std::string component_string{"RaftInstance"};
            return component_string;
        }
        case ZooKeeperClient: {
            static const std::string component_string{"ZooKeeperClient"};
            return component_string;
        }
    }
};
}

LoggerPtr getLogger(const char * name, LoggerComponent component)
{
    return getLogger(std::string_view{name}, component);
}

LoggerPtr getLogger(std::string_view name, LoggerComponent component)
{
    return std::make_shared<Logger>(name, getQuillLogger(component));
}

LoggerPtr getLogger(std::string name, LoggerComponent component)
{
    return std::make_shared<Logger>(std::move(name), getQuillLogger(component));
}

LoggerPtr getLogger(LoggerComponent component)
{
    return std::make_shared<Logger>(std::string_view{componentToString(component)}, getQuillLogger(component));
}

LoggerPtr createLogger(const std::string & name, std::vector<std::shared_ptr<quill::Sink>> sinks)
{
    return std::make_shared<Logger>(name, createQuillLogger(name, std::move(sinks)));
}

LoggerPtr createRootLogger(std::vector<std::shared_ptr<quill::Sink>> sinks)
{
    auto logger = std::make_shared<Logger>(std::string_view{root_logger_name}, createQuillLogger(root_logger_name, std::move(sinks)));
    root_logger.store(logger->getQuillLogger(), std::memory_order_relaxed);
    return logger;
}

LoggerPtr getRootLogger()
{
    return getLogger(std::string_view{root_logger_name});
}

DB::QuillLoggerPtr getQuillLogger(LoggerComponent component)
{
    if (component == LoggerComponent::Root)
        return root_logger.load(std::memory_order_relaxed);

    return getQuillLogger(componentToString(component));
}

DB::QuillLoggerPtr getQuillLogger(const std::string & name)
{
    auto * root = root_logger.load(std::memory_order_relaxed);

    if (!root)
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR, "Cannot create logger for component '{}' because root logger is not initialized", name);

    return DB::QuillFrontend::create_or_get_logger(name, root);
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


std::unordered_map<UInt64, std::pair<time_t, size_t>> LogFrequencyLimiterImpl::logged_messages;
time_t LogFrequencyLimiterImpl::last_cleanup = 0;
std::mutex LogFrequencyLimiterImpl::mutex;

bool LogFrequencyLimiterImpl::shouldLogMessage(Poco::Message & message)
{
    std::string_view pattern = message.getFormatString();
    if (pattern.empty())
    {
        /// Do not filter messages without a format string
        return true;
    }

    SipHash hash;
    hash.update(logger->getName());
    /// Format strings are compile-time constants, so they are uniquely identified by pointer and size
    hash.update(reinterpret_cast<uintptr_t>(pattern.data()));
    hash.update(pattern.size());

    time_t now = time(nullptr);
    size_t skipped_similar_messages = 0;
    bool need_cleanup;
    bool need_log;

    {
        std::lock_guard lock(mutex);
        need_cleanup = last_cleanup + 300 <= now;
        auto & info = logged_messages[hash.get64()];
        need_log = info.first + min_interval_s <= now;
        if (need_log)
        {
            skipped_similar_messages = info.second;
            info.first = now;
            info.second = 0;
        }
        else
        {
            ++info.second;
        }
    }

    /// We don't need all threads to do cleanup, just randomize
    if (need_cleanup && thread_local_rng() % 100 == 0)
        cleanup();

    /// The message it too frequent, skip it for now
    /// NOTE It's not optimal because we format the message first and only then check if we need to actually write it, see LOG_IMPL macro
    if (!need_log)
        return false;

    if (skipped_similar_messages)
        message.appendText(fmt::format(" (skipped {} similar messages)", skipped_similar_messages));

    return true;
}

void LogFrequencyLimiterImpl::cleanup(time_t too_old_threshold_s)
{
    time_t now = time(nullptr);
    time_t old = now - too_old_threshold_s;
    std::lock_guard lock(mutex);
    std::erase_if(logged_messages, [old](const auto & elem) { return elem.second.first < old; });
    last_cleanup = now;
}


std::mutex LogSeriesLimiter::mutex;
time_t LogSeriesLimiter::last_cleanup = 0;

LogSeriesLimiter::LogSeriesLimiter(LoggerPtr logger_, size_t allowed_count_, time_t interval_s_)
    : logger(std::move(logger_))
{
    if (allowed_count_ == 0)
    {
        accepted = false;
        return;
    }

    if (interval_s_ == 0)
    {
        accepted = true;
        return;
    }

    time_t now = time(nullptr);
    UInt128 name_hash = sipHash128(logger->getName().data(), logger->getName().size());

    std::lock_guard lock(mutex);

    if (last_cleanup == 0)
        last_cleanup = now;

    auto & series_records = getSeriesRecords();

    static const time_t cleanup_delay_s = 600;
    if (last_cleanup + cleanup_delay_s >= now)
    {
        time_t old = now - cleanup_delay_s;
        std::erase_if(series_records, [old](const auto & elem) { return get<0>(elem.second) < old; });
        last_cleanup = now;
    }

    auto register_as_first = [&] () TSA_REQUIRES(mutex)
    {
        assert(allowed_count_ > 0);
        accepted = true;
        series_records[name_hash] = std::make_tuple(now, 1, 1);
    };

    if (!series_records.contains(name_hash))
    {
        register_as_first();
        return;
    }

    auto & [last_time, accepted_count, total_count] = series_records[name_hash];
    if (last_time + interval_s_ <= now)
    {
        debug_message = fmt::format(
            " (LogSeriesLimiter: on interval from {} to {} accepted series {} / {} for the logger {})",
            DateLUT::instance().timeToString(last_time),
            DateLUT::instance().timeToString(now),
            accepted_count,
            total_count,
            logger->getName());

        register_as_first();
        return;
    }

    if (accepted_count < allowed_count_)
    {
        accepted = true;
        ++accepted_count;
    }
    ++total_count;
}

bool LogSeriesLimiter::shouldLogMessage(Poco::Message & message)
{
    std::string_view pattern = message.getFormatString();
    if (pattern.empty())
    {
        /// Do not filter messages without a format string
        return true;
    }

    if (!accepted)
        return false;

    if (!debug_message.empty())
    {
        message.appendText(debug_message);
        debug_message.clear();
    }

    return true;
}

bool LogToStrImpl::shouldLogMessage(Poco::Message & message)
{
    out_str = message.getText();
    if (!propagate_to_actual_log)
        return false;

    return true;
}
