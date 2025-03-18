#include <atomic>
#include <Common/QuillLogger.h>
#include <Common/Logger.h>
#include <Common/Exception.h>
#include <Loggers/OwnPatternFormatter.h>
#include <Loggers/TextLogSink.h>

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

DB::QuillLoggerPtr Logger::getQuillLogger()
{
    if (!logger)
        logger = root_logger.load(std::memory_order_relaxed);

    return logger;
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
