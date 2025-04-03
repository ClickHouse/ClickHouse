#include "Loggers.h"

#include <Loggers/OwnPatternFormatter.h>
#include <Loggers/OwnJSONPatternFormatter.h>
#include <Loggers/TextLogSink.h>

#include <iostream>
#include <memory>
#include <sstream>

#include <IO/ReadHelpers.h>
#include <quill/core/LogLevel.h>
#include <quill/sinks/Sink.h>
#include <Common/Logger.h>
#include <Common/QuillLogger.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/LockMemoryExceptionInThread.h>

#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Poco/Net/RemoteSyslogChannel.h>
#include <Poco/SyslogChannel.h>
#include <Poco/Util/AbstractConfiguration.h>

#ifndef WITHOUT_TEXT_LOG
    #include <Interpreters/TextLog.h>
#endif

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{
    class SensitiveDataMasker;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

}

namespace
{

std::string createDirectory(const std::string & file)
{
    auto path = fs::path(file).parent_path();
    if (path.empty())
        return "";
    fs::create_directories(path);
    return path;
}

std::string renderFileNameTemplate(time_t now, const std::string & file_path)
{
    fs::path path{file_path};
    std::tm buf;
    localtime_r(&now, &buf); /// NOLINT(cert-err33-c)
    std::ostringstream ss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    ss << std::put_time(&buf, path.filename().c_str());
    return path.replace_filename(ss.str());
}

size_t getLogFileMaxSize(const std::string & max_size_str)
{
    if (max_size_str == "never")
        return 0;

    return DB::parseWithSizeSuffix<size_t>(max_size_str);
}

}

/// NOLINTBEGIN(readability-static-accessed-through-instance)

void Loggers::buildLoggers(Poco::Util::AbstractConfiguration & config, const std::string & /*cmd_name*/, bool allow_console_only)
{
    if (initialized)
        return;

    /// if we are remapping executable, we can start another thread only AFTER we are done
    /// with remapping
    if (!config.getBool("remap_executable", false))
        DB::startQuillBackend(&config);

    DB::QuillFrontendOptions::initial_queue_capacity = config.getUInt64("logger.initial_queue_capacity", 4 * 1024);
    DB::QuillFrontendOptions::unbounded_queue_max_capacity = config.getUInt64("logger.queue_max_capacity", 2 * 1024 * 1024);

    std::vector<std::shared_ptr<quill::Sink>> sinks;

    auto current_logger = config.getString("logger", "");

    config_logger = current_logger;

    bool is_daemon = config.getBool("application.runAsDaemon", false);

    auto log_level_string = config.getString("logger.level", "trace");

    /// different channels (log, console, syslog) may have different loglevels configured
    /// The maximum (the most verbose) of those will be used as default for Poco loggers
    quill::LogLevel min_log_level = quill::LogLevel::None;

    time_t now = std::time({});

    bool use_json_format = config.getString("logger.formatting.type", "") == "json";
    if (use_json_format)
        Logger::setFormatter(std::make_unique<OwnJSONPatternFormatter>(config));
    else
        Logger::setFormatter(std::make_unique<OwnPatternFormatter>());

    const auto log_path_prop = config.getString("logger.log", "");
    if (!log_path_prop.empty() && !allow_console_only)
    {
        const auto log_path = renderFileNameTemplate(now, log_path_prop);
        createDirectory(log_path);

        std::cerr << "Logging " << log_level_string << " to " << log_path << std::endl;

        auto log_level = DB::parseQuillLogLevel(log_level_string);

        min_log_level = std::min(log_level, min_log_level);

        DB::RotatingSinkConfiguration file_config;
        file_config.compress = config.getBool("logger.compress", true);
        file_config.max_backup_files = config.getUInt64("logger.count", 1);
        file_config.max_file_size = getLogFileMaxSize(config.getString("logger.size", "100M"));
        file_config.rotate_on_open = config.getBool("logger.rotateOnOpen", false);

        auto & sink
            = sinks.emplace_back(DB::QuillFrontend::create_or_get_sink<DB::RotatingFileSink>(fs::weakly_canonical(log_path), file_config));
        log_file = std::static_pointer_cast<DB::RotatingFileSink>(sink);
        log_file->set_log_level_filter(log_level);
    }

    const auto errorlog_path_prop = config.getString("logger.errorlog", "");
    if (!errorlog_path_prop.empty() && !allow_console_only)
    {
        const auto errorlog_path = renderFileNameTemplate(now, errorlog_path_prop);
        createDirectory(errorlog_path);

        // NOTE: we don't use notice & critical in the code, so in practice error log collects fatal & error & warning.
        // (!) Warnings are important, they require attention and should never be silenced / ignored.
        auto errorlog_level = DB::parseQuillLogLevel(config.getString("logger.errorlog_level", "notice"));
        min_log_level = std::min(errorlog_level, min_log_level);

        std::string ext;
        if (config.getRawString("logger.stream_compress", "false") == "true")
            ext = ".lz4";

        std::cerr << "Logging errors to " << errorlog_path << ext << std::endl;

        DB::RotatingSinkConfiguration file_config;
        file_config.compress = config.getBool("logger.compress", true);
        file_config.max_backup_files = config.getUInt64("logger.count", 1);
        file_config.max_file_size = getLogFileMaxSize(config.getString("logger.size", "100M")); /// TODO: support readable size like 100M

        auto & sink = sinks.emplace_back(DB::QuillFrontend::create_or_get_sink<DB::RotatingFileSink>(fs::weakly_canonical(errorlog_path), file_config));
        sink->set_log_level_filter(errorlog_level);
        error_log_file = std::static_pointer_cast<DB::RotatingFileSink>(sink);
    }

    bool should_log_to_console = isatty(STDIN_FILENO) || isatty(STDERR_FILENO);
    bool color_logs_by_default = isatty(STDERR_FILENO);

    if (config.getBool("logger.console", false)
        || (!config.hasProperty("logger.console") && !is_daemon && should_log_to_console))
    {
        bool color_enabled = !use_json_format && config.getBool("logger.color_terminal", color_logs_by_default);

        auto console_log_level_string = config.getString("logger.console_log_level", log_level_string);
        auto console_log_level = DB::parseQuillLogLevel(console_log_level_string);
        min_log_level = std::min(console_log_level, min_log_level);
        auto & sink = sinks.emplace_back(DB::QuillFrontend::create_or_get_sink<DB::ConsoleSink>("ConsoleSink", DB::ConsoleSink::Stream::STDERR, color_enabled));
        sink->set_log_level_filter(console_log_level);
        console_sink = std::static_pointer_cast<DB::ConsoleSink>(sink);
    }

    auto logger = createRootLogger(sinks);
    logger->setLogLevel(min_log_level);

    if (allow_console_only)
        return;

#ifndef WITHOUT_TEXT_LOG
    if (allowTextLog() && config.has("text_log"))
    {
        String text_log_level_str = config.getString("text_log.level", "trace");
        int text_log_level = Poco::Logger::parseLevel(text_log_level_str);

        DB::SystemLogQueueSettings log_settings;
        log_settings.flush_interval_milliseconds = config.getUInt64("text_log.flush_interval_milliseconds",
                                                                    DB::TextLog::getDefaultFlushIntervalMilliseconds());

        log_settings.max_size_rows = config.getUInt64("text_log.max_size_rows",
                                                      DB::TextLog::getDefaultMaxSize());

        if (log_settings.max_size_rows< 1)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "text_log.max_size_rows {} should be 1 at least",
                                log_settings.max_size_rows);

        log_settings.reserved_size_rows = config.getUInt64("text_log.reserved_size_rows", DB::TextLog::getDefaultReservedSize());

        if (log_settings.max_size_rows < log_settings.reserved_size_rows)
        {
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS,
                                "text_log.max_size {0} should be greater or equal to text_log.reserved_size_rows {1}",
                                log_settings.max_size_rows,
                                log_settings.reserved_size_rows);
        }

        log_settings.buffer_size_rows_flush_threshold = config.getUInt64("text_log.buffer_size_rows_flush_threshold",
                                                                         log_settings.max_size_rows / 2);

        log_settings.notify_flush_on_crash = config.getBool("text_log.flush_on_crash",
                                                            DB::TextLog::shouldNotifyFlushOnCrash());

        log_settings.turn_off_logger = DB::TextLog::shouldTurnOffLogger();

        log_settings.database = config.getString("text_log.database", "system");
        log_settings.table = config.getString("text_log.table", "text_log");

        Logger::getTextLogSink().addTextLog(DB::TextLog::getLogQueue(log_settings), text_log_level);
    }
#endif

    initialized = true;
}

void Loggers::updateLevels(Poco::Util::AbstractConfiguration & config)
{
    quill::LogLevel min_log_level = quill::LogLevel::None;

    const auto log_level_string = config.getString("logger.level", "trace");

    if (log_file)
    {
        auto level = DB::parseQuillLogLevel(log_level_string);
        min_log_level = std::min(level, min_log_level);
        log_file->set_log_level_filter(level);
    }

    // Set level to console
    bool is_daemon = config.getBool("application.runAsDaemon", false);
    bool should_log_to_console = isatty(STDIN_FILENO) || isatty(STDERR_FILENO);
    if (console_sink && (config.getBool("logger.console", false)
        || (!config.hasProperty("logger.console") && !is_daemon && should_log_to_console)))
    {
        auto console_log_level_string = config.getString("logger.console_log_level", log_level_string);
        auto console_log_level = DB::parseQuillLogLevel(console_log_level_string);
        min_log_level = std::min(console_log_level, min_log_level);
        console_sink->set_log_level_filter(console_log_level);
    }
    else if (console_sink)
        console_sink->set_log_level_filter(quill::LogLevel::None);

    // Set level to errorlog
    if (error_log_file)
    {
        auto errorlog_level = DB::parseQuillLogLevel(config.getString("logger.errorlog_level", "notice"));
        min_log_level = std::min(errorlog_level, min_log_level);
        error_log_file->set_log_level_filter(errorlog_level);
    }

    // Global logging level (it can be overridden for specific loggers).
    getRootLogger()->setLogLevel(min_log_level);
}

/// NOLINTEND(readability-static-accessed-through-instance)

void Loggers::closeLogs()
{
    if (log_file)
        log_file->closeFile();
    if (error_log_file)
        error_log_file->closeFile();

    getRootLogger()->flushLogs();
}
