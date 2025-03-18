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

// TODO: move to libcommon
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

    std::string_view max_size_view{max_size_str};
    size_t multiplier = 1;
    if (max_size_view.ends_with('M'))
    {
        max_size_view.remove_suffix(1);
        multiplier = 1_MiB;
    }

    return DB::parse<size_t>(max_size_view) * multiplier;
}

}

/// NOLINTBEGIN(readability-static-accessed-through-instance)

void Loggers::buildLoggers(Poco::Util::AbstractConfiguration & config, const std::string & /*cmd_name*/, bool allow_console_only)
{
    /// if we are remaping executable, we can start another thread only AFTER we are done
    /// with remapping
    if (!config.getBool("remap_executable", false))
        DB::startQuillBackend();

    std::vector<std::shared_ptr<quill::Sink>> sinks;

    auto current_logger = config.getString("logger", "");
    if (config_logger.has_value())// && *config_logger == current_logger)
        return;

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

    // if (config.getBool("logger.use_syslog", false))
    // {
    //     auto syslog_level = Poco::Logger::parseLevel(config.getString("logger.syslog_level", log_level_string));
    //     max_log_level = std::max(syslog_level, max_log_level);

    //     if (config.has("logger.syslog.address"))
    //     {
    //         syslog_channel = new Poco::Net::RemoteSyslogChannel();
    //         // syslog address
    //         syslog_channel->setProperty(Poco::Net::RemoteSyslogChannel::PROP_LOGHOST, config.getString("logger.syslog.address"));
    //         if (config.has("logger.syslog.hostname"))
    //         {
    //             syslog_channel->setProperty(Poco::Net::RemoteSyslogChannel::PROP_HOST, config.getString("logger.syslog.hostname"));
    //         }
    //         syslog_channel->setProperty(Poco::Net::RemoteSyslogChannel::PROP_FORMAT, config.getString("logger.syslog.format", "syslog"));
    //         syslog_channel->setProperty(
    //             Poco::Net::RemoteSyslogChannel::PROP_FACILITY, config.getString("logger.syslog.facility", "LOG_USER"));
    //     }
    //     else
    //     {
    //         syslog_channel = new Poco::SyslogChannel();
    //         syslog_channel->setProperty(Poco::SyslogChannel::PROP_NAME, cmd_name);
    //         syslog_channel->setProperty(Poco::SyslogChannel::PROP_OPTIONS, config.getString("logger.syslog.options", "LOG_CONS|LOG_PID"));
    //         syslog_channel->setProperty(Poco::SyslogChannel::PROP_FACILITY, config.getString("logger.syslog.facility", "LOG_DAEMON"));
    //     }
    //     syslog_channel->open();

    //     Poco::AutoPtr<OwnPatternFormatter> pf;

    //     if (config.getString("logger.formatting.type", "") == "json")
    //         pf = new OwnJSONPatternFormatter(config);
    //     else
    //         pf = new OwnPatternFormatter;

    //     Poco::AutoPtr<DB::OwnFormattingChannel> log = new DB::OwnFormattingChannel(pf, syslog_channel);
    //     log->setLevel(syslog_level);

    //     split->addChannel(log, "syslog");
    // }

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

    // // Set level and channel to all already created loggers
    // std::vector<std::string> names;
    // logger.names(names);

    // for (const auto & name : names)
    // {
    //     logger.get(name).setLevel(max_log_level);
    //     logger.get(name).setChannel(split);
    // }

    // // Explicitly specified log levels for specific loggers.
    // {
    //     Poco::Util::AbstractConfiguration::Keys loggers_level;
    //     config.keys("logger.levels", loggers_level);

    //     if (!loggers_level.empty())
    //     {
    //         for (const auto & key : loggers_level)
    //         {
    //             if (key == "logger" || key.starts_with("logger["))
    //             {
    //                 const std::string name(config.getString("logger.levels." + key + ".name"));
    //                 const std::string level(config.getString("logger.levels." + key + ".level"));
    //                 logger.root().get(name).setLevel(level);
    //             }
    //             else
    //             {
    //                 // Legacy syntax
    //                 const std::string level(config.getString("logger.levels." + key, "trace"));
    //                 logger.root().get(key).setLevel(level);
    //             }
    //         }
    //     }
    // }
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
    if (config.getBool("logger.console", false)
        || (!config.hasProperty("logger.console") && !is_daemon && should_log_to_console))
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

    // // Set level to syslog
    // int syslog_level = 0;
    // if (config.getBool("logger.use_syslog", false))
    // {
    //     syslog_level = Poco::Logger::parseLevel(config.getString("logger.syslog_level", log_level_string));
    //     max_log_level = std::max(syslog_level, max_log_level);
    // }
    // split->setLevel("syslog", syslog_level);

    // Global logging level (it can be overridden for specific loggers).
    getRootLogger()->setLogLevel(min_log_level);

    // // Set level to all already created loggers
    // std::vector<std::string> names;

    // logger.root().names(names);
    // for (const auto & name : names)
    //     logger.root().get(name).setLevel(max_log_level);

    // logger.root().setLevel(max_log_level);

    // // Explicitly specified log levels for specific loggers.
    // {
    //     Poco::Util::AbstractConfiguration::Keys loggers_level;
    //     config.keys("logger.levels", loggers_level);

    //     if (!loggers_level.empty())
    //     {
    //         for (const auto & key : loggers_level)
    //         {
    //             if (key == "logger" || key.starts_with("logger["))
    //             {
    //                 const std::string name(config.getString("logger.levels." + key + ".name"));
    //                 const std::string level(config.getString("logger.levels." + key + ".level"));
    //                 logger.root().get(name).setLevel(level);
    //             }
    //             else
    //             {
    //                 // Legacy syntax
    //                 const std::string level(config.getString("logger.levels." + key, "trace"));
    //                 logger.root().get(key).setLevel(level);
    //             }
    //         }
    //     }
    // }
}

/// NOLINTEND(readability-static-accessed-through-instance)

void Loggers::closeLogs()
{
    if (log_file)
        log_file->closeFile();
    if (error_log_file)
        error_log_file->closeFile();
}
