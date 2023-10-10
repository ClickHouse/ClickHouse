#include "Loggers.h"

#include <iostream>
#include <Poco/SyslogChannel.h>
#include <Poco/Util/AbstractConfiguration.h>
#include "OwnFormattingChannel.h"
#include "OwnPatternFormatter.h"
#include "OwnSplitChannel.h"
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Poco/Net/RemoteSyslogChannel.h>

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


// TODO: move to libcommon
static std::string createDirectory(const std::string & file)
{
    auto path = fs::path(file).parent_path();
    if (path.empty())
        return "";
    fs::create_directories(path);
    return path;
}

static std::string renderFileNameTemplate(time_t now, const std::string & file_path)
{
    fs::path path{file_path};
    std::tm buf;
    localtime_r(&now, &buf);
    std::ostringstream ss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    ss << std::put_time(&buf, path.filename().c_str());
    return path.replace_filename(ss.str());
}

void Loggers::buildLoggers(Poco::Util::AbstractConfiguration & config, Poco::Logger & logger /*_root*/, const std::string & cmd_name)
{
    auto current_logger = config.getString("logger", "");
    if (config_logger.has_value() && *config_logger == current_logger)
        return;

    config_logger = current_logger;

    bool is_daemon = config.getBool("application.runAsDaemon", false);

    /// Split logs to ordinary log, error log, syslog and console.
    /// Use extended interface of Channel for more comprehensive logging.
    split = new DB::OwnSplitChannel();

    auto log_level_string = config.getString("logger.level", "trace");

    /// different channels (log, console, syslog) may have different loglevels configured
    /// The maximum (the most verbose) of those will be used as default for Poco loggers
    int max_log_level = 0;

    time_t now = std::time({});

    const auto log_path_prop = config.getString("logger.log", "");
    if (!log_path_prop.empty())
    {
        const auto log_path = renderFileNameTemplate(now, log_path_prop);
        createDirectory(log_path);

        std::string ext;
        if (config.getRawString("logger.stream_compress", "false") == "true")
            ext = ".lz4";

        std::cerr << "Logging " << log_level_string << " to " << log_path << ext << std::endl;

        auto log_level = Poco::Logger::parseLevel(log_level_string);
        if (log_level > max_log_level)
        {
            max_log_level = log_level;
        }

        // Set up two channel chains.
        log_file = new Poco::FileChannel;
        log_file->setProperty(Poco::FileChannel::PROP_PATH, fs::weakly_canonical(log_path));
        log_file->setProperty(Poco::FileChannel::PROP_ROTATION, config.getRawString("logger.size", "100M"));
        log_file->setProperty(Poco::FileChannel::PROP_ARCHIVE, "number");
        log_file->setProperty(Poco::FileChannel::PROP_COMPRESS, config.getRawString("logger.compress", "true"));
        log_file->setProperty(Poco::FileChannel::PROP_STREAMCOMPRESS, config.getRawString("logger.stream_compress", "false"));
        log_file->setProperty(Poco::FileChannel::PROP_PURGECOUNT, config.getRawString("logger.count", "1"));
        log_file->setProperty(Poco::FileChannel::PROP_FLUSH, config.getRawString("logger.flush", "true"));
        log_file->setProperty(Poco::FileChannel::PROP_ROTATEONOPEN, config.getRawString("logger.rotateOnOpen", "false"));
        log_file->open();

        Poco::AutoPtr<OwnPatternFormatter> pf;

        if (config.getString("logger.formatting.type", "") == "json")
            pf = new OwnJSONPatternFormatter(config);
        else
            pf = new OwnPatternFormatter;

        Poco::AutoPtr<DB::OwnFormattingChannel> log = new DB::OwnFormattingChannel(pf, log_file);
        log->setLevel(log_level);
        split->addChannel(log, "log");
    }

    const auto errorlog_path_prop = config.getString("logger.errorlog", "");
    if (!errorlog_path_prop.empty())
    {
        const auto errorlog_path = renderFileNameTemplate(now, errorlog_path_prop);
        createDirectory(errorlog_path);

        // NOTE: we don't use notice & critical in the code, so in practice error log collects fatal & error & warning.
        // (!) Warnings are important, they require attention and should never be silenced / ignored.
        auto errorlog_level = Poco::Logger::parseLevel(config.getString("logger.errorlog_level", "notice"));
        if (errorlog_level > max_log_level)
        {
            max_log_level = errorlog_level;
        }

        std::string ext;
        if (config.getRawString("logger.stream_compress", "false") == "true")
            ext = ".lz4";

        std::cerr << "Logging errors to " << errorlog_path << ext << std::endl;

        error_log_file = new Poco::FileChannel;
        error_log_file->setProperty(Poco::FileChannel::PROP_PATH, fs::weakly_canonical(errorlog_path));
        error_log_file->setProperty(Poco::FileChannel::PROP_ROTATION, config.getRawString("logger.size", "100M"));
        error_log_file->setProperty(Poco::FileChannel::PROP_ARCHIVE, "number");
        error_log_file->setProperty(Poco::FileChannel::PROP_COMPRESS, config.getRawString("logger.compress", "true"));
        error_log_file->setProperty(Poco::FileChannel::PROP_STREAMCOMPRESS, config.getRawString("logger.stream_compress", "false"));
        error_log_file->setProperty(Poco::FileChannel::PROP_PURGECOUNT, config.getRawString("logger.count", "1"));
        error_log_file->setProperty(Poco::FileChannel::PROP_FLUSH, config.getRawString("logger.flush", "true"));
        error_log_file->setProperty(Poco::FileChannel::PROP_ROTATEONOPEN, config.getRawString("logger.rotateOnOpen", "false"));

        Poco::AutoPtr<OwnPatternFormatter> pf;

        if (config.getString("logger.formatting.type", "") == "json")
            pf = new OwnJSONPatternFormatter(config);
        else
            pf = new OwnPatternFormatter;

        Poco::AutoPtr<DB::OwnFormattingChannel> errorlog = new DB::OwnFormattingChannel(pf, error_log_file);
        errorlog->setLevel(errorlog_level);
        errorlog->open();
        split->addChannel(errorlog, "errorlog");
    }

    if (config.getBool("logger.use_syslog", false))
    {
        //const std::string & cmd_name = commandName();
        auto syslog_level = Poco::Logger::parseLevel(config.getString("logger.syslog_level", log_level_string));
        if (syslog_level > max_log_level)
        {
            max_log_level = syslog_level;
        }

        if (config.has("logger.syslog.address"))
        {
            syslog_channel = new Poco::Net::RemoteSyslogChannel();
            // syslog address
            syslog_channel->setProperty(Poco::Net::RemoteSyslogChannel::PROP_LOGHOST, config.getString("logger.syslog.address"));
            if (config.has("logger.syslog.hostname"))
            {
                syslog_channel->setProperty(Poco::Net::RemoteSyslogChannel::PROP_HOST, config.getString("logger.syslog.hostname"));
            }
            syslog_channel->setProperty(Poco::Net::RemoteSyslogChannel::PROP_FORMAT, config.getString("logger.syslog.format", "syslog"));
            syslog_channel->setProperty(
                Poco::Net::RemoteSyslogChannel::PROP_FACILITY, config.getString("logger.syslog.facility", "LOG_USER"));
        }
        else
        {
            syslog_channel = new Poco::SyslogChannel();
            syslog_channel->setProperty(Poco::SyslogChannel::PROP_NAME, cmd_name);
            syslog_channel->setProperty(Poco::SyslogChannel::PROP_OPTIONS, config.getString("logger.syslog.options", "LOG_CONS|LOG_PID"));
            syslog_channel->setProperty(Poco::SyslogChannel::PROP_FACILITY, config.getString("logger.syslog.facility", "LOG_DAEMON"));
        }
        syslog_channel->open();

        Poco::AutoPtr<OwnPatternFormatter> pf;

        if (config.getString("logger.formatting.type", "") == "json")
            pf = new OwnJSONPatternFormatter(config);
        else
            pf = new OwnPatternFormatter;

        Poco::AutoPtr<DB::OwnFormattingChannel> log = new DB::OwnFormattingChannel(pf, syslog_channel);
        log->setLevel(syslog_level);

        split->addChannel(log, "syslog");
    }

    bool should_log_to_console = isatty(STDIN_FILENO) || isatty(STDERR_FILENO);
    bool color_logs_by_default = isatty(STDERR_FILENO);

    if (config.getBool("logger.console", false)
        || (!config.hasProperty("logger.console") && !is_daemon && should_log_to_console))
    {
        bool color_enabled = config.getBool("logger.color_terminal", color_logs_by_default);

        auto console_log_level_string = config.getString("logger.console_log_level", log_level_string);
        auto console_log_level = Poco::Logger::parseLevel(console_log_level_string);
        if (console_log_level > max_log_level)
        {
            max_log_level = console_log_level;
        }

        Poco::AutoPtr<OwnPatternFormatter> pf;
        if (config.getString("logger.formatting.type", "") == "json")
            pf = new OwnJSONPatternFormatter(config);
        else
            pf = new OwnPatternFormatter(color_enabled);
        Poco::AutoPtr<DB::OwnFormattingChannel> log = new DB::OwnFormattingChannel(pf, new Poco::ConsoleChannel);
        log->setLevel(console_log_level);
        split->addChannel(log, "console");
    }

    split->open();
    logger.close();
    logger.setChannel(split);

    // Global logging level (it can be overridden for specific loggers).
    logger.setLevel(max_log_level);

    // Set level to all already created loggers
    std::vector<std::string> names;
    //logger_root = Logger::root();
    logger.root().names(names);
    for (const auto & name : names)
        logger.root().get(name).setLevel(max_log_level);

    // Attach to the root logger.
    logger.root().setLevel(max_log_level);
    logger.root().setChannel(logger.getChannel());

    // Explicitly specified log levels for specific loggers.
    {
        Poco::Util::AbstractConfiguration::Keys loggers_level;
        config.keys("logger.levels", loggers_level);

        if (!loggers_level.empty())
        {
            for (const auto & key : loggers_level)
            {
                if (key == "logger" || key.starts_with("logger["))
                {
                    const std::string name(config.getString("logger.levels." + key + ".name"));
                    const std::string level(config.getString("logger.levels." + key + ".level"));
                    logger.root().get(name).setLevel(level);
                }
                else
                {
                    // Legacy syntax
                    const std::string level(config.getString("logger.levels." + key, "trace"));
                    logger.root().get(key).setLevel(level);
                }
            }
        }
    }
#ifndef WITHOUT_TEXT_LOG
    if (config.has("text_log"))
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

        split->addTextLog(DB::TextLog::getLogQueue(log_settings), text_log_level);
    }
#endif
}

void Loggers::updateLevels(Poco::Util::AbstractConfiguration & config, Poco::Logger & logger)
{
    int max_log_level = 0;

    const auto log_level_string = config.getString("logger.level", "trace");
    int log_level = Poco::Logger::parseLevel(log_level_string);
    if (log_level > max_log_level)
        max_log_level = log_level;

    if (log_file)
        split->setLevel("log", log_level);

    // Set level to console
    bool is_daemon = config.getBool("application.runAsDaemon", false);
    bool should_log_to_console = isatty(STDIN_FILENO) || isatty(STDERR_FILENO);
    if (config.getBool("logger.console", false)
        || (!config.hasProperty("logger.console") && !is_daemon && should_log_to_console))
        split->setLevel("console", log_level);
    else
        split->setLevel("console", 0);

    // Set level to errorlog
    if (error_log_file)
    {
        int errorlog_level = Poco::Logger::parseLevel(config.getString("logger.errorlog_level", "notice"));
        if (errorlog_level > max_log_level)
            max_log_level = errorlog_level;
        split->setLevel("errorlog", errorlog_level);
    }

    // Set level to syslog
    int syslog_level = 0;
    if (config.getBool("logger.use_syslog", false))
    {
        syslog_level = Poco::Logger::parseLevel(config.getString("logger.syslog_level", log_level_string));
        if (syslog_level > max_log_level)
            max_log_level = syslog_level;
    }
    split->setLevel("syslog", syslog_level);

    // Global logging level (it can be overridden for specific loggers).
    logger.setLevel(max_log_level);

    // Set level to all already created loggers
    std::vector<std::string> names;

    logger.root().names(names);
    for (const auto & name : names)
        logger.root().get(name).setLevel(max_log_level);

    logger.root().setLevel(max_log_level);

    // Explicitly specified log levels for specific loggers.
    {
        Poco::Util::AbstractConfiguration::Keys loggers_level;
        config.keys("logger.levels", loggers_level);

        if (!loggers_level.empty())
        {
            for (const auto & key : loggers_level)
            {
                if (key == "logger" || key.starts_with("logger["))
                {
                    const std::string name(config.getString("logger.levels." + key + ".name"));
                    const std::string level(config.getString("logger.levels." + key + ".level"));
                    logger.root().get(name).setLevel(level);
                }
                else
                {
                    // Legacy syntax
                    const std::string level(config.getString("logger.levels." + key, "trace"));
                    logger.root().get(key).setLevel(level);
                }
            }
        }
    }
}

void Loggers::closeLogs(Poco::Logger & logger)
{
    if (log_file)
        log_file->close();
    if (error_log_file)
        error_log_file->close();
    // Shouldn't syslog_channel be closed here too?

    if (!log_file)
        logger.warning("Logging to console but received signal to close log file (ignoring).");
}
