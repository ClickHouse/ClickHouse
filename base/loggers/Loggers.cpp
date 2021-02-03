#include "Loggers.h"

#include <iostream>
#include <Poco/SyslogChannel.h>
#include <Poco/Util/AbstractConfiguration.h>
#include "OwnFormattingChannel.h"
#include "OwnPatternFormatter.h"
#include <Poco/ConsoleChannel.h>
#include <Poco/File.h>
#include <Poco/Logger.h>
#include <Poco/Net/RemoteSyslogChannel.h>
#include <Poco/Path.h>

namespace DB
{
    class SensitiveDataMasker;
}


// TODO: move to libcommon
static std::string createDirectory(const std::string & file)
{
    auto path = Poco::Path(file).makeParent();
    if (path.toString().empty())
        return "";
    Poco::File(path).createDirectories();
    return path.toString();
};

void Loggers::setTextLog(std::shared_ptr<DB::TextLog> log, int max_priority)
{
    text_log = log;
    text_log_max_priority = max_priority;
}

void Loggers::buildLoggers(Poco::Util::AbstractConfiguration & config, Poco::Logger & logger /*_root*/, const std::string & cmd_name)
{
    if (split)
        if (auto log = text_log.lock())
            split->addTextLog(log, text_log_max_priority);

    auto current_logger = config.getString("logger", "");
    if (config_logger == current_logger)
        return;

    config_logger = current_logger;

    bool is_daemon = config.getBool("application.runAsDaemon", false);

    /// Split logs to ordinary log, error log, syslog and console.
    /// Use extended interface of Channel for more comprehensive logging.
    split = new DB::OwnSplitChannel();

    auto log_level = config.getString("logger.level", "trace");
    const auto log_path = config.getString("logger.log", "");
    if (!log_path.empty())
    {
        createDirectory(log_path);
        std::cerr << "Logging " << log_level << " to " << log_path << std::endl;

        // Set up two channel chains.
        log_file = new Poco::FileChannel;
        log_file->setProperty(Poco::FileChannel::PROP_PATH, Poco::Path(log_path).absolute().toString());
        log_file->setProperty(Poco::FileChannel::PROP_ROTATION, config.getRawString("logger.size", "100M"));
        log_file->setProperty(Poco::FileChannel::PROP_ARCHIVE, "number");
        log_file->setProperty(Poco::FileChannel::PROP_COMPRESS, config.getRawString("logger.compress", "true"));
        log_file->setProperty(Poco::FileChannel::PROP_PURGECOUNT, config.getRawString("logger.count", "1"));
        log_file->setProperty(Poco::FileChannel::PROP_FLUSH, config.getRawString("logger.flush", "true"));
        log_file->setProperty(Poco::FileChannel::PROP_ROTATEONOPEN, config.getRawString("logger.rotateOnOpen", "false"));
        log_file->open();

        Poco::AutoPtr<OwnPatternFormatter> pf = new OwnPatternFormatter(this);

        Poco::AutoPtr<DB::OwnFormattingChannel> log = new DB::OwnFormattingChannel(pf, log_file);
        split->addChannel(log);
    }

    const auto errorlog_path = config.getString("logger.errorlog", "");
    if (!errorlog_path.empty())
    {
        createDirectory(errorlog_path);
        std::cerr << "Logging errors to " << errorlog_path << std::endl;

        error_log_file = new Poco::FileChannel;
        error_log_file->setProperty(Poco::FileChannel::PROP_PATH, Poco::Path(errorlog_path).absolute().toString());
        error_log_file->setProperty(Poco::FileChannel::PROP_ROTATION, config.getRawString("logger.size", "100M"));
        error_log_file->setProperty(Poco::FileChannel::PROP_ARCHIVE, "number");
        error_log_file->setProperty(Poco::FileChannel::PROP_COMPRESS, config.getRawString("logger.compress", "true"));
        error_log_file->setProperty(Poco::FileChannel::PROP_PURGECOUNT, config.getRawString("logger.count", "1"));
        error_log_file->setProperty(Poco::FileChannel::PROP_FLUSH, config.getRawString("logger.flush", "true"));
        error_log_file->setProperty(Poco::FileChannel::PROP_ROTATEONOPEN, config.getRawString("logger.rotateOnOpen", "false"));

        Poco::AutoPtr<OwnPatternFormatter> pf = new OwnPatternFormatter(this);

        Poco::AutoPtr<DB::OwnFormattingChannel> errorlog = new DB::OwnFormattingChannel(pf, error_log_file);
        errorlog->setLevel(Poco::Message::PRIO_NOTICE);
        errorlog->open();
        split->addChannel(errorlog);
    }

    /// "dynamic_layer_selection" is needed only for Yandex.Metrika, that share part of ClickHouse code.
    /// We don't need this configuration parameter.

    if (config.getBool("logger.use_syslog", false) || config.getBool("dynamic_layer_selection", false))
    {
        //const std::string & cmd_name = commandName();

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

        Poco::AutoPtr<OwnPatternFormatter> pf = new OwnPatternFormatter(this, OwnPatternFormatter::ADD_LAYER_TAG);

        Poco::AutoPtr<DB::OwnFormattingChannel> log = new DB::OwnFormattingChannel(pf, syslog_channel);
        split->addChannel(log);
    }

    bool should_log_to_console = isatty(STDIN_FILENO) || isatty(STDERR_FILENO);
    bool color_logs_by_default = isatty(STDERR_FILENO);

    if (config.getBool("logger.console", false)
        || (!config.hasProperty("logger.console") && !is_daemon && should_log_to_console))
    {
        bool color_enabled = config.getBool("logger.color_terminal", color_logs_by_default);

        Poco::AutoPtr<OwnPatternFormatter> pf = new OwnPatternFormatter(this, OwnPatternFormatter::ADD_NOTHING, color_enabled);
        Poco::AutoPtr<DB::OwnFormattingChannel> log = new DB::OwnFormattingChannel(pf, new Poco::ConsoleChannel);
        logger.warning("Logging " + log_level + " to console");
        split->addChannel(log);
    }

    split->open();
    logger.close();
    logger.setChannel(split);

    // Global logging level (it can be overridden for specific loggers).
    logger.setLevel(log_level);

    // Set level to all already created loggers
    std::vector<std::string> names;
    //logger_root = Logger::root();
    logger.root().names(names);
    for (const auto & name : names)
        logger.root().get(name).setLevel(log_level);

    // Attach to the root logger.
    logger.root().setLevel(log_level);
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
