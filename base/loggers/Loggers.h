#pragma once

#include <optional>
#include <string>
#include <Poco/AutoPtr.h>
#include <Poco/FileChannel.h>
#include <Poco/Util/Application.h>
#include "OwnSplitChannel.h"

#ifdef WITH_TEXT_LOG
namespace DB
{
    class TextLog;
}
#endif

namespace Poco::Util
{
    class AbstractConfiguration;
}

class Loggers
{
public:
    void buildLoggers(Poco::Util::AbstractConfiguration & config, Poco::Logger & logger, const std::string & cmd_name = "");

    void updateLevels(Poco::Util::AbstractConfiguration & config, Poco::Logger & logger);

    /// Close log files. On next log write files will be reopened.
    void closeLogs(Poco::Logger & logger);

#ifdef WITH_TEXT_LOG
    void setTextLog(std::shared_ptr<DB::TextLog> log, int max_priority);
#endif

private:
    Poco::AutoPtr<Poco::FileChannel> log_file;
    Poco::AutoPtr<Poco::FileChannel> error_log_file;
    Poco::AutoPtr<Poco::Channel> syslog_channel;

    /// Previous value of logger element in config. It is used to reinitialize loggers whenever the value changed.
    std::string config_logger;

#ifdef WITH_TEXT_LOG
    std::weak_ptr<DB::TextLog> text_log;
    int text_log_max_priority = -1;
#endif

    Poco::AutoPtr<DB::OwnSplitChannel> split;
};
