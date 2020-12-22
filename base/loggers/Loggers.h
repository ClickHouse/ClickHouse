#pragma once

#include <optional>
#include <string>
#include <Poco/AutoPtr.h>
#include <Poco/FileChannel.h>
#include <Poco/Util/Application.h>
#include <Interpreters/TextLog.h>
#include "OwnSplitChannel.h"

namespace Poco::Util
{
    class AbstractConfiguration;
}

class Loggers
{
public:
    void buildLoggers(Poco::Util::AbstractConfiguration & config, Poco::Logger & logger, const std::string & cmd_name = "");

    /// Close log files. On next log write files will be reopened.
    void closeLogs(Poco::Logger & logger);

    std::optional<size_t> getLayer() const
    {
        return layer; /// layer set in inheritor class BaseDaemonApplication.
    }

    void setTextLog(std::shared_ptr<DB::TextLog> log, int max_priority);

protected:
    std::optional<size_t> layer;

private:
    Poco::AutoPtr<Poco::FileChannel> log_file;
    Poco::AutoPtr<Poco::FileChannel> error_log_file;
    Poco::AutoPtr<Poco::Channel> syslog_channel;

    /// Previous value of logger element in config. It is used to reinitialize loggers whenever the value changed.
    std::string config_logger;

    std::weak_ptr<DB::TextLog> text_log;
    int text_log_max_priority = -1;

    Poco::AutoPtr<DB::OwnSplitChannel> split;
};
