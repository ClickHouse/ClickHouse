#pragma once

#include <optional>
#include <string>
#include <Poco/AutoPtr.h>
#include <Poco/FileChannel.h>
#include <Poco/Util/Application.h>
#include "OwnSplitChannel.h"


namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace DB
{
class RotatingFileSink;
class ConsoleSink;
}

class Loggers
{
public:
    void buildLoggers(Poco::Util::AbstractConfiguration & config, const std::string & cmd_name = "", bool allow_console_only = false);

    void updateLevels(Poco::Util::AbstractConfiguration & config);

    /// Close log files. On next log write files will be reopened.
    void closeLogs();

    virtual ~Loggers() = default;

protected:
    virtual bool allowTextLog() const { return true; }

private:
    std::shared_ptr<DB::RotatingFileSink> log_file;
    std::shared_ptr<DB::RotatingFileSink> error_log_file;
    std::shared_ptr<DB::ConsoleSink> console_sink;


    Poco::AutoPtr<Poco::Channel> syslog_channel;

    /// Previous value of logger element in config. It is used to reinitialize loggers whenever the value changed.
    std::optional<std::string> config_logger;


    Poco::AutoPtr<DB::OwnSplitChannel> split;
};
