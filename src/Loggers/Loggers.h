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

class Loggers
{
public:
    void buildLoggers(Poco::Util::AbstractConfiguration & config, Poco::Logger & logger, const std::string & cmd_name = "");

    void updateLevels(Poco::Util::AbstractConfiguration & config, Poco::Logger & logger);

    /// Close log files. On next log write files will be reopened.
    void closeLogs(Poco::Logger & logger);

    virtual ~Loggers() = default;

protected:
    virtual bool allowTextLog() const { return true; }

private:
    Poco::AutoPtr<Poco::FileChannel> log_file;
    Poco::AutoPtr<Poco::FileChannel> error_log_file;
    Poco::AutoPtr<Poco::Channel> syslog_channel;

    /// Previous value of logger element in config. It is used to reinitialize loggers whenever the value changed.
    std::optional<std::string> config_logger;


    Poco::AutoPtr<DB::OwnSplitChannel> split;
};
