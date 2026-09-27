#pragma once

#include <Poco/AutoPtr.h>
#include <Poco/FileChannel.h>
#include <Poco/Util/Application.h>

#include <optional>
#include <string>

namespace DB
{
class OwnSplitChannelBase;

using AsyncLogQueueSize = std::pair<std::string, size_t>;
using AsyncLogQueueSizes = std::vector<AsyncLogQueueSize>;
}

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

    DB::AsyncLogQueueSizes getAsynchronousMetricsFromAsyncLogs();
    void flushTextLogs();

    virtual ~Loggers() = default;

    void stopLogging();

protected:
    virtual bool allowTextLog() const { return true; }

private:
    Poco::AutoPtr<Poco::FileChannel> log_file;
    Poco::AutoPtr<Poco::FileChannel> error_log_file;
    Poco::AutoPtr<Poco::Channel> syslog_channel;

    /// Previous value of logger element in config. It is used to reinitialize loggers whenever the value changed.
    std::optional<std::string> config_logger;

    Poco::AutoPtr<DB::OwnSplitChannelBase> split;
};

class OwnPatternFormatter;
Poco::AutoPtr<OwnPatternFormatter> getFormatForChannel(Poco::Util::AbstractConfiguration & config, const std::string & channel, bool color = false);
