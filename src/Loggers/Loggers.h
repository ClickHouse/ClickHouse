#pragma once

#include <Loggers/OwnSplitChannel.h>
#include <Poco/AutoPtr.h>
#include <Poco/FileChannel.h>
#include <Poco/Util/Application.h>

#include <optional>
#include <string>

namespace DB
{
class OwnSplitChannelBase;

using AsyncLogQueueSize = std::pair<std::string, size_t>;
using AsyncLogQueueSizes = VectorWithMemoryTracking<AsyncLogQueueSize>;
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

    /// Stop/restart the background logging threads. Used around remapExecutable, which rewrites the whole
    /// code segment and requires that no other thread runs code meanwhile (the async threads poll, so they
    /// must be joined for the duration). No-op for synchronous logging.
    void stopAsyncLoggingThreads();
    void startAsyncLoggingThreads();

    /// Best-effort variant for destructors: stop and join only the asynchronous logging threads,
    /// without shutting logging down for the synchronous path. Later destructors may still log,
    /// and a closed asynchronous channel delivers their messages synchronously.
    /// No-op for synchronous logging.
    void closeAsyncLogging();

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
