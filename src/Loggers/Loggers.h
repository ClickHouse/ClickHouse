#pragma once

#include <Loggers/AuditLog.h>
#include <Loggers/OwnSplitChannel.h>
#include <Poco/AutoPtr.h>
#include <Poco/FileChannel.h>
#include <Poco/Util/Application.h>

#include <memory>
#include <optional>
#include <string>

namespace DB
{
class OwnSplitChannelBase;
class AuditLog;

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

    /// Lazily create the experimental audit writer on configuration reload, so enabling
    /// `allow_experimental_audit_log` at runtime starts audit logging without a server restart.
    void updateAuditLog(Poco::Util::AbstractConfiguration & config);

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
    /// Create and open the standalone audit writer, but only when the experimental feature is
    /// enabled. Idempotent: keeps an already-created writer (the writer is torn down only at
    /// shutdown, never on reload, so concurrent LOG_AUDIT callers cannot use-after-free).
    void createAuditLog(Poco::Util::AbstractConfiguration & config, time_t now);

    Poco::AutoPtr<Poco::FileChannel> log_file;
    Poco::AutoPtr<Poco::FileChannel> error_log_file;
    Poco::AutoPtr<Poco::Channel> syslog_channel;

    /// Standalone audit log writer (bypasses OwnSplitChannel)
    std::unique_ptr<DB::AuditLog> audit_log;

    /// Previous value of logger element in config. It is used to reinitialize loggers whenever the value changed.
    std::optional<std::string> config_logger;

    Poco::AutoPtr<DB::OwnSplitChannelBase> split;
};

class OwnPatternFormatter;
Poco::AutoPtr<OwnPatternFormatter> getFormatForChannel(Poco::Util::AbstractConfiguration & config, const std::string & channel, bool color = false);
