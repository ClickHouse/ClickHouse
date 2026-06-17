#pragma once

#include <unistd.h>

#include <atomic>
#include <memory>
#include <string>

#include <Common/MemoryTrackerBlockerInThread.h>

#include <Poco/AutoPtr.h>
#include <Poco/FileChannel.h>
#include <Poco/Runnable.h>
#include <Poco/Thread.h>

#include <fmt/format.h>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

class OwnFormattingChannel;
class AsyncLogMessageQueue;

/// Standalone audit log writer.
/// Writes directly to its own OwnFormattingChannel / Poco::FileChannel,
/// bypassing OwnSplitChannel routing. Reuses AsyncLogMessageQueue for async mode.
class AuditLog
{
public:
    AuditLog(bool async, size_t queue_size);
    ~AuditLog();

    AuditLog(const AuditLog &) = delete;
    AuditLog & operator=(const AuditLog &) = delete;

    void configure(Poco::Util::AbstractConfiguration & config, const std::string & auditlog_path);
    void open();
    void close();

    /// Close the underlying file so it can be reopened on next write (SIGHUP rotation).
    void closeFile();

    void write(std::string message);

    size_t getQueueSize() const;

private:
    friend struct AuditLogRunnable;
    void run();

    Poco::AutoPtr<Poco::FileChannel> file_channel;
    std::shared_ptr<OwnFormattingChannel> formatting_channel;

    const bool is_async;
    std::unique_ptr<AsyncLogMessageQueue> queue;
    std::unique_ptr<Poco::Runnable> writer_runnable;
    std::unique_ptr<Poco::Thread> writer_thread;
    std::atomic<bool> is_open{false};
};

AuditLog * getAuditLog();
void setGlobalAuditLog(AuditLog * log);

/// Runtime gate for allow_experimental_audit_log.
/// Checked by getAuditLog; toggled by loadOrReloadAuditTypes.
void setAuditLoggingEnabled(bool enabled);

/// Whether a writer was created at startup (logger.auditlog configured).
/// Unlike getAuditLog, this ignores the allow_audit_logging flag.
bool hasGlobalAuditLog();

}

#define LOG_AUDIT(audit_log_ptr, ...) do                                                                    \
{                                                                                                           \
    auto * _audit_log_instance = (audit_log_ptr);                                                           \
    if (!_audit_log_instance)                                                                                \
        break;                                                                                              \
    MemoryTrackerBlockerInThread _audit_mem_block(VariableContext::Global);                                  \
    try                                                                                                     \
    {                                                                                                       \
        _audit_log_instance->write(fmt::format(__VA_ARGS__));                                               \
    }                                                                                                       \
    catch (...)                                                                                              \
    {                                                                                                       \
        (void)::write(STDERR_FILENO, static_cast<const void *>("Failed to write audit log message\n"), 34); \
    }                                                                                                       \
} while (false)
