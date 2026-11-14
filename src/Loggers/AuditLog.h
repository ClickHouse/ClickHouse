#pragma once

#include <unistd.h>

#include <atomic>
#include <memory>
#include <string>
#include <string_view>

#include <Common/MemoryTrackerBlockerInThread.h>

#include <Poco/AutoPtr.h>
#include <Poco/FileChannel.h>

#include <fmt/format.h>

namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

class OwnFormattingChannel;

/// Standalone audit log writer.
/// Writes directly to its own OwnFormattingChannel / Poco::FileChannel,
/// bypassing OwnSplitChannel routing. Audit records are written synchronously so they are not lost
/// if a shared asynchronous log queue is full.
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

    /// `query_id`, if not empty, replaces the query ID of the current thread in the record prefix: a record
    /// written on behalf of another query (e.g. an entry of a flushed asynchronous insert) carries its ID.
    void write(std::string message, std::string_view query_id = {});

    size_t getQueueSize() const;

private:
    Poco::AutoPtr<Poco::FileChannel> file_channel;
    std::shared_ptr<OwnFormattingChannel> formatting_channel;
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

/// The writer regardless of the runtime gate (nullptr only if none was ever created). For writing
/// records whose emission was already decided from the policy in effect when the query was admitted
/// (see `decideAuditLog`): a `SYSTEM RELOAD CONFIG` that turns the gate off in the meantime must not
/// make those records disappear. The writer is never destroyed once created.
AuditLog * getGlobalAuditLog();

}

#define LOG_AUDIT(audit_log_ptr, ...) LOG_AUDIT_FOR_QUERY(audit_log_ptr, std::string_view{}, __VA_ARGS__)

#define LOG_AUDIT_FOR_QUERY(audit_log_ptr, query_id, ...) do                                                 \
{                                                                                                           \
    auto * _audit_log_instance = (audit_log_ptr);                                                           \
    if (!_audit_log_instance)                                                                                \
        break;                                                                                              \
    MemoryTrackerBlockerInThread _audit_mem_block(VariableContext::Global);                                  \
    try                                                                                                     \
    {                                                                                                       \
        _audit_log_instance->write(fmt::format(__VA_ARGS__), (query_id));                                   \
    }                                                                                                       \
    catch (...)                                                                                              \
    {                                                                                                       \
        (void)::write(STDERR_FILENO, static_cast<const void *>("Failed to write audit log message\n"), 34); \
    }                                                                                                       \
} while (false)
