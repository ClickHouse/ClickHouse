#pragma once

#include <thread>
#include <atomic>
#include <memory>
#include <vector>
#include <condition_variable>
#include <boost/noncopyable.hpp>

#include <base/types.h>
#include <Core/Defines.h>
#include <Storages/IStorage_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Parsers/IAST_fwd.h>
#include <Common/ThreadPool.h>


namespace Poco
{
class Logger;
namespace Util
{
class AbstractConfiguration;
}
}

namespace DB
{


/** Allow to store structured log in system table.
  *
  * Logging is asynchronous. Data is put into queue from where it will be read by separate thread.
  * That thread inserts log into a table with no more than specified periodicity.
  */

/** Structure of log, template parameter.
  * Structure could change on server version update.
  * If on first write, existing table has different structure,
  *  then it get renamed (put aside) and new table is created.
  */
/* Example:
    struct LogElement
    {
        /// default constructor must be available
        /// fields

        static std::string name();
        static NamesAndTypesList getNamesAndTypes();
        static NamesAndAliases getNamesAndAliases();
        void appendToBlock(MutableColumns & columns) const;
    };
    */

class QueryLog;
class QueryThreadLog;
class PartLog;
class TextLog;
class TraceLog;
class CrashLog;
class MetricLog;
class AsynchronousMetricLog;
class OpenTelemetrySpanLog;
class QueryViewsLog;
class ZooKeeperLog;
class SessionLog;


class ISystemLog
{
public:
    virtual String getName() = 0;
    //// force -- force table creation (used for SYSTEM FLUSH LOGS)
    virtual void flush(bool force = false) = 0;
    virtual void prepareTable() = 0;

    /// Start the background thread.
    virtual void startup();

    /// Stop the background flush thread before destructor. No more data will be written.
    virtual void shutdown() = 0;

    virtual ~ISystemLog() = default;

    virtual void savingThreadFunction() = 0;

    /// returns CREATE TABLE query, but with removed:
    /// - UUID
    /// - SETTINGS (for MergeTree)
    /// That way it can be used to compare with the SystemLog::getCreateTableQuery()
    static ASTPtr getCreateTableQueryClean(const StorageID & table_id, ContextPtr context);

protected:
    ThreadFromGlobalPool saving_thread;

    /// Data shared between callers of add()/flush()/shutdown(), and the saving thread
    std::mutex mutex;

    bool is_shutdown = false;
    std::condition_variable flush_event;

    void stopFlushThread();
};


/// System logs should be destroyed in destructor of the last Context and before tables,
///  because SystemLog destruction makes insert query while flushing data into underlying tables
struct SystemLogs
{
    SystemLogs(ContextPtr global_context, const Poco::Util::AbstractConfiguration & config);
    ~SystemLogs();

    void shutdown();

    std::shared_ptr<QueryLog> query_log;                /// Used to log queries.
    std::shared_ptr<QueryThreadLog> query_thread_log;   /// Used to log query threads.
    std::shared_ptr<PartLog> part_log;                  /// Used to log operations with parts
    std::shared_ptr<TraceLog> trace_log;                /// Used to log traces from query profiler
    std::shared_ptr<CrashLog> crash_log;                /// Used to log server crashes.
    std::shared_ptr<TextLog> text_log;                  /// Used to log all text messages.
    std::shared_ptr<MetricLog> metric_log;              /// Used to log all metrics.
    /// Metrics from system.asynchronous_metrics.
    std::shared_ptr<AsynchronousMetricLog> asynchronous_metric_log;
    /// OpenTelemetry trace spans.
    std::shared_ptr<OpenTelemetrySpanLog> opentelemetry_span_log;
    /// Used to log queries of materialized and live views
    std::shared_ptr<QueryViewsLog> query_views_log;
    /// Used to log all actions of ZooKeeper client
    std::shared_ptr<ZooKeeperLog> zookeeper_log;
    /// Login, LogOut and Login failure events
    std::shared_ptr<SessionLog> session_log;

    std::vector<ISystemLog *> logs;
};


template <typename LogElement>
class SystemLog : public ISystemLog, private boost::noncopyable, WithContext
{
public:
    using Self = SystemLog;

    /** Parameter: table name where to write log.
      * If table is not exists, then it get created with specified engine.
      * If it already exists, then its structure is checked to be compatible with structure of log record.
      *  If it is compatible, then existing table will be used.
      *  If not - then existing table will be renamed to same name but with suffix '_N' at end,
      *   where N - is a minimal number from 1, for that table with corresponding name doesn't exist yet;
      *   and new table get created - as if previous table was not exist.
      */
    SystemLog(
        ContextPtr context_,
        const String & database_name_,
        const String & table_name_,
        const String & storage_def_,
        size_t flush_interval_milliseconds_);

    /** Append a record into log.
      * Writing to table will be done asynchronously and in case of failure, record could be lost.
      */
    void add(const LogElement & element);

    void shutdown() override;

    /// Flush data in the buffer to disk
    void flush(bool force) override;

    String getName() override
    {
        return LogElement::name();
    }

    ASTPtr getCreateTableQuery();

protected:
    Poco::Logger * log;

private:
    /* Saving thread data */
    const StorageID table_id;
    const String storage_def;
    String create_query;
    String old_create_query;
    bool is_prepared = false;
    const size_t flush_interval_milliseconds;

    // Queue is bounded. But its size is quite large to not block in all normal cases.
    std::vector<LogElement> queue;
    // An always-incrementing index of the first message currently in the queue.
    // We use it to give a global sequential index to every message, so that we
    // can wait until a particular message is flushed. This is used to implement
    // synchronous log flushing for SYSTEM FLUSH LOGS.
    uint64_t queue_front_index = 0;
    // A flag that says we must create the tables even if the queue is empty.
    bool is_force_prepare_tables = false;
    // Requested to flush logs up to this index, exclusive
    uint64_t requested_flush_up_to = 0;
    // Flushed log up to this index, exclusive
    uint64_t flushed_up_to = 0;
    // Logged overflow message at this queue front index
    uint64_t logged_queue_full_at_index = -1;

    void savingThreadFunction() override;

    /** Creates new table if it does not exist.
      * Renames old table if its structure is not suitable.
      * This cannot be done in constructor to avoid deadlock while renaming a table under locked Context when SystemLog object is created.
      */
    void prepareTable() override;

    /// flushImpl can be executed only in saving_thread.
    void flushImpl(const std::vector<LogElement> & to_flush, uint64_t to_flush_end);
};

}
