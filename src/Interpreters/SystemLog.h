#pragma once

#include <Common/SystemLogBase.h>

#include <Interpreters/StorageID.h>

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
class TransactionsInfoLog;
class ProcessorsProfileLog;

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
    /// Events related to transactions
    std::shared_ptr<TransactionsInfoLog> transactions_info_log;
    /// Used to log processors profiling
    std::shared_ptr<ProcessorsProfileLog> processors_profile_log;

    std::vector<ISystemLog *> logs;
};


template <typename LogElement>
class SystemLog : public SystemLogBase<LogElement>, private boost::noncopyable, WithContext
{
public:
    using Self = SystemLog;
    using Base = SystemLogBase<LogElement>;

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

    void shutdown() override;

protected:
    using ISystemLog::mutex;
    using ISystemLog::is_shutdown;
    using ISystemLog::flush_event;
    using ISystemLog::stopFlushThread;
    using Base::log;
    using Base::queue;
    using Base::queue_front_index;
    using Base::is_force_prepare_tables;
    using Base::requested_flush_up_to;
    using Base::flushed_up_to;
    using Base::logged_queue_full_at_index;

private:

    /* Saving thread data */
    const StorageID table_id;
    const String storage_def;
    String create_query;
    String old_create_query;
    bool is_prepared = false;
    const size_t flush_interval_milliseconds;

    /** Creates new table if it does not exist.
      * Renames old table if its structure is not suitable.
      * This cannot be done in constructor to avoid deadlock while renaming a table under locked Context when SystemLog object is created.
      */
    void prepareTable() override;

    void savingThreadFunction() override;

    /// flushImpl can be executed only in saving_thread.
    void flushImpl(const std::vector<LogElement> & to_flush, uint64_t to_flush_end);
    ASTPtr getCreateTableQuery();
};

}
