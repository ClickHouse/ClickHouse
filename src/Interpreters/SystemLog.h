#pragma once

#include <Interpreters/StorageID.h>
#include <Common/SystemLogBase.h>
#include <Parsers/IAST.h>

#include <boost/noncopyable.hpp>
#include <vector>

#define LIST_OF_ALL_SYSTEM_LOGS(M) \
    M(QueryLog, query_log, "Used to log queries.") \
    M(QueryThreadLog, query_thread_log, "Used to log query threads.") \
    M(PartLog, part_log, "Used to log operations with parts.") \
    M(TraceLog, trace_log, "Used to log traces from query profiler.") \
    M(CrashLog, crash_log, "Used to log server crashes.") \
    M(TextLog, text_log, "Used to log all text messages.") \
    M(MetricLog, metric_log, "Used to log all metrics.") \
    M(ErrorLog, error_log, "Used to log errors.") \
    M(FilesystemCacheLog, filesystem_cache_log, "") \
    M(FilesystemReadPrefetchesLog, filesystem_read_prefetches_log, "") \
    M(ObjectStorageQueueLog, s3_queue_log, "") \
    M(ObjectStorageQueueLog, azure_queue_log, "") \
    M(AsynchronousMetricLog, asynchronous_metric_log, "Metrics from system.asynchronous_metrics") \
    M(OpenTelemetrySpanLog, opentelemetry_span_log, "OpenTelemetry trace spans.") \
    M(QueryViewsLog, query_views_log, "Used to log queries of materialized and live views.") \
    M(ZooKeeperLog, zookeeper_log, "Used to log all actions of ZooKeeper client.") \
    M(SessionLog, session_log, "Login, LogOut and Login failure events.") \
    M(TransactionsInfoLog, transactions_info_log, "Events related to transactions.") \
    M(ProcessorsProfileLog, processors_profile_log, "Used to log processors profiling") \
    M(AsynchronousInsertLog, asynchronous_insert_log, "") \
    M(BackupLog, backup_log, "Backup and restore events") \
    M(BlobStorageLog, blob_storage_log, "Log blob storage operations") \


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
        static ColumnsDescription getColumnsDescription();
        /// TODO: Remove this method, we can return aliases directly from getColumnsDescription().
        static NamesAndAliases getNamesAndAliases();
        void appendToBlock(MutableColumns & columns) const;
    };
    */

/// NOLINTBEGIN(bugprone-macro-parentheses)
#define FORWARD_DECLARATION(log_type, member, descr) \
    class log_type; \

LIST_OF_ALL_SYSTEM_LOGS(FORWARD_DECLARATION)
#undef FORWARD_DECLARATION
/// NOLINTEND(bugprone-macro-parentheses)


/// System logs should be destroyed in destructor of the last Context and before tables,
///  because SystemLog destruction makes insert query while flushing data into underlying tables
class SystemLogs
{
public:
    SystemLogs() = default;
    SystemLogs(ContextPtr global_context, const Poco::Util::AbstractConfiguration & config);
    SystemLogs(const SystemLogs & other) = default;

    void flush(bool should_prepare_tables_anyway);
    void flushAndShutdown();
    void shutdown();
    void handleCrash();

/// NOLINTBEGIN(bugprone-macro-parentheses)
#define PUBLIC_MEMBERS(log_type, member, descr) \
    std::shared_ptr<log_type> member; \

    LIST_OF_ALL_SYSTEM_LOGS(PUBLIC_MEMBERS)
#undef PUBLIC_MEMBERS
/// NOLINTEND(bugprone-macro-parentheses)

private:
    std::vector<ISystemLog *> getAllLogs() const;
};

struct SystemLogSettings
{
    SystemLogQueueSettings queue_settings;

    String engine;
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
    SystemLog(ContextPtr context_,
              const SystemLogSettings & settings_,
              std::shared_ptr<SystemLogQueue<LogElement>> queue_ = nullptr);

    /** Append a record into log.
      * Writing to table will be done asynchronously and in case of failure, record could be lost.
      */

    void shutdown() override;

    void stopFlushThread() override;

    /** Creates new table if it does not exist.
      * Renames old table if its structure is not suitable.
      * This cannot be done in constructor to avoid deadlock while renaming a table under locked Context when SystemLog object is created.
      */
    void prepareTable() override;

protected:
    LoggerPtr log;

    using ISystemLog::is_shutdown;
    using ISystemLog::saving_thread;
    using ISystemLog::thread_mutex;
    using Base::queue;

    StoragePtr getStorage() const;

    /// Some tables can override settings for internal queries
    virtual void addSettingsForQuery(ContextMutablePtr & mutable_context, IAST::QueryKind query_kind) const;

private:
    /* Saving thread data */
    const StorageID table_id;
    const String storage_def;
    const String create_query;
    String old_create_query;
    bool is_prepared = false;

    void savingThreadFunction() override;

    /// flushImpl can be executed only in saving_thread.
    void flushImpl(const std::vector<LogElement> & to_flush, uint64_t to_flush_end);
    ASTPtr getCreateTableQuery();
};

}
