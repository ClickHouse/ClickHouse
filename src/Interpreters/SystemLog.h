#pragma once

#include <Interpreters/StorageID.h>
#include <Common/SystemLogBase.h>
#include <Parsers/IAST.h>

#include <boost/noncopyable.hpp>
#include <vector>

#define LIST_OF_ALL_SYSTEM_LOGS(M) \
    M(QueryLog,              query_log,            "Contains information about executed queries, for example, start time, duration of processing, error messages.") \
    M(QueryThreadLog,        query_thread_log,     "Contains information about threads that execute queries, for example, thread name, thread start time, duration of query processing.") \
    M(PartLog,               part_log,             "This table contains information about events that occurred with data parts in the MergeTree family tables, such as adding or merging data.") \
    M(TraceLog,              trace_log,            "Contains stack traces collected by the sampling query profiler.") \
    M(CrashLog,              crash_log,            "Contains information about stack traces for fatal errors. The table does not exist in the database by default, it is created only when fatal errors occur.") \
    M(TextLog,               text_log,             "Contains logging entries which are normally written to a log file or to stdout.") \
    M(MetricLog,             metric_log,           "Contains history of metrics values from tables system.metrics and system.events, periodically flushed to disk.") \
    M(ErrorLog,              error_log,            "Contains history of error values from table system.errors, periodically flushed to disk.") \
    M(FilesystemCacheLog,    filesystem_cache_log, "Contains a history of all events occurred with filesystem cache for objects on a remote filesystem.") \
    M(FilesystemReadPrefetchesLog, filesystem_read_prefetches_log, "Contains a history of all prefetches done during reading from MergeTables backed by a remote filesystem.") \
    M(ObjectStorageQueueLog, s3queue_log,          "Contains logging entries with the information files processes by S3Queue engine.") \
    M(ObjectStorageQueueLog, azure_queue_log,      "Contains logging entries with the information files processes by S3Queue engine.") \
    M(AsynchronousMetricLog, asynchronous_metric_log, "Contains the historical values for system.asynchronous_metrics, once per time interval (one second by default).") \
    M(OpenTelemetrySpanLog,  opentelemetry_span_log, "Contains information about trace spans for executed queries.") \
    M(QueryViewsLog,         query_views_log,      "Contains information about the dependent views executed when running a query, for example, the view type or the execution time.") \
    M(ZooKeeperLog,          zookeeper_log,        "This table contains information about the parameters of the request to the ZooKeeper server and the response from it.") \
    M(SessionLog,            session_log,          "Contains information about all successful and failed login and logout events.") \
    M(TransactionsInfoLog,   transactions_info_log, "Contains information about all transactions executed on a current server.") \
    M(ProcessorsProfileLog,  processors_profile_log, "Contains profiling information on processors level (building blocks for a pipeline for query execution.") \
    M(AsynchronousInsertLog, asynchronous_insert_log, "Contains a history for all asynchronous inserts executed on current server.") \
    M(BackupLog,             backup_log,           "Contains logging entries with the information about BACKUP and RESTORE operations.") \
    M(BlobStorageLog,        blob_storage_log,     "Contains logging entries with information about various blob storage operations such as uploads and deletes.") \


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

#define DECLARE_PUBLIC_MEMBERS(log_type, member, descr) \
    std::shared_ptr<log_type> member; \

    LIST_OF_ALL_SYSTEM_LOGS(DECLARE_PUBLIC_MEMBERS)
#undef DECLARE_PUBLIC_MEMBERS

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

    /** Creates new table if it does not exist.
      * Renames old table if its structure is not suitable.
      * This cannot be done in constructor to avoid deadlock while renaming a table under locked Context when SystemLog object is created.
      */
    void prepareTable() override;

protected:
    LoggerPtr log;

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
