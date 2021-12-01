#pragma once

#include <thread>
#include <atomic>
#include <memory>
#include <vector>

#include <condition_variable>
#include <boost/noncopyable.hpp>
#include <common/logger_useful.h>
#include <common/scope_guard.h>
#include <common/types.h>
#include <Core/Defines.h>
#include <Storages/IStorage.h>
#include <Common/Stopwatch.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTInsertQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/Context.h>
#include <Common/setThreadName.h>
#include <Common/ThreadPool.h>
#include <IO/WriteHelpers.h>
#include <Poco/Util/AbstractConfiguration.h>


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

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
}

#define DBMS_SYSTEM_LOG_QUEUE_SIZE 1048576

class QueryLog;
class QueryThreadLog;
class PartLog;
class TextLog;
class TraceLog;
class CrashLog;
class MetricLog;
class AsynchronousMetricLog;
class OpenTelemetrySpanLog;


class ISystemLog
{
public:
    virtual String getName() = 0;
    virtual ASTPtr getCreateTableQuery() = 0;
    //// force -- force table creation (used for SYSTEM FLUSH LOGS)
    virtual void flush(bool force = false) = 0;
    virtual void prepareTable() = 0;
    virtual void startup() = 0;
    virtual void shutdown() = 0;
    virtual ~ISystemLog() = default;
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

    void stopFlushThread();

    /// Flush data in the buffer to disk
    void flush(bool force = false) override;

    /// Start the background thread.
    void startup() override;

    /// Stop the background flush thread before destructor. No more data will be written.
    void shutdown() override
    {
        stopFlushThread();
        if (table)
            table->flushAndShutdown();
    }

    String getName() override
    {
        return LogElement::name();
    }

    ASTPtr getCreateTableQuery() override;

protected:
    Poco::Logger * log;

private:
    /* Saving thread data */
    const StorageID table_id;
    const String storage_def;
    StoragePtr table;
    bool is_prepared = false;
    const size_t flush_interval_milliseconds;
    ThreadFromGlobalPool saving_thread;

    /* Data shared between callers of add()/flush()/shutdown(), and the saving thread */
    std::mutex mutex;
    // Queue is bounded. But its size is quite large to not block in all normal cases.
    std::vector<LogElement> queue;
    // An always-incrementing index of the first message currently in the queue.
    // We use it to give a global sequential index to every message, so that we
    // can wait until a particular message is flushed. This is used to implement
    // synchronous log flushing for SYSTEM FLUSH LOGS.
    uint64_t queue_front_index = 0;
    bool is_shutdown = false;
    // A flag that says we must create the tables even if the queue is empty.
    bool is_force_prepare_tables = false;
    std::condition_variable flush_event;
    // Requested to flush logs up to this index, exclusive
    uint64_t requested_flush_up_to = 0;
    // Flushed log up to this index, exclusive
    uint64_t flushed_up_to = 0;
    // Logged overflow message at this queue front index
    uint64_t logged_queue_full_at_index = -1;

    void savingThreadFunction();

    /** Creates new table if it does not exist.
      * Renames old table if its structure is not suitable.
      * This cannot be done in constructor to avoid deadlock while renaming a table under locked Context when SystemLog object is created.
      */
    void prepareTable() override;

    /// flushImpl can be executed only in saving_thread.
    void flushImpl(const std::vector<LogElement> & to_flush, uint64_t to_flush_end);
};


template <typename LogElement>
SystemLog<LogElement>::SystemLog(
    ContextPtr context_,
    const String & database_name_,
    const String & table_name_,
    const String & storage_def_,
    size_t flush_interval_milliseconds_)
    : WithContext(context_)
    , table_id(database_name_, table_name_)
    , storage_def(storage_def_)
    , flush_interval_milliseconds(flush_interval_milliseconds_)
{
    assert(database_name_ == DatabaseCatalog::SYSTEM_DATABASE);
    log = &Poco::Logger::get("SystemLog (" + database_name_ + "." + table_name_ + ")");
}


template <typename LogElement>
void SystemLog<LogElement>::startup()
{
    std::lock_guard lock(mutex);
    saving_thread = ThreadFromGlobalPool([this] { savingThreadFunction(); });
}


static thread_local bool recursive_add_call = false;

template <typename LogElement>
void SystemLog<LogElement>::add(const LogElement & element)
{
    /// It is possible that the method will be called recursively.
    /// Better to drop these events to avoid complications.
    if (recursive_add_call)
        return;
    recursive_add_call = true;
    SCOPE_EXIT({ recursive_add_call = false; });

    /// Memory can be allocated while resizing on queue.push_back.
    /// The size of allocation can be in order of a few megabytes.
    /// But this should not be accounted for query memory usage.
    /// Otherwise the tests like 01017_uniqCombined_memory_usage.sql will be flacky.
    MemoryTracker::BlockerInThread temporarily_disable_memory_tracker(VariableContext::Global);

    /// Should not log messages under mutex.
    bool queue_is_half_full = false;

    {
        std::unique_lock lock(mutex);

        if (is_shutdown)
            return;

        if (queue.size() == DBMS_SYSTEM_LOG_QUEUE_SIZE / 2)
        {
            queue_is_half_full = true;

            // The queue more than half full, time to flush.
            // We only check for strict equality, because messages are added one
            // by one, under exclusive lock, so we will see each message count.
            // It is enough to only wake the flushing thread once, after the message
            // count increases past half available size.
            const uint64_t queue_end = queue_front_index + queue.size();
            if (requested_flush_up_to < queue_end)
                requested_flush_up_to = queue_end;

            flush_event.notify_all();
        }

        if (queue.size() >= DBMS_SYSTEM_LOG_QUEUE_SIZE)
        {
            // Ignore all further entries until the queue is flushed.
            // Log a message about that. Don't spam it -- this might be especially
            // problematic in case of trace log. Remember what the front index of the
            // queue was when we last logged the message. If it changed, it means the
            // queue was flushed, and we can log again.
            if (queue_front_index != logged_queue_full_at_index)
            {
                logged_queue_full_at_index = queue_front_index;

                // TextLog sets its logger level to 0, so this log is a noop and
                // there is no recursive logging.
                lock.unlock();
                LOG_ERROR(log, "Queue is full for system log '{}' at {}", demangle(typeid(*this).name()), queue_front_index);
            }

            return;
        }

        queue.push_back(element);
    }

    if (queue_is_half_full)
        LOG_INFO(log, "Queue is half full for system log '{}'.", demangle(typeid(*this).name()));
}


template <typename LogElement>
void SystemLog<LogElement>::flush(bool force)
{
    uint64_t this_thread_requested_offset;

    {
        std::unique_lock lock(mutex);

        if (is_shutdown)
            return;

        this_thread_requested_offset = queue_front_index + queue.size();

        // Publish our flush request, taking care not to overwrite the requests
        // made by other threads.
        is_force_prepare_tables |= force;
        requested_flush_up_to = std::max(requested_flush_up_to,
            this_thread_requested_offset);

        flush_event.notify_all();
    }

    LOG_DEBUG(log, "Requested flush up to offset {}",
        this_thread_requested_offset);

    // Use an arbitrary timeout to avoid endless waiting. 60s proved to be
    // too fast for our parallel functional tests, probably because they
    // heavily load the disk.
    const int timeout_seconds = 180;
    std::unique_lock lock(mutex);
    bool result = flush_event.wait_for(lock, std::chrono::seconds(timeout_seconds),
        [&] { return flushed_up_to >= this_thread_requested_offset
                && !is_force_prepare_tables; });

    if (!result)
    {
        throw Exception("Timeout exceeded (" + toString(timeout_seconds) + " s) while flushing system log '" + demangle(typeid(*this).name()) + "'.",
            ErrorCodes::TIMEOUT_EXCEEDED);
    }
}


template <typename LogElement>
void SystemLog<LogElement>::stopFlushThread()
{
    {
        std::lock_guard lock(mutex);

        if (!saving_thread.joinable())
        {
            return;
        }

        if (is_shutdown)
        {
            return;
        }

        is_shutdown = true;

        /// Tell thread to shutdown.
        flush_event.notify_all();
    }

    saving_thread.join();
}


template <typename LogElement>
void SystemLog<LogElement>::savingThreadFunction()
{
    setThreadName("SystemLogFlush");

    std::vector<LogElement> to_flush;
    bool exit_this_thread = false;
    while (!exit_this_thread)
    {
        try
        {
            // The end index (exclusive, like std end()) of the messages we are
            // going to flush.
            uint64_t to_flush_end = 0;
            // Should we prepare table even if there are no new messages.
            bool should_prepare_tables_anyway = false;

            {
                std::unique_lock lock(mutex);
                flush_event.wait_for(lock,
                    std::chrono::milliseconds(flush_interval_milliseconds),
                    [&] ()
                    {
                        return requested_flush_up_to > flushed_up_to || is_shutdown || is_force_prepare_tables;
                    }
                );

                queue_front_index += queue.size();
                to_flush_end = queue_front_index;
                // Swap with existing array from previous flush, to save memory
                // allocations.
                to_flush.resize(0);
                queue.swap(to_flush);

                should_prepare_tables_anyway = is_force_prepare_tables;

                exit_this_thread = is_shutdown;
            }

            if (to_flush.empty())
            {
                if (should_prepare_tables_anyway)
                {
                    prepareTable();
                    LOG_TRACE(log, "Table created (force)");

                    std::lock_guard lock(mutex);
                    is_force_prepare_tables = false;
                    flush_event.notify_all();
                }
            }
            else
            {
                flushImpl(to_flush, to_flush_end);
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
    LOG_TRACE(log, "Terminating");
}


template <typename LogElement>
void SystemLog<LogElement>::flushImpl(const std::vector<LogElement> & to_flush, uint64_t to_flush_end)
{
    try
    {
        LOG_TRACE(log, "Flushing system log, {} entries to flush up to offset {}",
            to_flush.size(), to_flush_end);

        /// We check for existence of the table and create it as needed at every
        /// flush. This is done to allow user to drop the table at any moment
        /// (new empty table will be created automatically). BTW, flush method
        /// is called from single thread.
        prepareTable();

        ColumnsWithTypeAndName log_element_columns;
        auto log_element_names_and_types = LogElement::getNamesAndTypes();

        for (auto name_and_type : log_element_names_and_types)
            log_element_columns.emplace_back(name_and_type.type, name_and_type.name);

        Block block(std::move(log_element_columns));

        MutableColumns columns = block.mutateColumns();
        for (const auto & elem : to_flush)
            elem.appendToBlock(columns);

        block.setColumns(std::move(columns));

        /// We write to table indirectly, using InterpreterInsertQuery.
        /// This is needed to support DEFAULT-columns in table.

        std::unique_ptr<ASTInsertQuery> insert = std::make_unique<ASTInsertQuery>();
        insert->table_id = table_id;
        ASTPtr query_ptr(insert.release());

        // we need query context to do inserts to target table with MV containing subqueries or joins
        auto insert_context = Context::createCopy(context);
        insert_context->makeQueryContext();

        InterpreterInsertQuery interpreter(query_ptr, insert_context);
        BlockIO io = interpreter.execute();

        io.out->writePrefix();
        io.out->write(block);
        io.out->writeSuffix();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    {
        std::lock_guard lock(mutex);
        flushed_up_to = to_flush_end;
        is_force_prepare_tables = false;
        flush_event.notify_all();
    }

    LOG_TRACE(log, "Flushed system log up to offset {}", to_flush_end);
}


template <typename LogElement>
void SystemLog<LogElement>::prepareTable()
{
    String description = table_id.getNameForLogs();

    table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());

    if (table)
    {
        auto metadata_columns = table->getInMemoryMetadataPtr()->getColumns();
        auto old_query = InterpreterCreateQuery::formatColumns(metadata_columns);

        auto ordinary_columns = LogElement::getNamesAndTypes();
        auto alias_columns = LogElement::getNamesAndAliases();
        auto current_query = InterpreterCreateQuery::formatColumns(ordinary_columns, alias_columns);

        if (serializeAST(*old_query) != serializeAST(*current_query))
        {
            /// Rename the existing table.
            int suffix = 0;
            while (DatabaseCatalog::instance().isTableExist(
                {table_id.database_name, table_id.table_name + "_" + toString(suffix)}, getContext()))
                ++suffix;

            auto rename = std::make_shared<ASTRenameQuery>();

            ASTRenameQuery::Table from;
            from.database = table_id.database_name;
            from.table = table_id.table_name;

            ASTRenameQuery::Table to;
            to.database = table_id.database_name;
            to.table = table_id.table_name + "_" + toString(suffix);

            ASTRenameQuery::Element elem;
            elem.from = from;
            elem.to = to;

            rename->elements.emplace_back(elem);

            LOG_DEBUG(
                log,
                "Existing table {} for system log has obsolete or different structure. Renaming it to {}",
                description,
                backQuoteIfNeed(to.table));

            auto query_context = Context::createCopy(context);
            query_context->makeQueryContext();
            InterpreterRenameQuery(rename, query_context).execute();

            /// The required table will be created.
            table = nullptr;
        }
        else if (!is_prepared)
            LOG_DEBUG(log, "Will use existing table {} for {}", description, LogElement::name());
    }

    if (!table)
    {
        /// Create the table.
        LOG_DEBUG(log, "Creating new table {} for {}", description, LogElement::name());

        auto create = getCreateTableQuery();


        auto query_context = Context::createCopy(context);
        query_context->makeQueryContext();

        InterpreterCreateQuery interpreter(create, query_context);
        interpreter.setInternal(true);
        interpreter.execute();

        table = DatabaseCatalog::instance().getTable(table_id, getContext());
    }

    is_prepared = true;
}


template <typename LogElement>
ASTPtr SystemLog<LogElement>::getCreateTableQuery()
{
    auto create = std::make_shared<ASTCreateQuery>();

    create->database = table_id.database_name;
    create->table = table_id.table_name;

    auto ordinary_columns = LogElement::getNamesAndTypes();
    auto alias_columns = LogElement::getNamesAndAliases();
    auto new_columns_list = std::make_shared<ASTColumns>();
    new_columns_list->set(new_columns_list->columns, InterpreterCreateQuery::formatColumns(ordinary_columns, alias_columns));
    create->set(create->columns_list, new_columns_list);

    ParserStorage storage_parser;
    ASTPtr storage_ast = parseQuery(
        storage_parser, storage_def.data(), storage_def.data() + storage_def.size(),
        "Storage to create table for " + LogElement::name(), 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
    create->set(create->storage, storage_ast);

    return create;
}

}
