#pragma once

#include <thread>
#include <boost/noncopyable.hpp>
#include <common/logger_useful.h>
#include <Core/Types.h>
#include <Common/ConcurrentBoundedQueue.h>
#include <Storages/IStorage.h>
#include <Interpreters/Context.h>
#include <Common/Stopwatch.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTInsertQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Common/setThreadName.h>
#include <IO/WriteHelpers.h>
#include <common/logger_useful.h>
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
        static Block createBlock();
        void appendToBlock(Block & block) const;
    };
    */


#define DBMS_SYSTEM_LOG_QUEUE_SIZE 1048576

class Context;
class QueryLog;
class QueryThreadLog;
class PartLog;


/// System logs should be destroyed in destructor of the last Context and before tables,
///  because SystemLog destruction makes insert query while flushing data into underlying tables
struct SystemLogs
{
    ~SystemLogs();

    std::unique_ptr<QueryLog> query_log;                /// Used to log queries.
    std::unique_ptr<QueryThreadLog> query_thread_log;   /// Used to log query threads.
    std::unique_ptr<PartLog> part_log;                  /// Used to log operations with parts
};


template <typename LogElement>
class SystemLog : private boost::noncopyable
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
        Context & context_,
        const String & database_name_,
        const String & table_name_,
        const String & storage_def_,
        size_t flush_interval_milliseconds_);

    ~SystemLog();

    /** Append a record into log.
      * Writing to table will be done asynchronously and in case of failure, record could be lost.
      */
    void add(const LogElement & element)
    {
        /// Without try we could block here in case of queue overflow.
        if (!queue.tryPush({false, element}))
            LOG_ERROR(log, "SystemLog queue is full");
    }

    /// Flush data in the buffer to disk
    void flush(bool quiet = false);

protected:
    Context & context;
    const String database_name;
    const String table_name;
    const String storage_def;
    StoragePtr table;
    const size_t flush_interval_milliseconds;

    using QueueItem = std::pair<bool, LogElement>;        /// First element is shutdown flag for thread.

    /// Queue is bounded. But its size is quite large to not block in all normal cases.
    ConcurrentBoundedQueue<QueueItem> queue {DBMS_SYSTEM_LOG_QUEUE_SIZE};

    /** Data that was pulled from queue. Data is accumulated here before enough time passed.
      * It's possible to implement double-buffering, but we assume that insertion into table is faster
      *  than accumulation of large amount of log records (for example, for query log - processing of large amount of queries).
      */
    std::vector<LogElement> data;
    std::mutex data_mutex;

    Logger * log;

    /** In this thread, data is pulled from 'queue' and stored in 'data', and then written into table.
      */
    std::thread saving_thread;

    void threadFunction();

    /** Creates new table if it does not exist.
      * Renames old table if its structure is not suitable.
      * This cannot be done in constructor to avoid deadlock while renaming a table under locked Context when SystemLog object is created.
      */
    bool is_prepared = false;
    void prepareTable();
};


template <typename LogElement>
SystemLog<LogElement>::SystemLog(Context & context_,
    const String & database_name_,
    const String & table_name_,
    const String & storage_def_,
    size_t flush_interval_milliseconds_)
    : context(context_),
    database_name(database_name_), table_name(table_name_), storage_def(storage_def_),
    flush_interval_milliseconds(flush_interval_milliseconds_)
{
    log = &Logger::get("SystemLog (" + database_name + "." + table_name + ")");

    data.reserve(DBMS_SYSTEM_LOG_QUEUE_SIZE);
    saving_thread = std::thread([this] { threadFunction(); });
}


template <typename LogElement>
SystemLog<LogElement>::~SystemLog()
{
    /// Tell thread to shutdown.
    queue.push({true, {}});
    saving_thread.join();
}


template <typename LogElement>
void SystemLog<LogElement>::threadFunction()
{
    setThreadName("SystemLogFlush");

    Stopwatch time_after_last_write;
    bool first = true;

    while (true)
    {
        try
        {
            if (first)
            {
                time_after_last_write.restart();
                first = false;
            }

            QueueItem element;
            bool has_element = false;

            bool is_empty;
            {
                std::unique_lock lock(data_mutex);
                is_empty = data.empty();
            }

            /// data.size() is increased only in this function
            /// TODO: get rid of data and queue duality

            if (is_empty)
            {
                queue.pop(element);
                has_element = true;
            }
            else
            {
                size_t milliseconds_elapsed = time_after_last_write.elapsed() / 1000000;
                if (milliseconds_elapsed < flush_interval_milliseconds)
                    has_element = queue.tryPop(element, flush_interval_milliseconds - milliseconds_elapsed);
            }

            if (has_element)
            {
                if (element.first)
                {
                    /// Shutdown.
                    /// NOTE: MergeTree engine can write data even it is already in shutdown state.
                    flush();
                    break;
                }
                else
                {
                    std::unique_lock lock(data_mutex);
                    data.push_back(element.second);
                }
            }

            size_t milliseconds_elapsed = time_after_last_write.elapsed() / 1000000;
            if (milliseconds_elapsed >= flush_interval_milliseconds)
            {
                /// Write data to a table.
                flush(true);
                time_after_last_write.restart();
            }
        }
        catch (...)
        {
            /// In case of exception we lost accumulated data - to avoid locking.
            data.clear();
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}


template <typename LogElement>
void SystemLog<LogElement>::flush(bool quiet)
{
    std::unique_lock lock(data_mutex);

    try
    {
        if (quiet && data.empty())
            return;

        LOG_TRACE(log, "Flushing system log");

        /// We check for existence of the table and create it as needed at every flush.
        /// This is done to allow user to drop the table at any moment (new empty table will be created automatically).
        /// BTW, flush method is called from single thread.
        prepareTable();

        Block block = LogElement::createBlock();
        for (const LogElement & elem : data)
            elem.appendToBlock(block);

        /// Clear queue early, because insertion to the table could lead to generation of more log entrites
        ///  and pushing them to already full queue will lead to deadlock.
        data.clear();

        /// We write to table indirectly, using InterpreterInsertQuery.
        /// This is needed to support DEFAULT-columns in table.

        std::unique_ptr<ASTInsertQuery> insert = std::make_unique<ASTInsertQuery>();
        insert->database = database_name;
        insert->table = table_name;
        ASTPtr query_ptr(insert.release());

        InterpreterInsertQuery interpreter(query_ptr, context);
        BlockIO io = interpreter.execute();

        io.out->writePrefix();
        io.out->write(block);
        io.out->writeSuffix();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        /// In case of exception, also clean accumulated data - to avoid locking.
        data.clear();
    }
}


template <typename LogElement>
void SystemLog<LogElement>::prepareTable()
{
    String description = backQuoteIfNeed(database_name) + "." + backQuoteIfNeed(table_name);

    table = context.tryGetTable(database_name, table_name);

    if (table)
    {
        const Block expected = LogElement::createBlock();
        const Block actual = table->getSampleBlockNonMaterialized();

        if (!blocksHaveEqualStructure(actual, expected))
        {
            /// Rename the existing table.
            int suffix = 0;
            while (context.isTableExist(database_name, table_name + "_" + toString(suffix)))
                ++suffix;

            auto rename = std::make_shared<ASTRenameQuery>();

            ASTRenameQuery::Table from;
            from.database = database_name;
            from.table = table_name;

            ASTRenameQuery::Table to;
            to.database = database_name;
            to.table = table_name + "_" + toString(suffix);

            ASTRenameQuery::Element elem;
            elem.from = from;
            elem.to = to;

            rename->elements.emplace_back(elem);

            LOG_DEBUG(log, "Existing table " << description << " for system log has obsolete or different structure."
            " Renaming it to " << backQuoteIfNeed(to.table));

            InterpreterRenameQuery(rename, context).execute();

            /// The required table will be created.
            table = nullptr;
        }
        else if (!is_prepared)
            LOG_DEBUG(log, "Will use existing table " << description << " for " + LogElement::name());
    }

    if (!table)
    {
        /// Create the table.
        LOG_DEBUG(log, "Creating new table " << description << " for " + LogElement::name());

        auto create = std::make_shared<ASTCreateQuery>();

        create->database = database_name;
        create->table = table_name;

        Block sample = LogElement::createBlock();
        create->set(create->columns, InterpreterCreateQuery::formatColumns(sample.getNamesAndTypesList()));

        ParserStorage storage_parser;
        ASTPtr storage_ast = parseQuery(
            storage_parser, storage_def.data(), storage_def.data() + storage_def.size(),
            "Storage to create table for " + LogElement::name(), 0);
        create->set(create->storage, storage_ast);

        InterpreterCreateQuery interpreter(create, context);
        interpreter.setInternal(true);
        interpreter.execute();

        table = context.getTable(database_name, table_name);
    }

    is_prepared = true;
}

/// Creates a system log with MergeTree engines using parameters from config
template<typename TSystemLog>
std::unique_ptr<TSystemLog> createDefaultSystemLog(
        Context & context_,
        const String & default_database_name,
        const String & default_table_name,
        Poco::Util::AbstractConfiguration & config,
        const String & config_prefix)
{
    String database     = config.getString(config_prefix + ".database",     default_database_name);
    String table        = config.getString(config_prefix + ".table",        default_table_name);
    String partition_by = config.getString(config_prefix + ".partition_by", "toYYYYMM(event_date)");
    String engine = "ENGINE = MergeTree PARTITION BY (" + partition_by + ") ORDER BY (event_date, event_time) SETTINGS index_granularity = 1024";

    size_t flush_interval_milliseconds = config.getUInt64("query_log.flush_interval_milliseconds", DEFAULT_QUERY_LOG_FLUSH_INTERVAL_MILLISECONDS);

    return std::make_unique<TSystemLog>(context_, database, table, engine, flush_interval_milliseconds);
}


}
