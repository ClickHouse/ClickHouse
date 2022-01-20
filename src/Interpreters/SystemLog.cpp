#include <Interpreters/AsynchronousMetricLog.h>
#include <Interpreters/CrashLog.h>
#include <Interpreters/MetricLog.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/QueryViewsLog.h>
#include <Interpreters/SessionLog.h>
#include <Interpreters/TextLog.h>
#include <Interpreters/TraceLog.h>
#include <Interpreters/ZooKeeperLog.h>
#include <Interpreters/TransactionsInfoLog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTInsertQuery.h>
#include <Storages/IStorage.h>
#include <Common/setThreadName.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <IO/WriteHelpers.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <base/logger_useful.h>
#include <base/scope_guard.h>


#define DBMS_SYSTEM_LOG_QUEUE_SIZE 1048576

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int TIMEOUT_EXCEEDED;
    extern const int LOGICAL_ERROR;
}

namespace
{

constexpr size_t DEFAULT_SYSTEM_LOG_FLUSH_INTERVAL_MILLISECONDS = 7500;
constexpr size_t DEFAULT_METRIC_LOG_COLLECT_INTERVAL_MILLISECONDS = 1000;

/// Creates a system log with MergeTree engine using parameters from config
template <typename TSystemLog>
std::shared_ptr<TSystemLog> createSystemLog(
    ContextPtr context,
    const String & default_database_name,
    const String & default_table_name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix)
{
    if (!config.has(config_prefix))
    {
        LOG_DEBUG(&Poco::Logger::get("SystemLog"),
                "Not creating {}.{} since corresponding section '{}' is missing from config",
                default_database_name, default_table_name, config_prefix);

        return {};
    }

    String database = config.getString(config_prefix + ".database", default_database_name);
    String table = config.getString(config_prefix + ".table", default_table_name);

    if (database != default_database_name)
    {
        /// System tables must be loaded before other tables, but loading order is undefined for all databases except `system`
        LOG_ERROR(&Poco::Logger::get("SystemLog"), "Custom database name for a system table specified in config."
            " Table `{}` will be created in `system` database instead of `{}`", table, database);
        database = default_database_name;
    }

    String engine;
    if (config.has(config_prefix + ".engine"))
    {
        if (config.has(config_prefix + ".partition_by"))
            throw Exception("If 'engine' is specified for system table, "
                "PARTITION BY parameters should be specified directly inside 'engine' and 'partition_by' setting doesn't make sense",
                ErrorCodes::BAD_ARGUMENTS);
        if (config.has(config_prefix + ".ttl"))
            throw Exception("If 'engine' is specified for system table, "
                            "TTL parameters should be specified directly inside 'engine' and 'ttl' setting doesn't make sense",
                            ErrorCodes::BAD_ARGUMENTS);
        engine = config.getString(config_prefix + ".engine");
    }
    else
    {
        String partition_by = config.getString(config_prefix + ".partition_by", "toYYYYMM(event_date)");
        engine = "ENGINE = MergeTree";
        if (!partition_by.empty())
            engine += " PARTITION BY (" + partition_by + ")";
        String ttl = config.getString(config_prefix + ".ttl", "");
        if (!ttl.empty())
            engine += " TTL " + ttl;
        engine += " ORDER BY (event_date, event_time)";
    }
    // Validate engine definition grammatically to prevent some configuration errors
    ParserStorage storage_parser;
    parseQuery(storage_parser, engine.data(), engine.data() + engine.size(),
            "Storage to create table for " + config_prefix, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

    size_t flush_interval_milliseconds = config.getUInt64(config_prefix + ".flush_interval_milliseconds",
                                                          DEFAULT_SYSTEM_LOG_FLUSH_INTERVAL_MILLISECONDS);

    return std::make_shared<TSystemLog>(context, database, table, engine, flush_interval_milliseconds);
}

}


///
/// ISystemLog
///
ASTPtr ISystemLog::getCreateTableQueryClean(const StorageID & table_id, ContextPtr context)
{
    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    ASTPtr old_ast = database->getCreateTableQuery(table_id.table_name, context);
    auto & old_create_query_ast = old_ast->as<ASTCreateQuery &>();
    /// Reset UUID
    old_create_query_ast.uuid = UUIDHelpers::Nil;
    /// Existing table has default settings (i.e. `index_granularity = 8192`), reset them.
    if (ASTStorage * storage = old_create_query_ast.storage)
    {
        storage->reset(storage->settings);
    }
    return old_ast;
}

void ISystemLog::stopFlushThread()
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

void ISystemLog::startup()
{
    std::lock_guard lock(mutex);
    saving_thread = ThreadFromGlobalPool([this] { savingThreadFunction(); });
}


///
/// SystemLogs
///
SystemLogs::SystemLogs(ContextPtr global_context, const Poco::Util::AbstractConfiguration & config)
{
    query_log = createSystemLog<QueryLog>(global_context, "system", "query_log", config, "query_log");
    query_thread_log = createSystemLog<QueryThreadLog>(global_context, "system", "query_thread_log", config, "query_thread_log");
    part_log = createSystemLog<PartLog>(global_context, "system", "part_log", config, "part_log");
    trace_log = createSystemLog<TraceLog>(global_context, "system", "trace_log", config, "trace_log");
    crash_log = createSystemLog<CrashLog>(global_context, "system", "crash_log", config, "crash_log");
    text_log = createSystemLog<TextLog>(global_context, "system", "text_log", config, "text_log");
    metric_log = createSystemLog<MetricLog>(global_context, "system", "metric_log", config, "metric_log");
    asynchronous_metric_log = createSystemLog<AsynchronousMetricLog>(
        global_context, "system", "asynchronous_metric_log", config,
        "asynchronous_metric_log");
    opentelemetry_span_log = createSystemLog<OpenTelemetrySpanLog>(
        global_context, "system", "opentelemetry_span_log", config,
        "opentelemetry_span_log");
    query_views_log = createSystemLog<QueryViewsLog>(global_context, "system", "query_views_log", config, "query_views_log");
    zookeeper_log = createSystemLog<ZooKeeperLog>(global_context, "system", "zookeeper_log", config, "zookeeper_log");
    session_log = createSystemLog<SessionLog>(global_context, "system", "session_log", config, "session_log");
    transactions_info_log = createSystemLog<TransactionsInfoLog>(
        global_context, "system", "transactions_info_log", config, "transactions_info_log");

    if (query_log)
        logs.emplace_back(query_log.get());
    if (query_thread_log)
        logs.emplace_back(query_thread_log.get());
    if (part_log)
        logs.emplace_back(part_log.get());
    if (trace_log)
        logs.emplace_back(trace_log.get());
    if (crash_log)
        logs.emplace_back(crash_log.get());
    if (text_log)
        logs.emplace_back(text_log.get());
    if (metric_log)
        logs.emplace_back(metric_log.get());
    if (asynchronous_metric_log)
        logs.emplace_back(asynchronous_metric_log.get());
    if (opentelemetry_span_log)
        logs.emplace_back(opentelemetry_span_log.get());
    if (query_views_log)
        logs.emplace_back(query_views_log.get());
    if (zookeeper_log)
        logs.emplace_back(zookeeper_log.get());
    if (session_log)
        logs.emplace_back(session_log.get());
    if (transactions_info_log)
        logs.emplace_back(transactions_info_log.get());

    try
    {
        for (auto & log : logs)
            log->startup();
    }
    catch (...)
    {
        /// join threads
        shutdown();
        throw;
    }

    if (metric_log)
    {
        size_t collect_interval_milliseconds = config.getUInt64("metric_log.collect_interval_milliseconds",
                                                                DEFAULT_METRIC_LOG_COLLECT_INTERVAL_MILLISECONDS);
        metric_log->startCollectMetric(collect_interval_milliseconds);
    }

    if (crash_log)
    {
        CrashLog::initialize(crash_log);
    }
}


SystemLogs::~SystemLogs()
{
    shutdown();
}

void SystemLogs::shutdown()
{
    for (auto & log : logs)
        log->shutdown();
}

///
/// SystemLog
///
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
    , create_query(serializeAST(*getCreateTableQuery()))
    , flush_interval_milliseconds(flush_interval_milliseconds_)
{
    assert(database_name_ == DatabaseCatalog::SYSTEM_DATABASE);
    log = &Poco::Logger::get("SystemLog (" + database_name_ + "." + table_name_ + ")");
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
    MemoryTrackerBlockerInThread temporarily_disable_memory_tracker(VariableContext::Global);

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
void SystemLog<LogElement>::shutdown()
{
    stopFlushThread();

    auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
    if (table)
        table->flushAndShutdown();
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

        for (const auto & name_and_type : log_element_names_and_types)
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

        PushingPipelineExecutor executor(io.pipeline);

        executor.start();
        executor.push(block);
        executor.finish();
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

    auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
    if (table)
    {
        if (old_create_query.empty())
        {
            old_create_query = serializeAST(*getCreateTableQueryClean(table_id, getContext()));
            if (old_create_query.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Empty CREATE QUERY for {}", backQuoteIfNeed(table_id.table_name));
        }

        if (old_create_query != create_query)
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
                "Existing table {} for system log has obsolete or different structure. Renaming it to {}.\nOld: {}\nNew: {}\n.",
                description,
                backQuoteIfNeed(to.table),
                old_create_query,
                create_query);

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

        auto query_context = Context::createCopy(context);
        query_context->makeQueryContext();

        auto create_query_ast = getCreateTableQuery();
        InterpreterCreateQuery interpreter(create_query_ast, query_context);
        interpreter.setInternal(true);
        interpreter.execute();

        table = DatabaseCatalog::instance().getTable(table_id, getContext());

        old_create_query.clear();
    }

    is_prepared = true;
}


template <typename LogElement>
ASTPtr SystemLog<LogElement>::getCreateTableQuery()
{
    auto create = std::make_shared<ASTCreateQuery>();

    create->setDatabase(table_id.database_name);
    create->setTable(table_id.table_name);

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

template class SystemLog<AsynchronousMetricLogElement>;
template class SystemLog<CrashLogElement>;
template class SystemLog<MetricLogElement>;
template class SystemLog<OpenTelemetrySpanLogElement>;
template class SystemLog<PartLogElement>;
template class SystemLog<QueryLogElement>;
template class SystemLog<QueryThreadLogElement>;
template class SystemLog<QueryViewsLogElement>;
template class SystemLog<SessionLogElement>;
template class SystemLog<TraceLogElement>;
template class SystemLog<ZooKeeperLogElement>;
template class SystemLog<TextLogElement>;
template class SystemLog<TransactionsInfoLogElement>;

}
