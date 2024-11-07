#include <Interpreters/SystemLog.h>

#include <base/scope_guard.h>
#include <Common/SystemLogBase.h>
#include <Common/logger_useful.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/quoteString.h>
#include <Common/setThreadName.h>
#include <Core/ServerSettings.h>
#include <Interpreters/AsynchronousInsertLog.h>
#include <Interpreters/AsynchronousMetricLog.h>
#include <Interpreters/BackupLog.h>
#include <Interpreters/BlobStorageLog.h>
#include <Interpreters/Context.h>
#include <Interpreters/CrashLog.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ErrorLog.h>
#include <Interpreters/FilesystemCacheLog.h>
#include <Interpreters/FilesystemReadPrefetchesLog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterRenameQuery.h>
#include <Interpreters/MetricLog.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/QueryMetricLog.h>
#include <Interpreters/QueryThreadLog.h>
#include <Interpreters/QueryViewsLog.h>
#include <Interpreters/ObjectStorageQueueLog.h>
#include <Interpreters/SessionLog.h>
#include <Interpreters/TextLog.h>
#include <Interpreters/TraceLog.h>
#include <Interpreters/TransactionsInfoLog.h>
#include <Interpreters/ZooKeeperLog.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

#include <fmt/core.h>


namespace DB
{

namespace ServerSetting
{
    extern const ServerSettingsBool prepare_system_log_tables_on_startup;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace ActionLocks
{
    extern const StorageActionBlockType PartsMerge;
}

namespace
{
    class StorageWithComment : public IAST
    {
    public:
        ASTPtr storage;
        ASTPtr comment;

        String getID(char) const override { return "Storage with comment definition"; }

        ASTPtr clone() const override
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method clone is not supported");
        }

        void formatImpl(const FormatSettings &, FormatState &, FormatStateStacked) const override
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method formatImpl is not supported");
        }
    };

    class ParserStorageWithComment : public IParserBase
    {
    protected:
        const char * getName() const override { return "storage definition with comment"; }
        bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override
        {
            ParserStorage storage_p{ParserStorage::TABLE_ENGINE};
            ASTPtr storage;

            if (!storage_p.parse(pos, storage, expected))
                return false;

            ParserKeyword s_comment(Keyword::COMMENT);
            ParserStringLiteral string_literal_parser;
            ASTPtr comment;

            if (s_comment.ignore(pos, expected))
                string_literal_parser.parse(pos, comment, expected);

            auto storage_with_comment = std::make_shared<StorageWithComment>();
            storage_with_comment->storage = std::move(storage);
            storage_with_comment->comment = std::move(comment);

            node = storage_with_comment;
            return true;
        }
    };
}

namespace
{

constexpr size_t DEFAULT_METRIC_LOG_COLLECT_INTERVAL_MILLISECONDS = 1000;
constexpr size_t DEFAULT_ERROR_LOG_COLLECT_INTERVAL_MILLISECONDS = 1000;

/// Creates a system log with MergeTree engine using parameters from config
template <typename TSystemLog>
std::shared_ptr<TSystemLog> createSystemLog(
    ContextPtr context,
    const String & default_database_name,
    const String & default_table_name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    const String & comment)
{
    if (!config.has(config_prefix))
    {
        LOG_DEBUG(getLogger("SystemLog"),
                "Not creating {}.{} since corresponding section '{}' is missing from config",
                default_database_name, default_table_name, config_prefix);

        return {};
    }
    LOG_DEBUG(getLogger("SystemLog"),
              "Creating {}.{} from {}", default_database_name, default_table_name, config_prefix);

    SystemLogSettings log_settings;

    log_settings.queue_settings.database = config.getString(config_prefix + ".database", default_database_name);
    log_settings.queue_settings.table = config.getString(config_prefix + ".table", default_table_name);

    if (log_settings.queue_settings.database != default_database_name)
    {
        /// System tables must be loaded before other tables, but loading order is undefined for all databases except `system`
        LOG_ERROR(
            getLogger("SystemLog"),
            "Custom database name for a system table specified in config."
            " Table `{}` will be created in `system` database instead of `{}`",
            log_settings.queue_settings.table,
            log_settings.queue_settings.database);

        log_settings.queue_settings.database = default_database_name;
    }

    if (config.has(config_prefix + ".engine"))
    {
        if (config.has(config_prefix + ".partition_by"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "If 'engine' is specified for system table, PARTITION BY parameters should "
                            "be specified directly inside 'engine' and 'partition_by' setting doesn't make sense");
        if (config.has(config_prefix + ".ttl"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "If 'engine' is specified for system table, TTL parameters should "
                            "be specified directly inside 'engine' and 'ttl' setting doesn't make sense");
        if (config.has(config_prefix + ".order_by"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "If 'engine' is specified for system table, ORDER BY parameters should "
                            "be specified directly inside 'engine' and 'order_by' setting doesn't make sense");
        if (config.has(config_prefix + ".storage_policy"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "If 'engine' is specified for system table, SETTINGS storage_policy = '...' should "
                            "be specified directly inside 'engine' and 'storage_policy' setting doesn't make sense");
        if (config.has(config_prefix + ".settings"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                            "If 'engine' is specified for system table, SETTINGS parameters should "
                            "be specified directly inside 'engine' and 'settings' setting doesn't make sense");

        log_settings.engine = config.getString(config_prefix + ".engine");
    }
    else
    {
        /// ENGINE expr is necessary.
        log_settings.engine = "ENGINE = MergeTree";

        /// PARTITION expr is not necessary.
        String partition_by = config.getString(config_prefix + ".partition_by", "toYYYYMM(event_date)");
        if (!partition_by.empty())
            log_settings.engine += " PARTITION BY (" + partition_by + ")";

        /// TTL expr is not necessary.
        String ttl = config.getString(config_prefix + ".ttl", "");
        if (!ttl.empty())
            log_settings.engine += " TTL " + ttl;

        /// ORDER BY expr is necessary.
        String order_by = config.getString(config_prefix + ".order_by", TSystemLog::getDefaultOrderBy());
        log_settings.engine += " ORDER BY (" + order_by + ")";

        /// SETTINGS expr is not necessary.
        ///   https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree#settings
        ///
        /// STORAGE POLICY expr is retained for backward compatible.
        String storage_policy = config.getString(config_prefix + ".storage_policy", "");
        String settings = config.getString(config_prefix + ".settings", "");
        if (!storage_policy.empty() || !settings.empty())
        {
            log_settings.engine += " SETTINGS";
            /// If 'storage_policy' is repeated, the 'settings' configuration is preferred.
            if (!storage_policy.empty())
                log_settings.engine += " storage_policy = " + quoteString(storage_policy);
            if (!settings.empty())
                log_settings.engine += (storage_policy.empty() ? " " : ", ") + settings;
        }
    }

    /// Validate engine definition syntax to prevent some configuration errors.
    ParserStorageWithComment storage_parser;
    auto storage_ast = parseQuery(storage_parser, log_settings.engine.data(), log_settings.engine.data() + log_settings.engine.size(),
            "Storage to create table for " + config_prefix, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    auto & storage_with_comment = storage_ast->as<StorageWithComment &>();

    /// Add comment to AST. So it will be saved when the table will be renamed.
    if (!storage_with_comment.comment || storage_with_comment.comment->as<ASTLiteral &>().value.safeGet<String>().empty())
        log_settings.engine += fmt::format(" COMMENT {} ", quoteString(comment));

    log_settings.queue_settings.flush_interval_milliseconds = config.getUInt64(config_prefix + ".flush_interval_milliseconds",
                                                                               TSystemLog::getDefaultFlushIntervalMilliseconds());

    log_settings.queue_settings.max_size_rows = config.getUInt64(config_prefix + ".max_size_rows",
                                                                 TSystemLog::getDefaultMaxSize());

    if (log_settings.queue_settings.max_size_rows < 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{0}.max_size_rows {1} should be 1 at least",
                        config_prefix,
                        log_settings.queue_settings.max_size_rows);

    log_settings.queue_settings.reserved_size_rows = config.getUInt64(config_prefix + ".reserved_size_rows",
                                                                      TSystemLog::getDefaultReservedSize());

    if (log_settings.queue_settings.max_size_rows < log_settings.queue_settings.reserved_size_rows)
    {
         throw Exception(ErrorCodes::BAD_ARGUMENTS,
                         "{0}.max_size_rows {1} should be greater or equal to {0}.reserved_size_rows {2}",
                         config_prefix,
                         log_settings.queue_settings.max_size_rows,
                         log_settings.queue_settings.reserved_size_rows);
    }

    log_settings.queue_settings.buffer_size_rows_flush_threshold = config.getUInt64(config_prefix + ".buffer_size_rows_flush_threshold",
                                                                                    log_settings.queue_settings.max_size_rows / 2);

    log_settings.queue_settings.notify_flush_on_crash = config.getBool(config_prefix + ".flush_on_crash",
                                                                       TSystemLog::shouldNotifyFlushOnCrash());

    log_settings.queue_settings.turn_off_logger = TSystemLog::shouldTurnOffLogger();

    return std::make_shared<TSystemLog>(context, log_settings);
}


/// returns CREATE TABLE query, but with removed UUID
/// That way it can be used to compare with the SystemLog::getCreateTableQuery()
ASTPtr getCreateTableQueryClean(const StorageID & table_id, ContextPtr context)
{
    DatabasePtr database = DatabaseCatalog::instance().getDatabase(table_id.database_name);
    ASTPtr old_ast = database->getCreateTableQuery(table_id.table_name, context);
    auto & old_create_query_ast = old_ast->as<ASTCreateQuery &>();
    /// Reset UUID
    old_create_query_ast.uuid = UUIDHelpers::Nil;
    return old_ast;
}

}


SystemLogs::SystemLogs(ContextPtr global_context, const Poco::Util::AbstractConfiguration & config)
{
/// NOLINTBEGIN(bugprone-macro-parentheses)
#define CREATE_PUBLIC_MEMBERS(log_type, member, descr) \
    member = createSystemLog<log_type>(global_context, "system", #member, config, #member, descr); \

    LIST_OF_ALL_SYSTEM_LOGS(CREATE_PUBLIC_MEMBERS)
#undef CREATE_PUBLIC_MEMBERS
/// NOLINTEND(bugprone-macro-parentheses)

    bool should_prepare = global_context->getServerSettings()[ServerSetting::prepare_system_log_tables_on_startup];
    try
    {
        for (auto & log : getAllLogs())
        {
            log->startup();
            if (should_prepare)
                log->prepareTable();
        }
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
        metric_log->startCollect("MetricLog", collect_interval_milliseconds);
    }

    if (error_log)
    {
        size_t collect_interval_milliseconds = config.getUInt64("error_log.collect_interval_milliseconds",
                                                                DEFAULT_ERROR_LOG_COLLECT_INTERVAL_MILLISECONDS);
        error_log->startCollect("ErrorLog", collect_interval_milliseconds);
    }

    if (crash_log)
    {
        CrashLog::initialize(crash_log);
    }
}

std::vector<ISystemLog *> SystemLogs::getAllLogs() const
{
#define GET_RAW_POINTERS(log_type, member, descr) \
    (member).get(), \

    std::vector<ISystemLog *> result = {
        LIST_OF_ALL_SYSTEM_LOGS(GET_RAW_POINTERS)
    };
#undef GET_RAW_POINTERS

    auto last_it = std::remove(result.begin(), result.end(), nullptr);
    result.erase(last_it, result.end());

    return result;
}

void SystemLogs::flush(bool should_prepare_tables_anyway)
{
    auto logs = getAllLogs();
    std::vector<ISystemLog::Index> logs_indexes(logs.size(), 0);

    for (size_t i = 0; i < logs.size(); ++i)
    {
        auto last_log_index = logs[i]->getLastLogIndex();
        logs_indexes[i] = last_log_index;
        logs[i]->notifyFlush(last_log_index, should_prepare_tables_anyway);
    }

    for (size_t i = 0; i < logs.size(); ++i)
        logs[i]->flush(logs_indexes[i], should_prepare_tables_anyway);
}

void SystemLogs::flushAndShutdown()
{
    flush(/* should_prepare_tables_anyway */ false);
    shutdown();
}

void SystemLogs::shutdown()
{
    auto logs = getAllLogs();
    for (auto & log : logs)
        log->shutdown();
}

void SystemLogs::handleCrash()
{
    auto logs = getAllLogs();
    for (auto & log : logs)
        log->handleCrash();
}

template <typename LogElement>
SystemLog<LogElement>::SystemLog(
    ContextPtr context_,
    const SystemLogSettings & settings_,
    std::shared_ptr<SystemLogQueue<LogElement>> queue_)
    : Base(settings_.queue_settings, queue_)
    , WithContext(context_)
    , log(getLogger("SystemLog (" + settings_.queue_settings.database + "." + settings_.queue_settings.table + ")"))
    , table_id(settings_.queue_settings.database, settings_.queue_settings.table)
    , storage_def(settings_.engine)
    , create_query(serializeAST(*getCreateTableQuery()))
{
    assert(settings_.queue_settings.database == DatabaseCatalog::SYSTEM_DATABASE);
}

template <typename LogElement>
void SystemLog<LogElement>::shutdown()
{
    Base::stopFlushThread();

    auto table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
    if (table)
        table->flushAndShutdown();
}


template <typename LogElement>
void SystemLog<LogElement>::savingThreadFunction()
{
    setThreadName("SystemLogFlush");

    while (true)
    {
        try
        {
            auto result = queue->pop();

            if (result.is_shutdown)
            {
                LOG_TRACE(log, "Terminating");
                return;
            }

            if (!result.logs.empty())
            {
                flushImpl(result.logs, result.last_log_index);
            }
            else if (result.create_table_force)
            {
                prepareTable();
                queue->confirm(result.last_log_index);
            }
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
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
        auto log_element_names_and_types = LogElement::getColumnsDescription();

        for (const auto & name_and_type : log_element_names_and_types.getAll())
            log_element_columns.emplace_back(name_and_type.type, name_and_type.name);

        Block block(std::move(log_element_columns));

        MutableColumns columns = block.mutateColumns();

        for (auto & column : columns)
            column->reserve(to_flush.size());

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
        addSettingsForQuery(insert_context, IAST::QueryKind::Insert);

        InterpreterInsertQuery interpreter(
            query_ptr,
            insert_context,
            /* allow_materialized */ false,
            /* no_squash */ false,
            /* no_destination */ false,
            /* async_isnert */ false);
        BlockIO io = interpreter.execute();

        PushingPipelineExecutor executor(io.pipeline);

        executor.start();
        executor.push(block);
        executor.finish();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, fmt::format("Failed to flush system log {} with {} entries up to offset {}",
            table_id.getNameForLogs(), to_flush.size(), to_flush_end));
    }

    queue->confirm(to_flush_end);

    LOG_TRACE(log, "Flushed system log up to offset {}", to_flush_end);
}

template <typename LogElement>
StoragePtr SystemLog<LogElement>::getStorage() const
{
    return DatabaseCatalog::instance().tryGetTable(table_id, getContext());
}

template <typename LogElement>
void SystemLog<LogElement>::prepareTable()
{
    String description = table_id.getNameForLogs();

    auto table = getStorage();
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
            /// TODO: Handle altering comment, because otherwise all table will be renamed.

            /// Rename the existing table.
            int suffix = 0;
            while (DatabaseCatalog::instance().isTableExist(
                {table_id.database_name, table_id.table_name + "_" + toString(suffix)}, getContext()))
                ++suffix;

            ASTRenameQuery::Element elem
            {
                ASTRenameQuery::Table
                {
                    table_id.database_name.empty() ? nullptr : std::make_shared<ASTIdentifier>(table_id.database_name),
                    std::make_shared<ASTIdentifier>(table_id.table_name)
                },
                ASTRenameQuery::Table
                {
                    table_id.database_name.empty() ? nullptr : std::make_shared<ASTIdentifier>(table_id.database_name),
                    std::make_shared<ASTIdentifier>(table_id.table_name + "_" + toString(suffix))
                }
            };

            LOG_DEBUG(
                log,
                "Existing table {} for system log has obsolete or different structure. Renaming it to {}.\nOld: {}\nNew: {}\n.",
                description,
                backQuoteIfNeed(elem.to.getTable()),
                old_create_query,
                create_query);

            auto rename = std::make_shared<ASTRenameQuery>(ASTRenameQuery::Elements{std::move(elem)});

            ActionLock merges_lock;
            if (DatabaseCatalog::instance().getDatabase(table_id.database_name)->getUUID() == UUIDHelpers::Nil)
                merges_lock = table->getActionLock(ActionLocks::PartsMerge);

            auto query_context = Context::createCopy(context);
            query_context->makeQueryContext();
            addSettingsForQuery(query_context, IAST::QueryKind::Rename);

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
        addSettingsForQuery(query_context, IAST::QueryKind::Create);

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
void SystemLog<LogElement>::addSettingsForQuery(ContextMutablePtr & mutable_context, IAST::QueryKind query_kind) const
{
    if (query_kind == IAST::QueryKind::Insert)
    {
        /// We always want to deliver the data to the original table regardless of the MVs
        mutable_context->setSetting("materialized_views_ignore_errors", true);
    }
    else if (query_kind == IAST::QueryKind::Rename)
    {
        /// As this operation is performed automatically we don't want it to fail because of user dependencies on log tables
        mutable_context->setSetting("check_table_dependencies", Field{false});
        mutable_context->setSetting("check_referential_table_dependencies", Field{false});
    }
}

template <typename LogElement>
ASTPtr SystemLog<LogElement>::getCreateTableQuery()
{
    auto create = std::make_shared<ASTCreateQuery>();

    create->setDatabase(table_id.database_name);
    create->setTable(table_id.table_name);

    auto new_columns_list = std::make_shared<ASTColumns>();
    auto ordinary_columns = LogElement::getColumnsDescription();
    auto alias_columns = LogElement::getNamesAndAliases();
    ordinary_columns.setAliases(alias_columns);

    new_columns_list->set(new_columns_list->columns, InterpreterCreateQuery::formatColumns(ordinary_columns));

    create->set(create->columns_list, new_columns_list);

    ParserStorageWithComment storage_parser;

    ASTPtr storage_with_comment_ast = parseQuery(
        storage_parser, storage_def.data(), storage_def.data() + storage_def.size(),
        "Storage to create table for " + LogElement::name(), 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);

    StorageWithComment & storage_with_comment = storage_with_comment_ast->as<StorageWithComment &>();

    create->set(create->storage, storage_with_comment.storage);
    create->set(create->comment, storage_with_comment.comment);

    /// Write additional (default) settings for MergeTree engine to make it make it possible to compare ASTs
    /// and recreate tables on settings changes.
    const auto & engine = create->storage->engine->as<ASTFunction &>();
    if (endsWith(engine.name, "MergeTree"))
    {
        auto storage_settings = std::make_unique<MergeTreeSettings>(getContext()->getMergeTreeSettings());
        storage_settings->loadFromQuery(*create->storage, getContext(), false);
    }

    return create;
}

#define INSTANTIATE_SYSTEM_LOG(ELEMENT) template class SystemLog<ELEMENT>;
SYSTEM_LOG_ELEMENTS(INSTANTIATE_SYSTEM_LOG)

}
