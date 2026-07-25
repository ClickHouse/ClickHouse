#include <Storages/Queue/StorageQueue.h>

#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/Defines.h>
#include <Core/Settings.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/StorageFactory.h>
#include <Storages/StreamingStorageRegistry.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <fmt/ranges.h>


namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_queue_table_engine;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_STORAGE;
}

namespace
{
    constexpr std::string_view ID_COLUMN = "_queue_id";
    constexpr std::string_view VERSION_COLUMN = "_queue_version";
    constexpr std::string_view IS_DELETED_COLUMN = "_queue_is_deleted";
    constexpr std::string_view CREATED_AT_COLUMN = "_queue_created_at";

    constexpr UInt64 DEFAULT_RETENTION_SECONDS = 7 * 24 * 60 * 60;
    constexpr UInt64 DEFAULT_MAX_BATCH_SIZE = 65536;
    constexpr UInt64 DEFAULT_POLLING_INTERVAL_MS = 100;

    ASTPtr parseCreateQuery(const String & query)
    {
        ParserCreateQuery parser;
        return parseQuery(
            parser,
            query,
            "internal Queue table definition",
            DBMS_DEFAULT_MAX_QUERY_SIZE,
            DBMS_DEFAULT_MAX_PARSER_DEPTH,
            DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    }

    String quoteTable(const StorageID & table_id)
    {
        return fmt::format(
            "{}.{}",
            backQuoteIfNeed(table_id.getDatabaseName()),
            backQuoteIfNeed(table_id.getTableName()));
    }

    ASTPtr makeInsertQuery(const StorageID & table_id, const Names & columns)
    {
        auto insert = make_intrusive<ASTInsertQuery>();
        insert->table_id = table_id;

        auto columns_ast = make_intrusive<ASTExpressionList>();
        for (const auto & name : columns)
            columns_ast->children.emplace_back(make_intrusive<ASTIdentifier>(name));
        insert->columns = columns_ast;
        return insert;
    }

    class QueueSink final : public SinkToStorage, WithContext
    {
    public:
        QueueSink(
            const StorageID & inner_table_id_,
            const Block & header_,
            ContextPtr context_,
            bool async_insert_)
            : SinkToStorage(std::make_shared<const Block>(header_))
            , WithContext(context_)
            , inner_table_id(inner_table_id_)
            , async_insert(async_insert_)
        {
        }

        ~QueueSink() override
        {
            if (executor)
            {
                try
                {
                    executor->cancel();
                }
                catch (...)
                {
                    tryLogCurrentException("QueueSink");
                }
            }
        }

        String getName() const override { return "QueueSink"; }

        void onStart() override
        {
            auto insert_context = Context::createCopy(getContext());
            insert_context->makeQueryContext();

            InterpreterInsertQuery interpreter(
                makeInsertQuery(inner_table_id, getHeader().getNames()),
                insert_context,
                /* allow_materialized */ true,
                /* no_squash */ false,
                /* no_destination */ false,
                async_insert);

            block_io = interpreter.execute();
            executor = std::make_unique<PushingPipelineExecutor>(block_io.pipeline);
            executor->start();
        }

        void consume(Chunk & chunk) override
        {
            if (!chunk.getNumRows())
                return;
            executor->push(getHeader().cloneWithColumns(chunk.detachColumns()));
        }

        void onFinish() override
        {
            executor->finish();
            executor.reset();
            block_io.onFinish();
            block_io = {};
        }

        void onException(std::exception_ptr) override
        {
            if (executor)
                executor->cancel();
            executor.reset();
            block_io.onException();
            block_io = {};
        }

    private:
        const StorageID inner_table_id;
        const bool async_insert;
        BlockIO block_io;
        std::unique_ptr<PushingPipelineExecutor> executor;
    };
}


String StorageQueue::getInnerTableName(const StorageID & queue_table_id)
{
    if (queue_table_id.hasUUID())
        return ".inner_id.queue." + toString(queue_table_id.uuid);
    return ".inner.queue." + queue_table_id.getTableName();
}


StorageID StorageQueue::createInnerTable(
    const ASTCreateQuery & outer_query,
    const StorageID & queue_table_id,
    ContextPtr local_context,
    LoadingStrictnessLevel mode,
    UInt64 retention_seconds)
{
    StorageID inner_table_id{
        queue_table_id.getDatabaseName(),
        getInnerTableName(queue_table_id)};

    if (mode > LoadingStrictnessLevel::SECONDARY_CREATE)
        return inner_table_id;

    const auto internal_definition = fmt::format(
        "CREATE TABLE queue_inner ("
        "`{}` UUID DEFAULT generateUUIDv4(), "
        "`{}` UInt64 DEFAULT 1, "
        "`{}` UInt8 DEFAULT 0, "
        "`{}` DateTime64(6) DEFAULT now64(6)) "
        "ENGINE = ReplacingMergeTree(`{}`, `{}`) "
        "PARTITION BY toYYYYMM(`{}`) "
        "ORDER BY `{}` "
        "TTL `{}` + toIntervalSecond({}) DELETE "
        "SETTINGS clean_deleted_rows = 'Always', allow_experimental_replacing_merge_with_cleanup = 1",
        ID_COLUMN,
        VERSION_COLUMN,
        IS_DELETED_COLUMN,
        CREATED_AT_COLUMN,
        VERSION_COLUMN,
        IS_DELETED_COLUMN,
        CREATED_AT_COLUMN,
        ID_COLUMN,
        CREATED_AT_COLUMN,
        retention_seconds);

    auto inner_query = parseCreateQuery(internal_definition);
    auto & inner_create = inner_query->as<ASTCreateQuery &>();
    inner_create.setDatabase(inner_table_id.getDatabaseName());
    inner_create.setTable(inner_table_id.getTableName());

    const auto * outer_columns = outer_query.columns_list ? outer_query.columns_list->columns : nullptr;
    if (!outer_columns)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The `Queue` table engine requires an explicit column list");

    auto * inner_columns = inner_create.columns_list->columns;
    ASTs user_columns;
    user_columns.reserve(outer_columns->children.size());
    for (const auto & column : outer_columns->children)
        user_columns.emplace_back(column->clone());
    inner_columns->children.insert(inner_columns->children.begin(), user_columns.begin(), user_columns.end());

    auto create_context = Context::createCopy(local_context);
    InterpreterCreateQuery interpreter(inner_query, create_context);
    interpreter.setInternal(true);
    interpreter.execute();
    return inner_table_id;
}


StorageQueue::StorageQueue(
    const StorageID & table_id_,
    ContextPtr context_,
    LoadingStrictnessLevel mode_,
    const ASTCreateQuery & query_,
    const ColumnsDescription & columns_,
    const String & comment_,
    UInt64 retention_seconds_,
    UInt64 max_batch_size_,
    UInt64 polling_interval_ms_)
    : IStreamingStorage(table_id_)
    , WithContext(context_->getGlobalContext())
    , inner_table_id(createInnerTable(query_, table_id_, context_, mode_, retention_seconds_))
    , max_batch_size(max_batch_size_)
    , polling_interval_ms(polling_interval_ms_)
    , log(getLogger("StorageQueue (" + table_id_.getFullTableName() + ")"))
{
    for (const auto internal_name : {ID_COLUMN, VERSION_COLUMN, IS_DELETED_COLUMN, CREATED_AT_COLUMN})
    {
        if (columns_.has(String(internal_name)))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column name `{}` is reserved by the `Queue` table engine", internal_name);
    }

    StorageInMemoryMetadata metadata;
    metadata.setColumns(columns_);
    metadata.setComment(comment_);
    setInMemoryMetadata(metadata);

    streaming_task = getContext()->getSchedulePool().createTask(
        getStorageID(),
        log->name(),
        [this] { threadFunc(); });
    streaming_task->deactivate();
}


StorageQueue::~StorageQueue()
{
    if (!shutdown_called)
        shutdown(false);
}


StoragePtr StorageQueue::getInnerTable(ContextPtr local_context) const
{
    return DatabaseCatalog::instance().getTable(inner_table_id, local_context);
}


void StorageQueue::startup()
{
    streaming_task->activateAndSchedule();
    StreamingStorageRegistry::instance().registerTable(getStorageID());
}


void StorageQueue::shutdown(bool)
{
    if (shutdown_called.exchange(true))
        return;

    streaming_task->deactivate();
    StreamingStorageRegistry::instance().unregisterTable(getStorageID(), /* if_exists */ true);
}


void StorageQueue::scheduleStreamingTasksImpl()
{
    streaming_task->schedule();
}


void StorageQueue::threadFunc()
{
    try
    {
        const UInt64 cycle_epoch = stream_control.currentCancelEpoch();
        const bool has_ready_views
            = !DatabaseCatalog::instance().getReadyDependentViews(getStorageID(), getContext()).empty();

        if (!shutdown_called
            && has_ready_views
            && stream_control.claimCycle(last_seen_refresh_epoch))
        {
            while (!shutdown_called
                && !stream_control.isCancelRequested(cycle_epoch)
                && streamToViews(cycle_epoch))
            {
                if (stream_control.isBlocked())
                    break;
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to consume from `Queue`");
    }

    if (!shutdown_called)
        streaming_task->scheduleAfter(polling_interval_ms);
}


bool StorageQueue::streamToViews(UInt64 cycle_epoch)
{
    std::lock_guard lock(consume_mutex);

    auto queue_context = Context::createCopy(getContext());
    queue_context->makeQueryContext();

    auto insert = make_intrusive<ASTInsertQuery>();
    insert->table_id = getStorageID();
    InterpreterInsertQuery view_interpreter(
        insert,
        queue_context,
        /* allow_materialized */ false,
        /* no_squash */ true,
        /* no_destination */ true,
        /* async_insert */ false);
    auto view_io = view_interpreter.execute();
    const Names delivery_columns = view_io.pipeline.getHeader().getNames();

    Names select_columns = delivery_columns;
    select_columns.emplace_back(ID_COLUMN);
    select_columns.emplace_back(VERSION_COLUMN);
    select_columns.emplace_back(IS_DELETED_COLUMN);
    select_columns.emplace_back(CREATED_AT_COLUMN);

    Strings quoted_columns;
    quoted_columns.reserve(select_columns.size());
    for (const auto & name : select_columns)
        quoted_columns.emplace_back(backQuoteIfNeed(name));

    const String select_query = fmt::format(
        "SELECT {} FROM {} FINAL ORDER BY `{}` LIMIT {}",
        fmt::join(quoted_columns, ", "),
        quoteTable(inner_table_id),
        ID_COLUMN,
        max_batch_size);

    auto select_io = executeQuery(select_query, queue_context, QueryFlags{.internal = true}).second;
    PullingPipelineExecutor reader(select_io.pipeline);
    PushingPipelineExecutor writer(view_io.pipeline);
    writer.start();

    Blocks acknowledgement_blocks;
    size_t rows = 0;

    bool view_query_finished = false;
    bool select_query_finished = false;
    try
    {
        Block block;
        while (reader.pull(block))
        {
            if (!block.rows())
                continue;

            Block payload;
            for (const auto & name : delivery_columns)
                payload.insert(block.getByName(name));
            writer.push(std::move(payload));

            Block acknowledgement;
            acknowledgement.insert(block.getByName(String(ID_COLUMN)));
            acknowledgement.insert(ColumnWithTypeAndName{
                ColumnUInt64::create(block.rows(), 2),
                block.getByName(String(VERSION_COLUMN)).type,
                String(VERSION_COLUMN)});
            acknowledgement.insert(ColumnWithTypeAndName{
                ColumnUInt8::create(block.rows(), 1),
                block.getByName(String(IS_DELETED_COLUMN)).type,
                String(IS_DELETED_COLUMN)});
            acknowledgement.insert(block.getByName(String(CREATED_AT_COLUMN)));
            acknowledgement_blocks.emplace_back(std::move(acknowledgement));
            rows += block.rows();
        }

        if (!rows)
        {
            writer.finish();
            select_io.onFinish();
            select_query_finished = true;
            view_io.onFinish();
            view_query_finished = true;
            return false;
        }

        writer.finish();
        view_io.onFinish();
        view_query_finished = true;
        select_io.onFinish();
        select_query_finished = true;

        if (stream_control.isCancelRequested(cycle_epoch))
            return false;

        acknowledge(acknowledgement_blocks, queue_context);
    }
    catch (...)
    {
        writer.cancel();
        if (!view_query_finished)
            view_io.onException();
        if (!select_query_finished)
            select_io.onException();
        throw;
    }

    LOG_DEBUG(log, "Delivered and acknowledged {} queued rows", rows);
    return true;
}


void StorageQueue::acknowledge(const Blocks & blocks, ContextMutablePtr queue_context)
{
    const Names columns{
        String(ID_COLUMN),
        String(VERSION_COLUMN),
        String(IS_DELETED_COLUMN),
        String(CREATED_AT_COLUMN)};

    InterpreterInsertQuery interpreter(
        makeInsertQuery(inner_table_id, columns),
        queue_context,
        /* allow_materialized */ true,
        /* no_squash */ true,
        /* no_destination */ false,
        /* async_insert */ false);
    auto block_io = interpreter.execute();
    PushingPipelineExecutor executor(block_io.pipeline);
    executor.start();

    try
    {
        for (const auto & block : blocks)
            executor.push(block);
        executor.finish();
        block_io.onFinish();
    }
    catch (...)
    {
        executor.cancel();
        block_io.onException();
        throw;
    }
}


void StorageQueue::read(
    QueryPlan &,
    const Names &,
    const StorageSnapshotPtr &,
    SelectQueryInfo &,
    ContextPtr,
    QueryProcessingStage::Enum,
    size_t,
    size_t)
{
    throw Exception(
        ErrorCodes::NOT_IMPLEMENTED,
        "Direct `SELECT` from a `Queue` table is not supported yet; attach a materialized view");
}


SinkToStoragePtr StorageQueue::write(
    const ASTPtr &,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr local_context,
    bool async_insert)
{
    const Block header = metadata_snapshot->getSampleBlockNonMaterialized();
    return std::make_shared<QueueSink>(inner_table_id, header, local_context, async_insert);
}


void StorageQueue::drop()
{
    dropInnerTableIfAny(/* sync */ false, getContext());
}


void StorageQueue::dropInnerTableIfAny(bool sync, ContextPtr local_context)
{
    if (!DatabaseCatalog::instance().tryGetTable(inner_table_id, local_context))
        return;

    const bool may_lock_ddl_guard = getStorageID().getQualifiedName() < inner_table_id.getQualifiedName();
    InterpreterDropQuery::executeDropQuery(
        ASTDropQuery::Kind::Drop,
        getContext(),
        local_context,
        inner_table_id,
        sync,
        /* ignore_sync_setting */ true,
        may_lock_ddl_guard);
}


void StorageQueue::checkTableSizeBelowDropLimit(ContextPtr query_context) const
{
    if (auto inner = DatabaseCatalog::instance().tryGetTable(inner_table_id, query_context))
        inner->checkTableSizeBelowDropLimit(query_context);
}


void StorageQueue::truncate(
    const ASTPtr &,
    const StorageMetadataPtr &,
    ContextPtr local_context,
    TableExclusiveLockHolder &)
{
    InterpreterDropQuery::executeDropQuery(
        ASTDropQuery::Kind::Truncate,
        getContext(),
        local_context,
        inner_table_id,
        /* sync */ true);
}


std::optional<UInt64> StorageQueue::totalRows(ContextPtr query_context) const
{
    return getInnerTable(query_context)->totalRows(query_context);
}


std::optional<UInt64> StorageQueue::totalBytes(ContextPtr query_context) const
{
    return getInnerTable(query_context)->totalBytes(query_context);
}


Strings StorageQueue::getDataPaths() const
{
    return getInnerTable(getContext())->getDataPaths();
}


void registerStorageQueue(StorageFactory & factory)
{
    factory.registerStorage(
        "Queue",
        [](const StorageFactory::Arguments & args)
        {
            if (args.mode <= LoadingStrictnessLevel::CREATE
                && !args.getLocalContext()->getSettingsRef()[Setting::allow_experimental_queue_table_engine])
            {
                throw Exception(
                    ErrorCodes::UNKNOWN_STORAGE,
                    "Table engine `Queue` is experimental. "
                    "Set `allow_experimental_queue_table_engine` to enable it");
            }

            if (args.engine_args.size() > 3)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "The `Queue` table engine accepts up to three arguments: "
                    "retention_seconds, max_batch_size, polling_interval_ms");

            for (const auto internal_name : {ID_COLUMN, VERSION_COLUMN, IS_DELETED_COLUMN, CREATED_AT_COLUMN})
            {
                if (args.columns.has(String(internal_name)))
                {
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Column name `{}` is reserved by the `Queue` table engine",
                        internal_name);
                }
            }

            UInt64 retention_seconds = DEFAULT_RETENTION_SECONDS;
            UInt64 max_batch_size = DEFAULT_MAX_BATCH_SIZE;
            UInt64 polling_interval_ms = DEFAULT_POLLING_INTERVAL_MS;

            if (!args.engine_args.empty())
                retention_seconds = checkAndGetLiteralArgument<UInt64>(args.engine_args[0], "retention_seconds");
            if (args.engine_args.size() >= 2)
                max_batch_size = checkAndGetLiteralArgument<UInt64>(args.engine_args[1], "max_batch_size");
            if (args.engine_args.size() >= 3)
                polling_interval_ms = checkAndGetLiteralArgument<UInt64>(args.engine_args[2], "polling_interval_ms");

            if (!retention_seconds || !max_batch_size || !polling_interval_ms)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "All `Queue` engine arguments must be greater than zero");

            return std::make_shared<StorageQueue>(
                args.table_id,
                args.getContext(),
                args.mode,
                args.query,
                args.columns,
                args.comment,
                retention_seconds,
                max_batch_size,
                polling_interval_ms);
        },
        {},
        Documentation{
            .description = "Experimental native persistent queue backed by an internal `ReplacingMergeTree`. "
                "Rows are delivered asynchronously to attached materialized views and acknowledged with tombstone versions only after delivery succeeds.",
            .syntax = "ENGINE = Queue([retention_seconds[, max_batch_size[, polling_interval_ms]]])"});
}

}
