#include <Interpreters/AsyncInsertSelectSource.h>

#include <Access/Common/AccessFlags.h>
#include <Common/logger_useful.h>
#include <Core/Block.h>
#include <Core/DeduplicateInsert.h>
#include <Core/Settings.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Processors/Formats/Impl/NullFormat.h>
#include <Processors/Sources/WaitForAsyncInsertSource.h>
#include <QueryPipeline/BlockIO.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/IStorage.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsSeconds wait_for_async_insert_timeout;
    extern const SettingsUInt64 async_insert_max_data_size;
    extern const SettingsBool insert_allow_materialized_columns;
    extern const SettingsString insert_deduplication_token;
}

AsyncInsertSelectSource::AsyncInsertSelectSource(
    QueryPipeline select_pipeline_,
    AsynchronousInsertQueue * queue_,
    ContextMutablePtr insert_context_,
    ContextMutablePtr context_,
    ASTPtr query_ast_,
    UInt64 max_data_size_,
    UInt64 wait_timeout_ms_,
    bool insert_allow_materialized_,
    StorageID table_id_,
    bool needs_null_default_sync_,
    InsertDependenciesBuilder::ConstPtr forced_insert_dependencies_)
    : ISource(std::make_shared<const Block>())
    , select_pipeline(std::move(select_pipeline_))
    , queue(queue_)
    , insert_context(std::move(insert_context_))
    , context(std::move(context_))
    , query_ast(std::move(query_ast_))
    , max_data_size(max_data_size_)
    , wait_timeout_ms(wait_timeout_ms_)
    , insert_allow_materialized(insert_allow_materialized_)
    , table_id(std::move(table_id_))
    , needs_null_default_sync(needs_null_default_sync_)
    , forced_insert_dependencies(std::move(forced_insert_dependencies_))
    , log(getLogger("executeQuery"))
{
}

Chunk AsyncInsertSelectSource::generate()
{
    if (done)
        return {};
    done = true;

    PullingPipelineExecutor pulling_executor(select_pipeline);

    std::optional<BlockIO> sync_io;
    std::unique_ptr<PushingPipelineExecutor> sync_exec;

    auto init_sync_fallback = [&](const String & reason)
    {
        if (sync_exec)
            return;
        LOG_DEBUG(log, "Setting async_insert=1, but INSERT ... SELECT will be executed synchronously (reason: {})", reason);
        auto sync_ast = query_ast->clone();
        auto & sync_insert_q = sync_ast->as<ASTInsertQuery &>();
        /// `async_insert_flush=true` skips the SELECT branch in `InterpreterInsertQuery::execute`
        /// so the SELECT does not run again.
        sync_insert_q.async_insert_flush = true;
        sync_insert_q.data = nullptr;
        sync_insert_q.end = nullptr;
        sync_insert_q.tail.reset();
        InterpreterInsertQuery sync_interpreter(
            sync_ast, insert_context,
            insert_allow_materialized,
            /* no_squash */ false, /* no_destination */ false, /* async_insert */ false);
        /// Use the graph frozen before the SELECT ran, not one rebuilt now.
        if (forced_insert_dependencies)
            sync_interpreter.setForcedInsertDependencies(forced_insert_dependencies);
        sync_io.emplace(sync_interpreter.execute());
        sync_io->pipeline.setProcessListElement(context->getProcessListElement());
        sync_exec = std::make_unique<PushingPipelineExecutor>(sync_io->pipeline);
        sync_exec->start();
    };

    /// The queue flush has no defaults step to apply, see `needs_null_default_sync` below.
    if (needs_null_default_sync)
        init_sync_fallback("insert_null_as_default requires substituting NULL with column defaults");

    /// Hold at most one block: the async queue path applies only when the SELECT result fits in
    /// a single block within `async_insert_max_data_size`.
    Block single_block;
    bool have_single = false;

    Block pulled;
    while (pulling_executor.pull(pulled))
    {
        if (pulled.rows() == 0)
            continue;

        if (sync_exec)
        {
            sync_exec->push(std::move(pulled));
            continue;
        }

        if (!have_single)
        {
            /// Materialize before sizing: `IColumn::bytes` on a `ColumnConst` or `ColumnSparse`
            /// undercounts the size a queued block would use. `convertSelectToInsertSchema`'s
            /// `ExpressionTransform` already materializes `ColumnConst`, and `MergeTree` reads
            /// already densify `ColumnSparse`; this is a defensive guard for a future source
            /// that skips both.
            materializeBlockInplace(pulled);
            if (pulled.bytes() > max_data_size)
            {
                init_sync_fallback(fmt::format("block size {} bytes exceeds async_insert_max_data_size {} bytes",
                    pulled.bytes(), max_data_size));
                sync_exec->push(std::move(pulled));
            }
            else
            {
                single_block = std::move(pulled);
                have_single = true;
            }
            continue;
        }

        /// A second non-empty block means the result is not a single block.
        init_sync_fallback("SELECT produced more than one block");
        sync_exec->push(std::exchange(single_block, Block{}));
        have_single = false;
        sync_exec->push(std::move(pulled));
    }

    /// `pull()` returns false on both EOF and cancellation; check limits explicitly.
    if (auto process_list_elem = context->getProcessListElement())
    {
        process_list_elem->checkTimeLimit();
        process_list_elem->throwIfKilled();
    }

    if (sync_exec)
    {
        sync_exec->finish();
    }
    else if (!have_single)
    {
        /// Empty SELECT: still run the insert so side effecting destinations (table functions,
        /// materialized views) get created.
        init_sync_fallback("INSERT ... SELECT produced zero rows");
        sync_exec->finish();
    }
    else
    {
        auto async_query = query_ast->clone();
        /// The pushed block is Preprocessed (Native-encoded). `preprocessInsertQuery` rejects an
        /// empty format, and a plain `INSERT ... SELECT` carries none, so set `Native` explicitly.
        async_query->as<ASTInsertQuery &>().format = "Native";
        /// Not frozen here: the queue batches pushes from multiple clients under one key and
        /// resolves the dependency graph once per batch at flush time; freezing it here would
        /// break that batching.
        auto result = queue->pushQueryWithBlock(async_query, std::move(single_block), insert_context);
        /// `report_read_progress=false`: reads were already counted by the SELECT pipeline.
        waitForAsyncInsertAndReportProgress(
            result.future, wait_timeout_ms,
            context->getProcessListElement(), context->getProgressCallback(),
            /* report_read_progress */ false);
    }

    /// `setInsertContextValues` in `executeQuery` already recorded the table with its column
    /// description; avoid clobbering it with an entry that has none.
    if (!table_id.empty() && !context->hasInsertionTableColumnsDescription())
        context->setInsertionTable(table_id);

    return {};
}

void buildAsyncInsertSelectPipeline(
    BlockIO & res,
    ASTInsertQuery & insert_query,
    const ASTPtr & query_ast,
    const StoragePtr & destination,
    AsynchronousInsertQueue * queue,
    ContextMutablePtr context,
    const Settings & settings,
    LoggerPtr log)
{
    /// Freeze the destination schema and column list before building the SELECT pipeline, so that
    /// a concurrent `ADD COLUMN` cannot widen the schema after this point.
    auto metadata_snapshot = destination->getInMemoryMetadataPtr(context, false);
    Block insert_schema = InterpreterInsertQuery::getSampleBlock(
        insert_query, destination, metadata_snapshot, context,
        /* no_destination */ false,
        settings[Setting::insert_allow_materialized_columns]);

    /// Rebuild unconditionally: a column transformer like `* EXCEPT b` re-resolves against
    /// whatever metadata is current whenever evaluated, which would defeat the freeze above.
    insert_query.columns = make_intrusive<ASTExpressionList>();
    for (const auto & col : insert_schema.getColumnsWithTypeAndName())
        insert_query.columns->children.push_back(make_intrusive<ASTIdentifier>(col.name));

    if (!insert_query.table_function)
        context->checkAccess(AccessType::INSERT, insert_query.table_id, insert_schema.getNames());

    /// Disable parallel replicas for the SELECT, matching `InterpreterInsertQuery::addInsertToSelectPipeline`
    /// and `buildInsertPipeline`: the fallback must run the SELECT locally, not fan out to replicas.
    if (context->canUseParallelReplicasOnInitiator())
    {
        auto mutable_context = Context::createCopy(context);
        mutable_context->setSetting("enable_parallel_replicas", Field{0});
        context = mutable_context;
    }

    /// For a trivial SELECT, raise `max_block_size` to the INSERT granularity so it emits one
    /// block instead of many, avoiding a spurious fallback to the synchronous path. This also
    /// forces `max_threads` to 1 (the synchronous path uses `max_insert_threads` instead), because
    /// a parallel read would itself produce more than one block.
    ContextPtr select_context = context;
    InterpreterInsertQuery::applyTrivialInsertSelectOptimization(insert_query, destination->prefersLargeBlocks(), /* effective_max_insert_threads */ 1, select_context);

    auto select_query_options = SelectQueryOptions(QueryProcessingStage::Complete, 1);
    QueryPipelineBuilder select_pipeline;
    if (settings[Setting::allow_experimental_analyzer])
    {
        InterpreterSelectQueryAnalyzer interpreter_select(insert_query.select, select_context, select_query_options);
        select_pipeline = interpreter_select.buildQueryPipeline();
    }
    else
    {
        InterpreterSelectWithUnionQuery interpreter_select(insert_query.select, select_context, select_query_options);
        select_pipeline = interpreter_select.buildQueryPipeline();
    }

    ContextMutablePtr insert_context = context;
    {
        const bool select_query_sorted =
            InterpreterInsertQuery::queryHasOrderByAll(insert_query.select)
            && select_pipeline.getNumStreams() <= 1;
        const bool dedup = isDeduplicationEnabledForInsertSelect(
            select_query_sorted, settings,
            settings[Setting::insert_deduplication_token].value, log);
        if (dedup != isDeduplicationEnabledForInsert(false, settings))
        {
            insert_context = Context::createCopy(context);
            overrideDeduplicationSetting(dedup, insert_context);
        }
    }

    /// Convert exactly as the synchronous `INSERT ... SELECT` path does in
    /// `InterpreterInsertQuery::addInsertToSelectPipeline`, so both paths agree on types and on
    /// the exceptions they raise. Under `insert_null_as_default` this widens target columns to
    /// `Nullable`, which the next check relies on.
    Block converted_schema = InterpreterInsertQuery::convertSelectToInsertSchema(
        select_pipeline, insert_query, destination, context,
        /* no_destination */ false,
        settings[Setting::insert_allow_materialized_columns]);

    /// A structural difference from `insert_schema` means `insert_null_as_default` widened a
    /// column to `Nullable`; the queue flush has no defaults step to undo that, so force the
    /// synchronous fallback.
    bool needs_null_default_sync = !blocksHaveEqualStructure(converted_schema, insert_schema);

    /// Freeze the dependency graph (materialized views) before the SELECT runs: a concurrent
    /// `CREATE MATERIALIZED VIEW ... TO` landing after must not change where the fallback writes.
    /// This does not freeze INSERT authorization; the fallback still checks access again before
    /// it writes. The argument is `converted_schema` rather than `insert_schema` because the
    /// chain's leading `ConvertingTransform` substitutes NULL with column defaults from it.
    auto forced_insert_dependencies = InsertDependenciesBuilder::create(
        destination, query_ast, std::make_shared<const Block>(converted_schema),
        /* async_insert */ false, /* skip_destination_table */ false,
        /* max_insert_threads */ 1, insert_context);

    /// Register the SELECT pipeline with the process list so that KILL QUERY and
    /// `max_execution_time` can interrupt the pull phase.
    select_pipeline.setProcessListElement(context->getProcessListElement());

    auto source = std::make_shared<AsyncInsertSelectSource>(
        QueryPipelineBuilder::getPipeline(std::move(select_pipeline)),
        queue,
        insert_context,
        context,
        query_ast,
        settings[Setting::async_insert_max_data_size],
        settings[Setting::wait_for_async_insert_timeout].totalMilliseconds(),
        settings[Setting::insert_allow_materialized_columns],
        insert_query.table_id,
        needs_null_default_sync,
        forced_insert_dependencies);
    res.pipeline = QueryPipeline(Pipe(std::move(source)));
    res.pipeline.complete(std::make_shared<NullOutputFormat>(std::make_shared<const Block>(Block())));
}

}
