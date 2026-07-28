#include <Interpreters/AsyncInsertSelectSource.h>

#include <Access/Common/AccessFlags.h>
#include <Common/logger_useful.h>
#include <Core/Block.h>
#include <Core/DeduplicateInsert.h>
#include <Core/Settings.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/addMissingDefaults.h>
#include <Interpreters/createSubcolumnsExtractionActions.h>
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
#include <Processors/Transforms/ExpressionTransform.h>
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
    extern const SettingsBool insert_null_as_default;
}

namespace ErrorCodes
{
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
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
    std::shared_ptr<ExpressionActions> sync_converter;

    auto init_sync_fallback = [&](const String & reason)
    {
        if (sync_exec)
            return;
        LOG_DEBUG(log, "Setting async_insert=1, but INSERT ... SELECT will be executed synchronously (reason: {})", reason);
        auto sync_ast = query_ast->clone();
        auto & sync_insert_q = sync_ast->as<ASTInsertQuery &>();
        /// `async_insert_flush=true` makes `InterpreterInsertQuery::execute` skip the SELECT
        /// branch and build a pushing pipeline, so the SELECT is not run again.
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
        /// The SELECT output was through a rename-only transform (names match the insert schema,
        /// but types are the original SELECT output types). Build a converting expression so each
        /// pushed block is cast to the types the push pipeline expects (insert schema types).
        auto convert_dag = ActionsDAG::makeConvertingActions(
            pulling_executor.getHeader().getColumnsWithTypeAndName(),
            sync_exec->getHeader().getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            context);
        sync_converter = std::make_shared<ExpressionActions>(std::move(convert_dag));
    };

    /// The async queue flush cannot substitute NULL with column defaults, so a Nullable SELECT
    /// column feeding a non-Nullable one under insert_null_as_default must go synchronous.
    if (needs_null_default_sync)
        init_sync_fallback("insert_null_as_default requires substituting NULL with column defaults");

    /// Hold at most one block. The async queue path is taken only when the whole SELECT result
    /// is a single block within `async_insert_max_data_size`. Any other shape (more than one
    /// block, an oversized block, or an empty result) goes through the synchronous insert.
    Block single_block;
    bool have_single = false;

    Block pulled;
    while (pulling_executor.pull(pulled))
    {
        if (pulled.rows() == 0)
            continue;

        if (sync_exec)
        {
            sync_converter->execute(pulled);
            sync_exec->push(std::move(pulled));
            continue;
        }

        if (!have_single)
        {
            /// Materialize before sizing: IColumn::bytes() on a ColumnConst or ColumnSparse
            /// reflects only its compact representation, not the size it would take once
            /// queued. The rename step in buildAsyncInsertSelectPipeline already materializes
            /// any ColumnConst reaching it, and MergeTree reads already densify ColumnSparse,
            /// so this call is a defensive guard against a future source that skips both.
            materializeBlockInplace(pulled);
            if (pulled.bytes() > max_data_size)
            {
                init_sync_fallback(fmt::format("block size {} bytes exceeds async_insert_max_data_size {} bytes",
                    pulled.bytes(), max_data_size));
                sync_converter->execute(pulled);
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
        sync_converter->execute(single_block);
        sync_exec->push(std::exchange(single_block, Block{}));
        have_single = false;
        sync_converter->execute(pulled);
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
        /// Empty SELECT: still run the insert so side-effecting destinations
        /// (file table functions, empty parts, materialized views) are created.
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
        /// resolves the dependency graph once at flush time for the whole batch, existing
        /// async-insert behavior unrelated to SELECT, so freezing it here would break batching.
        auto result = queue->pushQueryWithBlock(async_query, std::move(single_block), insert_context);
        /// `report_read_progress=false`: reads were already counted by the SELECT pipeline.
        waitForAsyncInsertAndReportProgress(
            result.future, wait_timeout_ms,
            context->getProcessListElement(), context->getProgressCallback(),
            /* report_read_progress */ false);
    }

    /// `setInsertContextValues` in executeQuery already recorded the table with its column
    /// description; do not clobber it with a column-less entry.
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
    /// Take the destination schema snapshot and freeze the column list before building the SELECT
    /// pipeline (which can run for a while under a slow build). Otherwise a concurrent ADD COLUMN
    /// landing between this point and a later re-fetch would widen the schema mid-flight and make
    /// the positional column-count check below fail spuriously.
    StorageMetadataHandle metadata_snapshot;
    Block insert_schema;
    InsertDependenciesBuilder::ConstPtr forced_insert_dependencies;
    if (destination)
    {
        metadata_snapshot = destination->getInMemoryMetadataPtr(context, false);
        insert_schema = InterpreterInsertQuery::getSampleBlock(
            insert_query, destination, metadata_snapshot, context,
            /* no_destination */ false,
            settings[Setting::insert_allow_materialized_columns]);

        if (!insert_query.columns)
        {
            insert_query.columns = make_intrusive<ASTExpressionList>();
            for (const auto & col : insert_schema.getColumnsWithTypeAndName())
                insert_query.columns->children.push_back(make_intrusive<ASTIdentifier>(col.name));
        }
    }

    if (!insert_query.table_function)
        context->checkAccess(AccessType::INSERT, insert_query.table_id, insert_schema.getNames());

    // Disable parallel replicas for the SELECT, matching the synchronous insert path
    // (InterpreterInsertQuery::addInsertToSelectPipeline / buildInsertPipeline).
    // When parallel_distributed_insert_select=0 the async fallback must still run
    // the SELECT locally so it does not fan out to replicas on the initiator.
    if (context->canUseParallelReplicasOnInitiator())
    {
        auto mutable_context = Context::createCopy(context);
        mutable_context->setSetting("enable_parallel_replicas", Field{0});
        context = mutable_context;
    }

    /// Apply trivial INSERT...SELECT optimization: for a trivial SELECT (no joins/subqueries),
    /// raise max_block_size to match the INSERT block granularity so the SELECT produces a single
    /// large block instead of many default-sized (~65k rows) blocks, preventing spurious
    /// multi-block fallback to the synchronous insert path. This also forces max_threads to 1,
    /// unlike the synchronous path, which uses max_insert_threads: a parallel read would itself
    /// produce more than one block and defeat the single-block async check below. If the query
    /// still falls back to the synchronous path afterward, the remaining read stays
    /// single-threaded too, an accepted tradeoff of using optimize_trivial_insert_select together
    /// with async_insert (the setting defaults to off, so this does not affect other queries).
    ContextPtr select_context = context;
    if (destination)
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

    bool needs_null_default_sync = false;
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

    if (destination)
    {
        /// Freeze the dependency graph (materialized views) before the SELECT runs, for the
        /// synchronous fallback below. Otherwise a concurrent CREATE MATERIALIZED VIEW ... TO
        /// <destination> could change where the fallback writes after the SELECT already read.
        /// This does not freeze INSERT authorization: the fallback still checks access again,
        /// right before it writes, the same way a flushed async insert rechecks access.
        /// insert_context is the source of truth here: the builder latches the deduplication
        /// decision at construction, and that decision depends on the SELECT's stability.
        forced_insert_dependencies = InsertDependenciesBuilder::create(
            destination, query_ast, std::make_shared<const Block>(insert_schema),
            /* async_insert */ false, /* skip_destination_table */ false,
            /* max_insert_threads */ 1, insert_context);
    }

    if (destination)
    {
        /// Rename SELECT output columns to insert-schema names by position so that
        /// processPreprocessedEntries (MatchColumnsMode::Name) and the sync-fallback
        /// ConvertingTransform can locate them.  Type conversion happens exactly once
        /// inside the insert pipeline; doing it here would double-convert and can
        /// crash with a materialize/Nullable type mismatch.
        select_pipeline.dropTotalsAndExtremes();

        const auto & src_cols = select_pipeline.getHeader().getColumnsWithTypeAndName();
        const auto & dst_cols = insert_schema.getColumnsWithTypeAndName();

        /// Positional insert: the SELECT must produce exactly one column per insert-schema column.
        /// Validate before indexing dst_cols below so a user mismatch raises the standard exception
        /// instead of reading past dst_cols. insert_schema was captured before the SELECT pipeline
        /// was built, so this compares against a schema a concurrent ADD COLUMN cannot have widened.
        if (src_cols.size() != dst_cols.size())
            throw Exception(
                ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH,
                "Number of columns in INSERT ... SELECT doesn't match: SELECT returns {}, but the target expects {}",
                src_cols.size(), dst_cols.size());

        /// The queue flush has no defaults step, so a Nullable-to-non-Nullable column under
        /// insert_null_as_default cannot be substituted there.
        if (settings[Setting::insert_null_as_default])
        {
            for (size_t i = 0; i < src_cols.size(); ++i)
            {
                if (isNullableOrLowCardinalityNullable(src_cols[i].type) && !isNullableOrLowCardinalityNullable(dst_cols[i].type)
                    && !isVariant(dst_cols[i].type) && !isDynamic(dst_cols[i].type))
                {
                    needs_null_default_sync = true;
                    break;
                }
            }
        }

        if (needs_null_default_sync)
        {
            /// Apply the full type-conversion and NULL-to-default substitution on the SELECT
            /// pipeline so the blocks handed downstream already match the non-nullable insert schema.
            auto nullable_schema = InterpreterInsertQuery::convertSelectToInsertSchema(
                select_pipeline, insert_query, destination, context,
                /* no_destination */ false,
                settings[Setting::insert_allow_materialized_columns]);

            auto defaults_dag = addMissingDefaults(
                nullable_schema,
                insert_schema.getNamesAndTypesList(),
                metadata_snapshot->getColumns(),
                context,
                /* null_as_default */ true);

            auto subcolumns_dag = createSubcolumnsExtractionActions(
                nullable_schema, defaults_dag.getRequiredColumnsNames(), context);

            auto merged_dag = ActionsDAG::merge(std::move(subcolumns_dag), std::move(defaults_dag));
            auto defaults_actions = std::make_shared<ExpressionActions>(std::move(merged_dag));
            select_pipeline.addSimpleTransform(
                [&](const SharedHeader & in_header) -> ProcessorPtr
                { return std::make_shared<ExpressionTransform>(in_header, defaults_actions); });
        }
        else
        {
            ColumnsWithTypeAndName rename_cols;
            rename_cols.reserve(src_cols.size());
            for (size_t i = 0; i < src_cols.size(); ++i)
                rename_cols.push_back({nullptr, src_cols[i].type, dst_cols[i].name});

            auto rename_dag = ActionsDAG::makeConvertingActions(src_cols, rename_cols, ActionsDAG::MatchColumnsMode::Position, context);
            auto rename_actions = std::make_shared<ExpressionActions>(std::move(rename_dag));
            select_pipeline.addSimpleTransform(
                [&](const SharedHeader & in_header) -> ProcessorPtr
                { return std::make_shared<ExpressionTransform>(in_header, rename_actions); });
        }
    }

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
