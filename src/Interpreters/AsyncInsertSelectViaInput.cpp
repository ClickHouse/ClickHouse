#include <Interpreters/AsyncInsertSelectViaInput.h>

#include <Interpreters/Squashing.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Processors/Sources/WaitForAsyncInsertSource.h>
#include <Parsers/ASTInsertQuery.h>
#include <Interpreters/Context.h>
#include <QueryPipeline/BlockIO.h>
#include <Common/logger_useful.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/SelectQueryOptions.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>
#include <Core/DeduplicateInsert.h>
#include <Core/Settings.h>
#include <Processors/Formats/Impl/NullFormat.h>
#include <Storages/IStorage.h>
#include <Access/Common/AccessFlags.h>

namespace ProfileEvents
{
    extern const Event InsertQuery;
}

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool wait_for_async_insert;
    extern const SettingsSeconds wait_for_async_insert_timeout;
    extern const SettingsUInt64 async_insert_max_data_size;
    extern const SettingsBool insert_allow_materialized_columns;
    extern const SettingsString insert_deduplication_token;
}

AsyncInsertSelectViaInputSource::AsyncInsertSelectViaInputSource(
    QueryPipeline select_pipeline_,
    AsynchronousInsertQueue * queue_,
    ContextMutablePtr insert_context_,
    ContextMutablePtr context_,
    ASTPtr query_ast_,
    UInt64 max_data_size_,
    bool wait_for_async_insert_,
    UInt64 wait_timeout_ms_,
    bool insert_allow_materialized_,
    StorageID table_id_)
    : ISource(std::make_shared<const Block>())
    , select_pipeline(std::move(select_pipeline_))
    , queue(queue_)
    , insert_context(std::move(insert_context_))
    , context(std::move(context_))
    , query_ast(std::move(query_ast_))
    , max_data_size(max_data_size_)
    , wait_for_async_insert(wait_for_async_insert_)
    , wait_timeout_ms(wait_timeout_ms_)
    , insert_allow_materialized(insert_allow_materialized_)
    , table_id(std::move(table_id_))
    , log(getLogger("executeQuery"))
{
}

Chunk AsyncInsertSelectViaInputSource::generate()
{
    if (done)
        return {};
    done = true;

    PullingPipelineExecutor pulling_executor(select_pipeline);

    Squashing squashing(
        pulling_executor.getSharedHeader(),
        /*min_block_size_rows*/ 0,
        /*min_block_size_bytes*/ max_data_size);

    std::optional<BlockIO> sync_io;
    std::unique_ptr<PushingPipelineExecutor> sync_exec;

    auto init_sync_fallback = [&]
    {
        if (sync_exec)
            return;
        LOG_DEBUG(log,
            "Setting async_insert=1, but INSERT...SELECT FROM input() will be "
            "executed synchronously because payload exceeded "
            "async_insert_max_data_size ({} bytes)",
            max_data_size);
        auto sync_ast = query_ast->clone();
        auto & sync_insert_q = sync_ast->as<ASTInsertQuery &>();
        sync_insert_q.async_insert_flush = true;
        /// Prevent buildInsertPipeline from consuming the one-shot input() body again.
        sync_insert_q.data = nullptr;
        sync_insert_q.end = nullptr;
        sync_insert_q.tail.reset();
        InterpreterInsertQuery sync_interpreter(
            sync_ast, insert_context,
            insert_allow_materialized,
            /* no_squash */ false, /* no_destination */ false, /* async_insert */ false);
        sync_io.emplace(sync_interpreter.execute());
        sync_io->pipeline.setProcessListElement(context->getProcessListElement());
        sync_exec = std::make_unique<PushingPipelineExecutor>(sync_io->pipeline);
        sync_exec->start();
    };

    Block pulled;
    while (pulling_executor.pull(pulled))
    {
        squashing.add({pulled.getColumns(), pulled.rows()});
        auto overflow = Squashing::squash(
            squashing.generate(/*flush_if_enough_size*/ true),
            squashing.getHeader());

        if (!overflow)
            continue;

        init_sync_fallback();
        sync_exec->push(squashing.getHeader()->cloneWithColumns(overflow.detachColumns()));
    }

    /// pull() returns false on both EOF and cancellation.
    if (auto process_list_elem = context->getProcessListElement())
    {
        process_list_elem->checkTimeLimit();
        process_list_elem->throwIfKilled();
    }

    if (sync_exec)
    {
        auto remainder = Squashing::squash(squashing.flush(), squashing.getHeader());
        if (remainder)
            sync_exec->push(squashing.getHeader()->cloneWithColumns(remainder.detachColumns()));
        sync_exec->finish();
        ProfileEvents::increment(ProfileEvents::InsertQuery);
    }
    else
    {
        auto final_chunk = Squashing::squash(squashing.flush(), squashing.getHeader());
        Block merged_block;
        if (final_chunk)
            merged_block = squashing.getHeader()->cloneWithColumns(final_chunk.detachColumns());
        else
            merged_block = squashing.getHeader()->cloneWithoutColumns();

        auto async_query = query_ast->clone();
        auto result = queue->pushQueryWithBlock(async_query, std::move(merged_block), insert_context);
        ProfileEvents::increment(ProfileEvents::InsertQuery);
        if (wait_for_async_insert)
            /// report_read_progress=false: reads were already counted by the SELECT.
            waitForAsyncInsertAndReportProgress(
                result.future, wait_timeout_ms,
                context->getProcessListElement(), context->getProgressCallback(),
                /* report_read_progress */ false);
    }

    if (!table_id.empty())
        context->setInsertionTable(table_id);

    return {};
}

void buildAsyncInsertSelectViaInputPipeline(
    BlockIO & res,
    ASTInsertQuery & insert_query,
    const ASTPtr & query_ast,
    const StoragePtr & destination,
    AsynchronousInsertQueue * queue,
    ContextMutablePtr context,
    const Settings & settings,
    LoggerPtr log)
{
    if (!insert_query.table_function)
    {
        auto validate_metadata = destination->getInMemoryMetadataPtr(context, false);
        auto validate_block = InterpreterInsertQuery::getSampleBlock(
            insert_query, destination, validate_metadata, context,
            /* no_destination */ false,
            settings[Setting::insert_allow_materialized_columns]);
        context->checkAccess(AccessType::INSERT, insert_query.table_id, validate_block.getNames());
    }

    auto select_query_options = SelectQueryOptions(QueryProcessingStage::Complete, 1);
    QueryPipelineBuilder select_pipeline;
    if (settings[Setting::allow_experimental_analyzer])
    {
        InterpreterSelectQueryAnalyzer interpreter_select(insert_query.select, context, select_query_options);
        select_pipeline = interpreter_select.buildQueryPipeline();
    }
    else
    {
        InterpreterSelectWithUnionQuery interpreter_select(insert_query.select, context, select_query_options);
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

    if (destination)
        InterpreterInsertQuery::convertSelectToInsertSchema(
            select_pipeline, insert_query, destination, context,
            /* no_destination */ false,
            settings[Setting::insert_allow_materialized_columns]);

    auto source = std::make_shared<AsyncInsertSelectViaInputSource>(
        QueryPipelineBuilder::getPipeline(std::move(select_pipeline)),
        queue,
        insert_context,
        context,
        query_ast,
        settings[Setting::async_insert_max_data_size],
        settings[Setting::wait_for_async_insert],
        settings[Setting::wait_for_async_insert_timeout].totalMilliseconds(),
        settings[Setting::insert_allow_materialized_columns],
        insert_query.table_id);
    res.pipeline = QueryPipeline(Pipe(std::move(source)));
    res.pipeline.complete(std::make_shared<NullOutputFormat>(std::make_shared<const Block>(Block())));
}

}
