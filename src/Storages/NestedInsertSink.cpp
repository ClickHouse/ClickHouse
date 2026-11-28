#include <Storages/NestedInsertSink.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Common/Exception.h>


namespace DB
{

NestedInsertSink::NestedInsertSink(
    SharedHeader header,
    StorageID target_table_id_,
    Names columns_to_insert_,
    bool explicit_column_list_,
    ContextPtr context_,
    bool async_insert_)
    : SinkToStorage(std::move(header))
    , WithContext(context_)
    , target_table_id(std::move(target_table_id_))
    , columns_to_insert(std::move(columns_to_insert_))
    , explicit_column_list(explicit_column_list_)
    , async_insert(async_insert_)
{
    for (const auto & name : columns_to_insert)
        insert_header.insert(getHeader().getByName(name));

    /// The nested query is built here, when the chain of the outer query is built, and not in `onStart`, so that
    /// the errors it finds (an unknown table, missing grants, also deeper in a chain of forwarding storages) are
    /// thrown before any data is written into the other tables of the outer query.
    auto insert = make_intrusive<ASTInsertQuery>();
    insert->table_id = target_table_id;
    if (explicit_column_list)
    {
        auto list_of_columns = make_intrusive<ASTExpressionList>();
        list_of_columns->children.reserve(columns_to_insert.size());
        for (const auto & name : columns_to_insert)
            list_of_columns->children.push_back(make_intrusive<ASTIdentifier>(name));
        insert->columns = list_of_columns;
        insert->children.push_back(insert->columns);
    }
    ASTPtr query_ptr = insert;

    auto insert_context = Context::createCopy(getContext());
    insert_context->makeQueryContext();
    /// A part of the outer query: plan it from the same snapshot.
    insert_context->shareQueryMaxThreadsSnapshot(*getContext());
    if (getContext()->hasQueryContext())
        insert_context->setQueryAccessInfo(getContext()->getQueryContext()->getQueryAccessInfoPtr());
    addInterpreterContext(insert_context);

    /// This sink is one branch of the outer query's `max_insert_threads` fan-out (or its only
    /// stream). Keep the nested INSERT single-stream: with the outer fan-out already in place,
    /// letting every branch fan out again would multiply the number of real sink branches (part
    /// writers, squashing and compression buffers) up to `max_insert_threads^2`, exceeding the
    /// budget the user allowed for this INSERT.
    insert_context->setSetting("max_insert_threads", 1);

    /// Thread the outer async-insert flag into the nested target pipeline so the forwarded INSERT
    /// matches a direct insert: async batches select async dedup settings and skip the
    /// strict block-limit squashing that would slice their multi-token DeduplicationInfo and
    /// break its row/offset invariant.
    InterpreterInsertQuery interpreter(
        query_ptr,
        insert_context,
        /* allow_materialized */ false,
        /* no_squash */ false,
        /* no_destination */ false,
        /* async_insert */ async_insert);

    block_io = interpreter.execute();

    const auto & nested_header = block_io.pipeline.getHeader();
    if (!blocksHaveEqualStructure(insert_header, nested_header))
    {
        auto converting_dag = ActionsDAG::makeConvertingActions(
            insert_header.getColumnsWithTypeAndName(),
            nested_header.getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            insert_context);
        converting_actions = std::make_shared<ExpressionActions>(std::move(converting_dag));
    }
}

NestedInsertSink::~NestedInsertSink()
{
    /// On cancellation without an exception (e.g. `KILL QUERY`) neither
    /// onFinish() nor onException() runs, leaving the nested executor started but unfinished.
    /// Cancel it so ~PushingPipelineExecutor's finished-or-unwinding invariant holds.
    if (executor)
    {
        try
        {
            executor->cancel();
        }
        catch (...)
        {
            tryLogCurrentException("NestedInsertSink");
        }
    }
}

void NestedInsertSink::onStart()
{
    executor = std::make_unique<PushingPipelineExecutor>(block_io.pipeline);
    executor->start();
}

void NestedInsertSink::consume(Chunk & chunk)
{
    if (chunk.getNumRows() == 0)
        return;

    auto block = getHeader().cloneWithColumns(chunk.getColumns());

    Block block_to_insert;
    for (const auto & column : insert_header)
        block_to_insert.insert(block.getByName(column.name));

    auto chunk_infos = chunk.getChunkInfos().clone();

    /// The nested INSERT re-anchors the deduplication info to its own chunks (its squashing and
    /// `AddDeduplicationInfoTransform` call `updateOriginalBlock`). When this sink is fed by a
    /// dependent materialized view whose inner query changed the number of rows, those chunks
    /// no longer match the rows the info's offsets describe, and computing a data hash after
    /// that re-anchoring would read out of the block's bounds. Cache the hashes now, while the
    /// info is still consistent.
    if (auto deduplication_info = chunk_infos.get<DeduplicationInfo>())
        deduplication_info->cacheDataHashes(data_hash_cache);

    if (converting_actions)
        converting_actions->execute(block_to_insert);

    size_t num_rows = block_to_insert.rows();
    Chunk chunk_to_insert(block_to_insert.getColumns(), num_rows);
    chunk_to_insert.setChunkInfos(std::move(chunk_infos));

    executor->push(std::move(chunk_to_insert));
}

void NestedInsertSink::onFinish()
{
    executor->finish();
    executor.reset();

    block_io.onFinish();
    block_io = {};
}

void NestedInsertSink::onPipelineFinished()
{
    /// Stopped by `timeout_overflow_mode = 'break'`: the nested INSERT finishes as if the input had ended.
    if (isCancelled() && stage != Stage::Exception && executor)
        onFinish();
}

void NestedInsertSink::onException(std::exception_ptr)
{
    if (executor)
        executor->cancel();
    executor.reset();
    block_io.onException();
    block_io = {};
}

}
