#include <Processors/Transforms/AggregatingInOrderTransform.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Core/SortCursor.h>
#include <base/range.h>

namespace DB
{

AggregatingInOrderTransform::AggregatingInOrderTransform(
    Block header,
    AggregatingTransformParamsPtr params_,
    const SortDescription & group_by_description_,
    size_t max_block_size_, size_t max_block_bytes_)
    : AggregatingInOrderTransform(std::move(header), std::move(params_),
        group_by_description_, max_block_size_, max_block_bytes_,
        std::make_unique<ManyAggregatedData>(1), 0)
{
}

AggregatingInOrderTransform::AggregatingInOrderTransform(
    Block header, AggregatingTransformParamsPtr params_,
    const SortDescription & group_by_description_,
    size_t max_block_size_, size_t max_block_bytes_,
    ManyAggregatedDataPtr many_data_, size_t current_variant)
    : IProcessor({std::move(header)}, {params_->getCustomHeader(false)})
    , max_block_size(max_block_size_)
    , max_block_bytes(max_block_bytes_)
    , params(std::move(params_))
    , group_by_description(group_by_description_)
    , aggregate_columns(params->params.aggregates_size)
    , many_data(std::move(many_data_))
    , variants(*many_data->variants[current_variant])
{
    /// We won't finalize states in order to merge same states (generated due to multi-thread execution) in AggregatingSortedTransform
    res_header = params->getCustomHeader(false);

    /// Replace column names to column position in description_sorted.
    for (auto & column_description : group_by_description)
    {
        if (!column_description.column_name.empty())
        {
            column_description.column_number = res_header.getPositionByName(column_description.column_name);
            column_description.column_name.clear();
        }
    }
}

AggregatingInOrderTransform::~AggregatingInOrderTransform() = default;

static Int64 getCurrentMemoryUsage()
{
    Int64 current_memory_usage = 0;
    if (auto * memory_tracker = CurrentThread::getMemoryTracker())
        current_memory_usage = memory_tracker->get();
    return current_memory_usage;
}

void AggregatingInOrderTransform::consume(Chunk chunk)
{
    Int64 initial_memory_usage = getCurrentMemoryUsage();

    size_t rows = chunk.getNumRows();
    if (rows == 0)
        return;

    if (!is_consume_started)
    {
        LOG_TRACE(log, "Aggregating in order");
        is_consume_started = true;
    }

    src_rows += rows;
    src_bytes += chunk.bytes();

    Columns materialized_columns;
    Columns key_columns(params->params.keys_size);
    for (size_t i = 0; i < params->params.keys_size; ++i)
    {
        materialized_columns.push_back(chunk.getColumns().at(params->params.keys[i])->convertToFullColumnIfConst());
        key_columns[i] = materialized_columns.back();
    }

    Aggregator::NestedColumnsHolder nested_columns_holder;
    Aggregator::AggregateFunctionInstructions aggregate_function_instructions;
    params->aggregator.prepareAggregateInstructions(chunk.getColumns(), aggregate_columns, materialized_columns, aggregate_function_instructions, nested_columns_holder);

    size_t key_end = 0;
    size_t key_begin = 0;

    /// If we don't have a block we create it and fill with first key
    if (!cur_block_size)
    {
        res_key_columns.resize(params->params.keys_size);
        res_aggregate_columns.resize(params->params.aggregates_size);

        for (size_t i = 0; i < params->params.keys_size; ++i)
            res_key_columns[i] = res_header.safeGetByPosition(i).type->createColumn();

        for (size_t i = 0; i < params->params.aggregates_size; ++i)
            res_aggregate_columns[i] = res_header.safeGetByPosition(i + params->params.keys_size).type->createColumn();

        params->aggregator.createStatesAndFillKeyColumnsWithSingleKey(variants, key_columns, key_begin, res_key_columns);
        params->aggregator.addArenasToAggregateColumns(variants, res_aggregate_columns);
        ++cur_block_size;
    }

    Int64 current_memory_usage = 0;

    /// Will split block into segments with the same key
    while (key_end != rows)
    {
        /// Find the first position of new (not current) key in current chunk
        auto indices = collections::range(key_begin, rows);
        auto it = std::upper_bound(indices.begin(), indices.end(), cur_block_size - 1,
            [&](size_t lhs_row, size_t rhs_row)
            {
                return less(res_key_columns, key_columns, lhs_row, rhs_row, group_by_description);
            });

        key_end = (it == indices.end() ? rows : *it);

        /// Add data to aggr. state if interval is not empty. Empty when haven't found current key in new block.
        if (key_begin != key_end)
            params->aggregator.executeOnIntervalWithoutKeyImpl(variants, key_begin, key_end, aggregate_function_instructions.data(), variants.aggregates_pool);

        current_memory_usage = getCurrentMemoryUsage() - initial_memory_usage;

        /// We finalize last key aggregation state if a new key found.
        if (key_end != rows)
        {
            params->aggregator.addSingleKeyToAggregateColumns(variants, res_aggregate_columns);

            /// If max_block_size is reached we have to stop consuming and generate the block. Save the extra rows into new chunk.
            if (cur_block_size >= max_block_size || cur_block_bytes + current_memory_usage >= max_block_bytes)
            {
                cur_block_bytes += current_memory_usage;
                finalizeCurrentChunk(std::move(chunk), key_end);
                return;
            }

            /// We create a new state for the new key and update res_key_columns
            params->aggregator.createStatesAndFillKeyColumnsWithSingleKey(variants, key_columns, key_end, res_key_columns);
            ++cur_block_size;
        }

        key_begin = key_end;
    }

    cur_block_bytes += current_memory_usage;
    block_end_reached = false;
}

void AggregatingInOrderTransform::finalizeCurrentChunk(Chunk chunk, size_t key_end)
{
    size_t rows = chunk.getNumRows();
    Columns source_columns = chunk.detachColumns();

    for (auto & source_column : source_columns)
        source_column = source_column->cut(key_end, rows - key_end);

    current_chunk = Chunk(source_columns, rows - key_end);
    src_rows -= current_chunk.getNumRows();

    block_end_reached = true;
    need_generate = true;
    variants.without_key = nullptr;
}

void AggregatingInOrderTransform::work()
{
    if (is_consume_finished || need_generate)
    {
        generate();
    }
    else
    {
        consume(std::move(current_chunk));
    }
}


IProcessor::Status AggregatingInOrderTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.back();

    /// Check can output.
    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (block_end_reached)
    {
        if (need_generate)
        {
            return Status::Ready;
        }
        else
        {
            output.push(std::move(to_push_chunk));
            return Status::Ready;
        }
    }
    else
    {
        if (is_consume_finished)
        {
            output.push(std::move(to_push_chunk));
            output.finish();
            LOG_DEBUG(log, "Aggregated. {} to {} rows (from {})",
                src_rows, res_rows, formatReadableSizeWithBinarySuffix(src_bytes));
            return Status::Finished;
        }

        if (input.isFinished())
        {
            is_consume_finished = true;
            return Status::Ready;
        }
    }
    if (!input.hasData())
    {
        input.setNeeded();
        return Status::NeedData;
    }

    assert(!is_consume_finished);
    current_chunk = input.pull(true /* set_not_needed */);
    convertToFullIfSparse(current_chunk);
    return Status::Ready;
}

void AggregatingInOrderTransform::generate()
{
    if (cur_block_size && is_consume_finished)
    {
        params->aggregator.addSingleKeyToAggregateColumns(variants, res_aggregate_columns);
        variants.without_key = nullptr;
    }

    Block res = res_header.cloneEmpty();

    for (size_t i = 0; i < res_key_columns.size(); ++i)
        res.getByPosition(i).column = std::move(res_key_columns[i]);

    for (size_t i = 0; i < res_aggregate_columns.size(); ++i)
        res.getByPosition(i + res_key_columns.size()).column = std::move(res_aggregate_columns[i]);

    to_push_chunk = convertToChunk(res);
    if (!to_push_chunk.getNumRows())
        return;

    /// Clear arenas to allow to free them, when chunk will reach the end of pipeline.
    /// It's safe clear them here, because columns with aggregate functions already holds them.
    variants.aggregates_pools = { std::make_shared<Arena>() };
    variants.aggregates_pool = variants.aggregates_pools.at(0).get();

    /// Pass info about used memory by aggregate functions further.
    to_push_chunk.setChunkInfo(std::make_shared<ChunkInfoWithAllocatedBytes>(cur_block_bytes));

    cur_block_bytes = 0;
    cur_block_size = 0;

    res_rows += to_push_chunk.getNumRows();
    need_generate = false;
}


}
