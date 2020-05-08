#include <Processors/Transforms/AggregatingInOrderTransform.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{

AggregatingInOrderTransform::AggregatingInOrderTransform(
    Block header, AggregatingTransformParamsPtr params_, SortDescription & sort_description_,
    SortDescription & group_by_description_, size_t max_block_size_)
    : IProcessor({std::move(header)}, {params_->getHeader()})
    , max_block_size(max_block_size_)
    , params(std::move(params_))
    , sort_description(sort_description_)
    , group_by_description(group_by_description_)
    , aggregate_columns(params->params.aggregates_size)
    , many_data(std::make_shared<ManyAggregatedData>(1))
    , variants(*many_data->variants[0])
{
    Block res_header = params->getHeader();

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

static bool less(const MutableColumns & lhs, const Columns & rhs, size_t i, size_t j, const SortDescription & descr)
{
    for (const auto & elem : descr)
    {
        size_t ind = elem.column_number;
        int res = elem.direction * lhs[ind]->compareAt(i, j, *rhs[ind], elem.nulls_direction);
        if (res < 0)
            return true;
        else if (res > 0)
            return false;
    }
    return false;
}


void AggregatingInOrderTransform::consume(Chunk chunk)
{
    /// Find the position of last already read key in current chunk.
    size_t rows = chunk.getNumRows();

    if (rows == 0)
        return;

    Columns materialized_columns;
    Columns key_columns(params->params.keys_size);
    for (size_t i = 0; i < params->params.keys_size; ++i)
    {
        materialized_columns.push_back(chunk.getColumns().at(params->params.keys[i])->convertToFullColumnIfConst());
        key_columns[i] = materialized_columns.back();
    }

    Aggregator::AggregateFunctionInstructions aggregate_function_instructions;
    params->aggregator.prepareAggregateInstructions(chunk.getColumns(), aggregate_columns, materialized_columns, aggregate_function_instructions);

    size_t key_end = 0;
    size_t key_begin = 0;

    if (!res_block_size)
    {
        res_key_columns.resize(params->params.keys_size);
        res_aggregate_columns.resize(params->params.aggregates_size);

        for (size_t i = 0; i < params->params.keys_size; ++i)
        {
            res_key_columns[i] = params->getHeader().safeGetByPosition(i).type->createColumn();
        }

        for (size_t i = 0; i < params->params.aggregates_size; ++i)
        {
            res_aggregate_columns[i] = params->aggregator.aggregate_functions[i]->getReturnType()->createColumn();
        }
        params->aggregator.createStatesAndFillKeyColumnsWithSingleKey(variants, key_columns, key_begin, res_key_columns);
        ++res_block_size;
    }
    size_t mid = 0;
    size_t high = 0;
    size_t low = -1;

    while (key_end != rows)
    {
        high = rows;
        /// Find the first position of new key in current chunk
        while (high - low > 1)
        {
            mid = (low + high) / 2;
            if (!less(res_key_columns, key_columns, res_block_size - 1, mid, group_by_description))
                low = mid;
            else
                high = mid;
        }
        key_end = high;

        if (key_begin != key_end)
        {
            /// Add data to the state if segment is not empty (Empty when we were looking for last key in new block and haven't found it)
            params->aggregator.executeOnIntervalWithoutKeyImpl(variants.without_key, key_begin, key_end, aggregate_function_instructions.data(), variants.aggregates_pool);
        }

        low = key_begin = key_end;

        if (key_begin != rows)
        {
            /// We finalize last key aggregation states if a new key found (Not found if high == rows)
            params->aggregator.fillAggregateColumnsWithSingleKey(variants, res_aggregate_columns);

            if (res_block_size == max_block_size) {
                Columns source_columns = chunk.detachColumns();

                for (auto & source_column : source_columns)
                    source_column = source_column->cut(key_begin, rows - key_begin);

                current_chunk = Chunk(source_columns, rows - key_begin);
                block_end_reached = true;
                need_generate = true;
                res_block_size = 0;
                return;
            }

            /// We create a new state for the new key and update res_key_columns
            params->aggregator.createStatesAndFillKeyColumnsWithSingleKey(variants, key_columns, key_begin, res_key_columns);
            ++res_block_size;
        }
    }
    block_end_reached = false;
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

/// TODO less complicated
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
    if (!block_end_reached)
    {
        if (is_consume_finished)
        {
            output.push(std::move(to_push_chunk));
            output.finish();
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
    current_chunk = input.pull(!is_consume_finished);
    return Status::Ready;
}


/// Convert block to chunk.
/// Adds additional info about aggregation.
Chunk convertToChunk(const Block & block)
{
    auto info = std::make_shared<AggregatedChunkInfo>();
    info->bucket_num = block.info.bucket_num;
    info->is_overflows = block.info.is_overflows;

    UInt64 num_rows = block.rows();
    Chunk chunk(block.getColumns(), num_rows);
    chunk.setChunkInfo(std::move(info));

    return chunk;
}


void AggregatingInOrderTransform::generate()
{
    if (res_block_size && is_consume_finished)
        params->aggregator.fillAggregateColumnsWithSingleKey(variants, res_aggregate_columns);

    LOG_TRACE(log, "Aggregated");
    Block res = params->getHeader().cloneEmpty();

    for (size_t i = 0; i < res_key_columns.size(); ++i)
    {
        res.getByPosition(i).column = std::move(res_key_columns[i]);
    }
    for (size_t i = 0; i < res_aggregate_columns.size(); ++i)
    {
        res.getByPosition(i + res_key_columns.size()).column = std::move(res_aggregate_columns[i]);
    }
    to_push_chunk = convertToChunk(res);
    need_generate = false;
}

}
