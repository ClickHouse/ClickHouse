#include <Processors/Transforms/AggregatingInOrderTransform.h>

#include <utility>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

AggregatingInOrderTransform::AggregatingInOrderTransform(
    Block header, AggregatingTransformParamsPtr params_, SortDescription & sort_description_,
    SortDescription & group_by_description_)
    : IProcessor({std::move(header)}, {params_->getHeader()})
    , params(std::move(params_))
    , sort_description(sort_description_)
    , group_by_description(group_by_description_)
    , key_columns(params->params.keys_size)
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

    res_key_columns.resize(params->params.keys_size);
    res_aggregate_columns.resize(params->params.aggregates_size);

    for (size_t i = 0; i < params->params.keys_size; ++i)
    {
        /// TODO key_columns have low cardinality removed but res_key_columns not
        res_key_columns[i] = res_header.safeGetByPosition(i).type->createColumn();
    }

    for (size_t i = 0; i < params->params.aggregates_size; ++i)
    {
        res_aggregate_columns[i] = params->aggregator.aggregate_functions[i]->getReturnType()->createColumn();
    }
}

AggregatingInOrderTransform::~AggregatingInOrderTransform() = default;

static bool less(const MutableColumns & lhs, const ColumnRawPtrs & rhs, size_t i, size_t j, const SortDescription & descr)
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
/// TODO something broken when there are 10'000'000 rows od data need to investigate
/// TODO maybe move all things inside the Aggregator?

void AggregatingInOrderTransform::consume(Chunk chunk)
{
    /// Find the position of last already read key in current chunk.
    size_t rows = chunk.getNumRows();

    if (rows == 0)
        return;

    size_t mid = 0;
    size_t high = 0;
    size_t low = -1;

    size_t key_end = 0;
    size_t key_begin = 0;

    /// So that key_columns could live longer xD
    /// Need a better construction probably
    Columns materialized_columns;

    AggregateFunctionInstructions aggregate_function_instructions =
        params->aggregator.prepareBlockForAggregation(materialized_columns, chunk.detachColumns(), variants, key_columns, aggregate_columns);

//    std::cerr << "\nPrepared block of size " << rows << "\n";

    if (!res_block_size)
    {
//        std::cerr << "\nCreating first state with key " << key_begin << "\n";
        params->aggregator.createStatesAndFillKeyColumnsWithSingleKey(variants, key_columns, key_begin, res_key_columns);
        ++res_block_size;
    }

    while (key_end != rows)
    {
        high = rows;

        /// Find the first position of new key in current chunk
        while (high - low > 1)
        {
            mid = (low + high) / 2;
//            std::cerr << "Comparing last key and row " << mid << "\n";
            if (!less(res_key_columns, key_columns, res_block_size - 1, mid, group_by_description))
            {
                low = mid;
            }
            else
            {
                high = mid;
            }
        }

        key_end = high;

        if (key_begin != key_end)
        {
//            std::cerr << "Executing from " << key_begin << " to " << key_end << "\n";
            /// Add data to the state if segment is not empty (Empty when we were looking for last key in new block and haven't found it)
            params->aggregator.executeOnIntervalWithoutKeyImpl(variants.without_key, key_begin, key_end, aggregate_function_instructions.data(), variants.aggregates_pool);
        }

        low = key_begin = key_end;

        if (key_begin != rows)
        {
//            std::cerr << "\nFinalizing the last state.\n";
            /// We finalize last key aggregation states if a new key found (Not found if high == rows)
            params->aggregator.fillAggregateColumnsWithSingleKey(variants, res_aggregate_columns);

//            std::cerr << "\nCreating state with key " << key_begin << "\n";
            /// We create a new state for the new key and update res_key_columns
            params->aggregator.createStatesAndFillKeyColumnsWithSingleKey(variants, key_columns, key_begin, res_key_columns);
            ++res_block_size;
        }
    }

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

void AggregatingInOrderTransform::work()
{
    if (is_consume_finished)
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

    /// Last output is current. All other outputs should already be closed.
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

    /// Get chunk from input.
    if (input.isFinished() && !is_consume_finished)
    {
        is_consume_finished = true;
        return Status::Ready;
    }

    if (is_consume_finished)
    {
        /// TODO many blocks
        output.push(std::move(current_chunk));
        output.finish();
        return Status::Finished;
    }

    if (!input.hasData())
    {
        input.setNeeded();
        return Status::NeedData;
    }

    current_chunk = input.pull();
    return Status::Ready;
}


void AggregatingInOrderTransform::generate()
{
//    std::cerr << "\nFinalizing the last state in generate().\n";
    params->aggregator.fillAggregateColumnsWithSingleKey(variants, res_aggregate_columns);

    Block res = params->getHeader().cloneEmpty();

    for (size_t i = 0; i < res_key_columns.size(); ++i)
        res.getByPosition(i).column = std::move(res_key_columns[i]);

    for (size_t i = 0; i < res_aggregate_columns.size(); ++i)
    {
        res.getByPosition(i + res_key_columns.size()).column = std::move(res_aggregate_columns[i]);
    }
    current_chunk = convertToChunk(res);
}

}
