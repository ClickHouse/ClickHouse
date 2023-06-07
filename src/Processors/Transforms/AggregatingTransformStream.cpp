#include <Processors/Transforms/AggregatingTransformStream.h>

#include <Formats/NativeReader.h>
#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>
#include <Processors/Transforms/MergingAggregatedMemoryEfficientTransform.h>
#include <Core/ProtocolDefines.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_AGGREGATED_DATA_VARIANT;
    extern const int LOGICAL_ERROR;
}

AggregatingTransformStream::AggregatingTransformStream(Block header, AggregatingTransformParamsPtr params_)
    : IProcessor({std::move(header)}, {params_->getHeader()})
    , params(std::move(params_))
    , many_data(std::make_unique<ManyAggregatedData>(1))
    , variants(*many_data->variants[1])
{
}

AggregatingTransformStream::~AggregatingTransformStream() = default;


void AggregatingTransformStream::work()
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

void AggregatingTransformStream::generate()
{
    if (cur_block_size && is_consume_finished)
    {
        params->aggregator.addSingleKeyToAggregateColumns(variants, res_aggregate_columns);
        variants.invalidate();
    }

    if (is_consume_finished)
    {
        Block res = res_header.cloneEmpty();
        to_push_chunk = convertToChunk(res);
    }

    if (!to_push_chunk.getNumRows())
        return;

    variants.aggregates_pools = { std::make_shared<Arena>() };
    variants.aggregates_pool = variants.aggregates_pools.at(0).get();
    to_push_chunk.setChunkInfo(std::make_shared<ChunkInfoAccocate>(cur_block_bytes));
    cur_block_bytes = 0;
    cur_block_size = 0;
    res_rows += to_push_chunk.getNumRows();
    need_generate = false;
}

void AggregatingTransformStream::consume(Chunk chunk)
{
    size_t key_end = 0;
    size_t key_begin = 0;
    const UInt64 num_rows = chunk.getNumRows();
    Aggregator::AggregateFunctionInstructions aggregate_function_instructions;

    const Columns & columns = chunk.getColumns();
    Int64 initial_memory_usage = CurrentThread::getMemoryTracker() == nullptr ? 0 : CurrentThread::getMemoryTracker()->get();

    if (chunk.getNumRows() == 0)
        return;

    if (!is_consume_started)
        is_consume_started = true;

    Columns key_columns(params->params.keys_size);
      if (!cur_block_size)
    {
        res_key_columns.resize(params->params.keys_size);
        for (size_t i = 0; i < params->params.keys_size; ++i)
            res_key_columns[i] = res_header.safeGetByPosition(i).type->createColumn();

        params->aggregator.createStatesAndFillKeyColumnsWithSingleKey(variants, key_columns, key_begin, res_key_columns);
        res_aggregate_columns.resize(params->params.aggregates_size);
        for (size_t i = 0; i < params->params.aggregates_size; ++i)
            res_aggregate_columns[i] = res_header.safeGetByPosition(i + params->params.keys_size).type->createColumn();

        params->aggregator.addArenasToAggregateColumns(variants, res_aggregate_columns);
        ++cur_block_size;
    }

    Int64 current_memory_usage = 0;

    Aggregator::AggregateColumnsConstData aggregate_columns_data(params->params.aggregates_size);
    if (params->params.only_merge)
    {
        for (size_t i = 0, j = 0; i < columns.size(); ++i)
        {
            if (!aggregates_mask[i])
                continue;
            aggregate_columns_data[j++] = &typeid_cast<const ColumnAggregateFunction &>(*columns[i]).getData();
        }
    }

    while (key_end != num_rows)
    {
        key_end = num_rows; 
        if (key_begin != key_end)
        {
            if (params->params.only_merge)
            {
                params->aggregator.mergeOnIntervalWithoutKeyImpl(variants, key_begin, key_end, aggregate_columns_data);
            }
            else
            {
                params->aggregator.executeOnIntervalWithoutKeyImpl(variants, key_begin, key_end, aggregate_function_instructions.data());
            }
        }

        current_memory_usage = std::max<Int64>(CurrentThread::getMemoryTracker() == nullptr ? 0 : CurrentThread::getMemoryTracker()->get() - initial_memory_usage, 0);

        if (key_end != num_rows)
        {
            params->aggregator.addSingleKeyToAggregateColumns(variants, res_aggregate_columns);

            if (cur_block_size >= max_block_size || cur_block_bytes + current_memory_usage >= max_block_bytes)
            {
                cur_block_bytes += current_memory_usage;
                Columns source_columns = chunk.detachColumns();

                for (auto & source_column : source_columns)
                    source_column = source_column->cut(key_end, chunk.getNumRows() - key_end);

                current_chunk = Chunk(source_columns, chunk.getNumRows() - key_end);
                block_end_reached = true;
                need_generate = true;
                variants.invalidate();
                return;
            }
            params->aggregator.createStatesAndFillKeyColumnsWithSingleKey(variants, key_columns, key_end, res_key_columns);
            ++cur_block_size;
        }

        key_begin = key_end;
    }

    cur_block_bytes += current_memory_usage;
    block_end_reached = false;

}

IProcessor::Status AggregatingTransformStream::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.back();

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
    current_chunk = input.pull(true);
    convertToFullIfSparse(current_chunk);
    return Status::Ready;
}

}
