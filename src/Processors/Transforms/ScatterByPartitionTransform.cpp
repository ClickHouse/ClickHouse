#include <Columns/ColumnsScatter.h>
#include <Columns/IColumn.h>
#include <Core/ColumnNumbers.h>
#include <Processors/Port.h>
#include <Processors/Transforms/ScatterByPartitionTransform.h>
#include <Common/MapToRange.h>
#include <Common/PODArray.h>

#include <array>

namespace DB
{
ScatterByPartitionTransform::ScatterByPartitionTransform(SharedHeader header, size_t output_size_, ColumnNumbers key_columns_)
    : IProcessor(InputPorts{header}, OutputPorts{output_size_, header})
    , output_size(output_size_)
    , key_columns(std::move(key_columns_))
    , hash(0)
    , pids(0)
    , rows_per_shard(output_size_, 0)
{
}

IProcessor::Status ScatterByPartitionTransform::prepare()
{
    auto & input = getInputs().front();

    /// Check all outputs are finished or ready to get data.

    bool all_finished = true;
    for (auto & output : outputs)
    {
        if (output.isFinished())
            continue;

        all_finished = false;
    }

    if (all_finished)
    {
        input.close();
        return Status::Finished;
    }

    if (!all_outputs_processed)
    {
        auto output_it = outputs.begin();
        bool can_push = false;
        for (size_t i = 0; i < output_size; ++i, ++output_it)
            if (!was_output_processed[i] && output_it->canPush())
                can_push = true;
        if (!can_push)
            return Status::PortFull;
        return Status::Ready;
    }
    /// Try get chunk from input.

    if (input.isFinished())
    {
        for (auto & output : outputs)
            output.finish();

        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    chunk = input.pull();
    has_data = true;
    was_output_processed.assign(outputs.size(), false);

    return Status::Ready;
}

void ScatterByPartitionTransform::work()
{
    if (all_outputs_processed)
        generateOutputChunks();
    all_outputs_processed = true;

    size_t chunk_number = 0;
    for (auto & output : outputs)
    {
        auto & was_processed = was_output_processed[chunk_number];
        auto & output_chunk = output_chunks[chunk_number];
        ++chunk_number;

        if (was_processed)
            continue;

        if (output.isFinished())
            continue;

        if (output_chunk.getNumRows() == 0 && output_chunk.getChunkInfos().empty())
        {
            /// Avoid pushing empty data chunks downstream.
            was_processed = true;
            continue;
        }

        if (!output.canPush())
        {
            all_outputs_processed = false;
            continue;
        }

        output.push(std::move(output_chunk));
        was_processed = true;
    }

    if (all_outputs_processed)
    {
        has_data = false;
        output_chunks.clear();
    }
}

void ScatterByPartitionTransform::generateOutputChunks()
{
    auto num_rows = chunk.getNumRows();
    const auto & columns = chunk.getColumns();

    output_chunks.resize(output_size);

    /// Special case for 0 key columns. It is an unlikely but still valid case.
    if (key_columns.empty())
    {
        /// Put all rows into the first bucket
        if (output_size > 0)
            output_chunks[0] = Chunk(columns, num_rows);
        /// All other buckets are empty
        if (output_size > 1)
        {
            Chunk empty_chunk(chunk.cloneEmptyColumns(), 0);
            for (size_t i = 1; i < output_size; ++i)
                output_chunks[i] = Chunk(empty_chunk.getColumns(), 0);
        }

        return;
    }

    chassert(!columns.empty());

    hash.resize(num_rows);

    bool initial = true;
    for (const auto & column_number : key_columns)
    {
        columns[column_number]->computeHashInto(0, num_rows, hash.data(), initial);
        initial = false;
    }

    pids.resize(num_rows);
    mapToRange(hash.data(), num_rows, static_cast<UInt32>(output_size), pids.data());

    const std::span<const UInt32> pids_span{pids.data(), num_rows};
    const std::array<std::span<const UInt32>, 1> pids_per_source{pids_span};

    std::fill(rows_per_shard.begin(), rows_per_shard.end(), 0);
    ColumnsScatter::countRowsPerShard(pids_per_source, rows_per_shard);

    std::array<const IColumn *, 1> source_columns{};
    for (const auto & column : columns)
    {
        source_columns[0] = column.get();
        MutableColumns scattered = ColumnsScatter::scatter(source_columns, pids_per_source, output_size, rows_per_shard);
        for (size_t i = 0; i < output_size; ++i)
            output_chunks[i].addColumn(std::move(scattered[i]));
    }
}

}
