#include <Processors/Transforms/ScatterByPartitionTransform.h>

#include <Common/PODArray.h>
#include <Core/ColumnNumbers.h>

namespace DB
{
ScatterByPartitionTransform::ScatterByPartitionTransform(Block header, size_t output_size_, ColumnNumbers key_columns_)
    : IProcessor(InputPorts{header}, OutputPorts{output_size_, header})
    , output_size(output_size_)
    , key_columns(std::move(key_columns_))
    , hash(0)
{}

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

    hash.reset(num_rows);

    for (const auto & column_number : key_columns)
        columns[column_number]->updateWeakHash32(hash);

    const auto & hash_data = hash.getData();
    IColumn::Selector selector(num_rows);

    for (size_t row = 0; row < num_rows; ++row)
    {
        selector[row] = hash_data[row]; /// [0, 2^32)
        selector[row] *= output_size; /// [0, output_size * 2^32), selector stores 64 bit values.
        selector[row] >>= 32u; /// [0, output_size)
    }

    output_chunks.resize(output_size);
    for (const auto & column : columns)
    {
        auto filtered_columns = column->scatter(output_size, selector);
        for (size_t i = 0; i < output_size; ++i)
            output_chunks[i].addColumn(std::move(filtered_columns[i]));
    }
}

}
