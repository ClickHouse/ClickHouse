#include <Processors/Transforms/ScatterByRangeTransform.h>
#include <Processors/Port.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnSparse.h>
#include <Columns/IColumn.h>

namespace DB
{

ScatterByRangeTransform::ScatterByRangeTransform(
    SharedHeader header, size_t output_size_,
    size_t key_column_position_, UInt64 min_value_, UInt64 max_value_)
    : IProcessor(InputPorts{header}, OutputPorts{output_size_, header})
    , output_size(output_size_)
    , key_column_position(key_column_position_)
    , min_value(min_value_)
    , range(max_value_ - min_value_ + 1)
{
}

/// prepare() and work() follow the same pattern as ScatterByPartitionTransform.

IProcessor::Status ScatterByRangeTransform::prepare()
{
    auto & input = getInputs().front();

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
        for (size_t output_idx = 0; output_idx < output_size; ++output_idx, ++output_it)
            if (!was_output_processed[output_idx] && output_it->canPush())
                can_push = true;
        if (!can_push)
            return Status::PortFull;
        return Status::Ready;
    }

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

void ScatterByRangeTransform::work()
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

void ScatterByRangeTransform::generateOutputChunks()
{
    auto num_rows = chunk.getNumRows();
    const auto & columns = chunk.getColumns();

    output_chunks.resize(output_size);

    if (num_rows == 0)
        return;

    auto key_col = removeSpecialRepresentations(columns[key_column_position]);
    const auto & data = assert_cast<const ColumnUInt64 &>(*key_col).getData();

    IColumn::Selector selector(num_rows);
    for (size_t row = 0; row < num_rows; ++row)
    {
        UInt64 value = data[row];
        size_t bucket = (value >= min_value)
            ? std::min(static_cast<size_t>((value - min_value) * output_size / range), output_size - 1)
            : 0;
        selector[row] = bucket;
    }

    for (const auto & column : columns)
    {
        auto filtered_columns = column->scatter(output_size, selector);
        for (size_t output_idx = 0; output_idx < output_size; ++output_idx)
            output_chunks[output_idx].addColumn(std::move(filtered_columns[output_idx]));
    }
}

}
