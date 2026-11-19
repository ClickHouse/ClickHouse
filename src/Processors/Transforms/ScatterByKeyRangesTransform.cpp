#include <Processors/Transforms/ScatterByKeyRangesTransform.h>

#include <Columns/IColumn.h>
#include <Processors/Port.h>
#include <Common/PODArray.h>
#include <Common/VectorWithMemoryTracking.h>

#include <algorithm>

namespace DB
{

namespace
{

/// A lookup pays a fixed price to plan its read, so fewer keys are not worth a lookup of their own.
constexpr size_t min_rows_per_part = 16;

}

ScatterByKeyRangesTransform::ScatterByKeyRangesTransform(SharedHeader header, size_t num_outputs, size_t key_position_)
    : IProcessor(InputPorts{header}, OutputPorts(num_outputs, header))
    , key_position(key_position_)
{
}

IProcessor::Status ScatterByKeyRangesTransform::prepare()
{
    auto & input = inputs.front();

    if (std::ranges::all_of(outputs, [](const auto & output) { return output.isFinished(); }))
    {
        input.close();
        return Status::Finished;
    }

    /// `work` runs without the lock of the executor's graph, so the ports are only touched here.
    if (!pushParts())
    {
        input.setNotNeeded();
        return Status::PortFull;
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
    return Status::Ready;
}

bool ScatterByKeyRangesTransform::pushParts()
{
    for (auto & output : outputs)
    {
        if (parts.empty())
            break;

        if (output.isFinished() || !output.canPush())
            continue;

        output.push(std::move(parts.back()));
        parts.pop_back();
    }

    return parts.empty();
}

void ScatterByKeyRangesTransform::work()
{
    const size_t rows = chunk.getNumRows();
    const size_t num_parts = std::clamp<size_t>(rows / min_rows_per_part, 1, outputs.size());

    /// A chunk that is not split passes as it is, with its `ChunkInfo`.
    if (num_parts == 1)
    {
        parts.push_back(std::move(chunk));
        chunk.clear();
        return;
    }

    IColumn::Permutation permutation;
    chunk.getColumns()[key_position]->getPermutation(
        IColumn::PermutationSortDirection::Ascending, IColumn::PermutationSortStability::Unstable, 0, 1, permutation);

    IColumn::Selector selector(rows);
    for (size_t i = 0; i < rows; ++i)
        selector[permutation[i]] = i * num_parts / rows;

    VectorWithMemoryTracking<MutableColumns> part_columns(num_parts);
    for (const auto & column : chunk.getColumns())
    {
        auto scattered = column->scatter(num_parts, selector);
        for (size_t part = 0; part < num_parts; ++part)
            part_columns[part].push_back(std::move(scattered[part]));
    }
    chunk.clear();

    for (auto & columns : part_columns)
    {
        const size_t part_rows = columns.front()->size();
        parts.emplace_back(std::move(columns), part_rows);
    }
}

}
