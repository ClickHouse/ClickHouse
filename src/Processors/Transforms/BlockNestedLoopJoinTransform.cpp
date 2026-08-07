#include <Columns/ColumnConst.h>
#include <Core/Block.h>
#include <Processors/Transforms/BlockNestedLoopJoinTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

BlockNestedLoopProbeTransform::BlockNestedLoopProbeTransform(
    SharedHeader probe_header_,
    SharedHeader output_header_,
    BlockNestedLoopJoinDataPtr data_,
    BlockNestedLoopPredicate predicate_,
    size_t max_block_size_,
    size_t max_block_bytes_)
    : IProcessor({std::move(probe_header_)}, {std::move(output_header_)})
    , data(std::move(data_))
    , predicate(std::move(predicate_))
    , max_block_size(max_block_size_)
    , max_block_bytes(max_block_bytes_)
{
}

IProcessor::Status BlockNestedLoopProbeTransform::prepare()
{
    auto & output = outputs.front();
    auto & input = inputs.front();

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

    if (output_chunk)
    {
        output.push(std::move(*output_chunk));
        output_chunk.reset();
        return Status::PortFull;
    }

    if (has_input)
        return Status::Ready;

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    input_chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

void BlockNestedLoopProbeTransform::work()
{
    has_input = false;
    /// A probe chunk with no rows has no pair to evaluate; unmatched build rows, which are the only
    /// rows an empty probe side can still produce, are emitted by a stage of their own.
    if (input_chunk.getNumRows() == 0)
        return;

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Block nested loop join is not implemented");
}

BlockNestedLoopTotalsTransform::BlockNestedLoopTotalsTransform(
    SharedHeader probe_header_,
    SharedHeader output_header_,
    BlockNestedLoopJoinDataPtr data_,
    bool probe_totals_are_default_)
    : ISimpleTransform(std::move(probe_header_), std::move(output_header_), /*skip_empty_chunks_=*/ true)
    , data(std::move(data_))
    , probe_totals_are_default(probe_totals_are_default_)
{
}

/// The totals value of the column at `position` of `source`, or the type's default when `source`
/// does not reach that far. `arrayJoin` in the totals expression can make the row count differ
/// from one, so the result is always cut back to a single row.
static ColumnPtr totalsColumnAt(const Block & source, size_t position, const DataTypePtr & type)
{
    if (position >= source.columns())
        return type->createColumnConstWithDefaultValue(1)->convertToFullColumnIfConst();

    auto column = source.getByPosition(position).column->convertToFullColumnIfConst();
    if (column->size() != 1)
        column = column->cloneResized(1);
    return column;
}

void BlockNestedLoopTotalsTransform::transform(Chunk & chunk)
{
    const auto & build_totals = data->getBuildSideTotals();
    if (probe_totals_are_default && build_totals.columns() == 0)
    {
        chunk.clear();
        return;
    }

    const auto & probe_header = getInputPort().getHeader();
    const auto & output_header = getOutputPort().getHeader();
    Block probe_totals = probe_header.cloneWithColumns(chunk.detachColumns());

    Columns columns;
    columns.reserve(output_header.columns());
    for (size_t i = 0; i < output_header.columns(); ++i)
    {
        const auto & target = output_header.getByPosition(i);
        columns.push_back(i < probe_header.columns()
            ? totalsColumnAt(probe_totals, i, target.type)
            : totalsColumnAt(build_totals, i - probe_header.columns(), target.type));
    }

    chunk.setColumns(std::move(columns), 1);
}

}
