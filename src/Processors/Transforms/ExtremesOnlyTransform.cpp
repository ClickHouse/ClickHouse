#include <Processors/Transforms/ExtremesOnlyTransform.h>
#include <Processors/Transforms/ExtremesTransform.h>

namespace DB
{

ExtremesOnlyTransform::ExtremesOnlyTransform(SharedHeader header)
    : IAccumulatingTransform(header, header)
{
}

void ExtremesOnlyTransform::consume(Chunk chunk)
{
    accumulateExtremes(extremes_columns, chunk);
}

Chunk ExtremesOnlyTransform::generate()
{
    /// An empty chunk is how `IAccumulatingTransform` learns to finish, so the accumulator must be
    /// cleared here rather than left in a moved-from state.
    if (extremes_columns.empty())
        return {};

    MutableColumns columns = std::move(extremes_columns);
    extremes_columns.clear();

    Chunk extremes;
    extremes.setColumns(std::move(columns), 2);
    return extremes;
}

}
