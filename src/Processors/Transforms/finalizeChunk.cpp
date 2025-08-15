#include <Processors/Transforms/finalizeChunk.h>
#include <Columns/ColumnAggregateFunction.h>

namespace DB
{

ColumnsMask getAggregatesMask(const Block & header, const AggregateDescriptions & aggregates)
{
    ColumnsMask mask(header.columns());
    for (const auto & aggregate : aggregates)
        mask[header.getPositionByName(aggregate.column_name)] = true;
    return mask;
}

void finalizeChunk(Chunk & chunk, const ColumnsMask & aggregates_mask)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (!aggregates_mask[i])
            continue;

        auto & column = columns[i];
        column = ColumnAggregateFunction::convertToValues(IColumn::mutate(std::move(column)));
    }

    chunk.setColumns(std::move(columns), num_rows);
}

}
