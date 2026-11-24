#include <Processors/Transforms/finalizeChunk.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Core/Block.h>
#include <DataTypes/IDataType.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
}

ColumnsMask getAggregatesMask(const Block & header, const AggregateDescriptions & aggregates)
{
    ColumnsMask mask(header.columns());
    for (const auto & aggregate : aggregates)
        mask[header.getPositionByName(aggregate.column_name)] = true;
    return mask;
}

void finalizeChunk(Chunk & chunk, const ColumnsMask & aggregates_mask, const Block & finalized_header)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    for (size_t i = 0; i < columns.size(); ++i)
    {
        if (!aggregates_mask[i])
            continue;

        auto & column = columns[i];

        /// A column is finalized with its own function, which may differ from the one of the type in the header
        /// if they share the state representation (like `quantile` and `quantiles`): `DataTypeAggregateFunction::equals`
        /// considers such types equal, so such a column can pass through a `Union`. Their result types may differ.
        const auto & column_aggregate_function = typeid_cast<const ColumnAggregateFunction &>(*column);
        const auto & actual_result_type = column_aggregate_function.getAggregateFunction()->getResultType();
        const auto & expected = finalized_header.getByPosition(i);
        if (!actual_result_type->equals(*expected.type))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                "Cannot finalize the state of aggregate function {} in column {}: it finalizes to {}, but {} is expected. "
                "States of different aggregate functions that share a state representation cannot be finalized together",
                column_aggregate_function.getAggregateFunction()->getName(), expected.name,
                actual_result_type->getName(), expected.type->getName());

        column = ColumnAggregateFunction::convertToValues(IColumn::mutate(std::move(column)));
    }

    chunk.setColumns(std::move(columns), num_rows);
}

}
