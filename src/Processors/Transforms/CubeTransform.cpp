#include <Processors/Transforms/CubeTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include "Processors/Transforms/RollupTransform.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int TOO_MANY_COLUMNS;
}

CubeTransform::CubeTransform(Block header, AggregatingTransformParamsPtr params_, bool use_nulls_)
    : GroupByModifierTransform(std::move(header), params_, use_nulls_)
    , aggregates_mask(getAggregatesMask(params->getHeader(), params->params.aggregates))
{
    if (keys.size() >= 8 * sizeof(mask))
        throw Exception(ErrorCodes::TOO_MANY_COLUMNS, "Too many keys ({}) are used for CubeTransform, the maximum is {}.", keys.size(), 8 * sizeof(mask) - 1);
}

Chunk CubeTransform::generate()
{
    if (!consumed_chunks.empty())
    {
        mergeConsumed();

        auto num_rows = current_chunk.getNumRows();
        mask = (static_cast<UInt64>(1) << keys.size()) - 1;

        current_columns = current_chunk.getColumns();
        current_zero_columns.clear();
        current_zero_columns.reserve(keys.size());

        for (auto key : keys)
            current_zero_columns.emplace_back(getColumnWithDefaults(key, num_rows));
    }

    auto gen_chunk = std::move(current_chunk);

    if (mask)
    {
        --mask;

        auto columns = current_columns;
        auto size = keys.size();
        for (size_t i = 0; i < size; ++i)
            /// Reverse bit order to support previous behaviour.
            if ((mask & (UInt64(1) << (size - i - 1))) == 0)
                columns[keys[i]] = current_zero_columns[i];

        Chunks chunks;
        chunks.emplace_back(std::move(columns), current_columns.front()->size());
        current_chunk = merge(std::move(chunks), !use_nulls, false);
    }

    finalizeChunk(gen_chunk, aggregates_mask);
    if (!gen_chunk.empty())
        gen_chunk.addColumn(0, ColumnUInt64::create(gen_chunk.getNumRows(), grouping_set++));
    return gen_chunk;
}

}
