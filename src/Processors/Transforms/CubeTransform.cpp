#include <Processors/Transforms/CubeTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/Transforms/RollupTransform.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int TOO_MANY_COLUMNS;
}

CubeTransform::CubeTransform(
    SharedHeader header, AggregatingTransformParamsPtr params_, bool use_nulls_, const std::vector<size_t> & key_positions_)
    : GroupByModifierTransform(std::move(header), params_, use_nulls_)
    , aggregates_mask(getAggregatesMask(params->getHeader(), params->params.aggregates))
{
    num_group_by_elements = key_positions_.empty() ? keys.size() : key_positions_.size();

    /// The power set is taken over the GROUP BY list as written, so the limit counts its elements:
    /// a repeated key adds a position, and so doubles the number of subsets, without adding a key.
    if (num_group_by_elements >= 8 * sizeof(mask))
        throw Exception(
            ErrorCodes::TOO_MANY_COLUMNS,
            "Too many keys ({}) are used for CubeTransform, the maximum is {}.",
            num_group_by_elements,
            8 * sizeof(mask) - 1);

    const size_t num_keys = keys.size();
    position_masks.assign(num_keys, 0);
    for (size_t position = 0; position < num_group_by_elements; ++position)
    {
        const size_t key = key_positions_.empty() ? position : key_positions_[position];
        /// Bit order is reversed to keep the order the subsets were emitted in before.
        position_masks[key] |= UInt64(1) << (num_group_by_elements - position - 1);
    }
}

UInt64 CubeTransform::groupingSetForMask(UInt64 position_mask) const
{
    /// `__grouping_set` numbers the subsets of the *deduplicated* keys, which is what the `GROUPING`
    /// implementations index into, so several emitted subsets can share a number when a key repeats.
    const size_t num_keys = keys.size();
    UInt64 kept = 0;
    for (size_t key = 0; key < num_keys; ++key)
        if (position_mask & position_masks[key])
            kept |= UInt64(1) << (num_keys - key - 1);

    return ((UInt64(1) << num_keys) - 1) - kept;
}

Chunk CubeTransform::generate()
{
    if (!consumed_chunks.empty())
    {
        mergeConsumed();

        auto num_rows = current_chunk.getNumRows();
        mask = (static_cast<UInt64>(1) << num_group_by_elements) - 1;

        current_columns = current_chunk.getColumns();
        current_zero_columns.clear();
        current_zero_columns.reserve(keys.size());

        for (auto key : keys)
            current_zero_columns.emplace_back(getColumnWithDefaults(key, num_rows));
    }

    auto gen_chunk = std::move(current_chunk);
    /// `mask` still describes the chunk being emitted; it is advanced below to build the next one.
    const UInt64 gen_mask = mask;

    if (mask)
    {
        --mask;

        auto columns = current_columns;
        const size_t num_keys = keys.size();
        for (size_t i = 0; i < num_keys; ++i)
            if ((mask & position_masks[i]) == 0)
                columns[keys[i]] = current_zero_columns[i];

        Chunks chunks;
        chunks.emplace_back(std::move(columns), current_columns.front()->size());
        current_chunk = merge(std::move(chunks), !use_nulls, false);
    }

    finalizeChunk(gen_chunk, aggregates_mask);
    if (!gen_chunk.empty())
        gen_chunk.addColumn(0, ColumnUInt64::create(gen_chunk.getNumRows(), groupingSetForMask(gen_mask)));
    return gen_chunk;
}

}
