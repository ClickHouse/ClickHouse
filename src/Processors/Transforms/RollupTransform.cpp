#include <Processors/Transforms/RollupTransform.h>
#include <Processors/Transforms/TotalsHavingTransform.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Columns/ColumnNullable.h>

namespace DB
{

GroupByModifierTransform::GroupByModifierTransform(SharedHeader header, AggregatingTransformParamsPtr params_, bool use_nulls_)
    : IAccumulatingTransform(std::move(header), std::make_shared<const Block>(generateOutputHeader(params_->getHeader(), params_->params.keys, use_nulls_)))
    , params(std::move(params_))
    , use_nulls(use_nulls_)
{
    keys.reserve(params->params.keys_size);
    for (const auto & key : params->params.keys)
        keys.emplace_back(input.getHeader().getPositionByName(key));

    intermediate_header = getOutputPort().getHeader();
    intermediate_header.erase(0);

    if (use_nulls)
    {
        auto output_aggregator_params = params->params;
        output_aggregator = std::make_unique<Aggregator>(intermediate_header, output_aggregator_params);
    }
}

void GroupByModifierTransform::consume(Chunk chunk)
{
    consumed_chunks.emplace_back(std::move(chunk));
}

void GroupByModifierTransform::mergeConsumed()
{
    if (consumed_chunks.size() > 1)
        current_chunk = merge(std::move(consumed_chunks), true, false);
    else
        current_chunk = std::move(consumed_chunks.front());

    size_t rows = current_chunk.getNumRows();
    auto columns = current_chunk.getColumns();
    if (use_nulls)
    {
        for (auto key : keys)
            columns[key] = makeNullableOrLowCardinalityNullableSafe(columns[key]);
    }
    current_chunk = Chunk{ columns, rows };

    consumed_chunks.clear();
}

Chunk GroupByModifierTransform::merge(Chunks && chunks, bool is_input, bool final)
{
    auto header = is_input ? getInputPort().getHeader() : intermediate_header;

    Aggregator::AggregatedChunks agg_chunks;
    for (auto & chunk : chunks)
        agg_chunks.emplace_back(std::move(chunk));

    auto & aggregator = is_input ? params->aggregator : *output_aggregator;
    auto result = aggregator.mergeBlocks(agg_chunks, final, is_cancelled, /* dataflow_cache_updater= */ nullptr);
    return std::move(result.chunk);
}

MutableColumnPtr GroupByModifierTransform::getColumnWithDefaults(size_t key, size_t n) const
{
    auto const & col = intermediate_header.getByPosition(key);
    auto result_column = col.column->cloneEmpty();
    col.type->insertManyDefaultsInto(*result_column, n);
    return result_column;
}

RollupTransform::RollupTransform(
    SharedHeader header, AggregatingTransformParamsPtr params_, bool use_nulls_, const std::vector<size_t> & key_positions_)
    : GroupByModifierTransform(std::move(header), params_, use_nulls_)
    , aggregates_mask(getAggregatesMask(params->getHeader(), params->params.aggregates))
{
    num_group_by_elements = key_positions_.empty() ? keys.size() : key_positions_.size();

    /// A prefix holds a key iff it reaches any position that key was written at, so the key leaves
    /// the prefix only once it has shrunk past the *first* of them. Shrinking past a later position
    /// of a repeated key drops nothing and re-emits the same grouping set, which is what the list
    /// as written asks for.
    key_dropped_at_position.assign(num_group_by_elements, no_key);
    std::vector<bool> already_seen(keys.size(), false);
    for (size_t position = 0; position < num_group_by_elements; ++position)
    {
        const size_t key = key_positions_.empty() ? position : key_positions_[position];
        if (!already_seen[key])
        {
            already_seen[key] = true;
            key_dropped_at_position[position] = key;
        }
    }
}

Chunk RollupTransform::generate()
{
    if (!consumed_chunks.empty())
    {
        mergeConsumed();
        last_removed_position = num_group_by_elements;
        set_counter = 0;
    }

    auto gen_chunk = std::move(current_chunk);
    /// `set_counter` counts the keys dropped so far, which is the number the emitted chunk carries.
    const UInt64 gen_set = set_counter;

    if (last_removed_position)
    {
        --last_removed_position;

        auto num_rows = gen_chunk.getNumRows();
        auto columns = gen_chunk.getColumns();

        if (const size_t key_to_drop = key_dropped_at_position[last_removed_position]; key_to_drop != no_key)
        {
            auto key = keys[key_to_drop];
            columns[key] = getColumnWithDefaults(key, num_rows);
            ++set_counter;
        }

        Chunks chunks;
        chunks.emplace_back(std::move(columns), num_rows);
        current_chunk = merge(std::move(chunks), !use_nulls, false);
    }

    finalizeChunk(gen_chunk, aggregates_mask);
    if (!gen_chunk.empty())
        gen_chunk.addColumn(0, ColumnUInt64::create(gen_chunk.getNumRows(), gen_set));
    return gen_chunk;
}

}
