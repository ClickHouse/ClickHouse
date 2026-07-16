#include <Processors/Transforms/TopNAggregatingTransform.h>

#include <Columns/IColumn.h>

namespace DB
{

TopNSortedAggregatingTransform::TopNSortedAggregatingTransform(
    const Block & input_header_,
    const Block & output_header_,
    Aggregator::Params params_,
    const SortDescription & sort_description_,
    size_t limit_)
    : IAccumulatingTransform(
        std::make_shared<const Block>(input_header_),
        std::make_shared<const Block>(output_header_))
    , sort_description(sort_description_)
    , limit(limit_)
    , order_column_index(output_header_.getPositionByName(sort_description_.front().column_name))
    , aggregator(input_header_, params_)
    , key_columns(params_.keys_size)
    , aggregate_columns(params_.aggregates_size)
{
}

void TopNSortedAggregatingTransform::consume(Chunk chunk)
{
    const size_t num_rows = chunk.getNumRows();
    if (num_rows == 0)
        return;

    has_data = true;
    aggregator.executeOnBlock(
        chunk.detachColumns(), 0, num_rows, variants, key_columns, aggregate_columns, no_more_keys);

    /// Early termination: once K distinct groups exist, every group's aggregate is
    /// final (sorted input -> decided by first row) and no unseen group can rank
    /// above them, so stop pulling input.
    if (variants.size() >= limit)
        finishConsume();
}

Chunk TopNSortedAggregatingTransform::generate()
{
    if (generated)
        return {};
    generated = true;

    if (!has_data)
        return {};

    auto aggregated = aggregator.convertToChunks(variants, /*final=*/true);

    /// Collect all aggregated groups into one set of columns (usually a single
    /// chunk; two-level aggregation may produce several), then order by the
    /// ORDER BY aggregate and keep the top `limit`.
    MutableColumns columns = getOutputPort().getHeader().cloneEmptyColumns();
    size_t total_groups = 0;
    for (const auto & aggregated_chunk : aggregated)
    {
        const size_t rows = aggregated_chunk.chunk.getNumRows();
        if (rows == 0)
            continue;
        const auto & src = aggregated_chunk.chunk.getColumns();
        for (size_t i = 0; i < columns.size(); ++i)
            columns[i]->insertRangeFrom(*src[i], 0, rows);
        total_groups += rows;
    }

    if (total_groups == 0)
        return {};

    const size_t output_limit = std::min(limit, total_groups);
    const auto & sort_column = sort_description.front();
    const auto direction = sort_column.direction < 0
        ? IColumn::PermutationSortDirection::Descending
        : IColumn::PermutationSortDirection::Ascending;

    IColumn::Permutation permutation;
    if (sort_column.collator)
        columns[order_column_index]->getPermutationWithCollation(
            *sort_column.collator, direction, IColumn::PermutationSortStability::Unstable,
            output_limit, sort_column.nulls_direction, permutation);
    else
        columns[order_column_index]->getPermutation(
            direction, IColumn::PermutationSortStability::Unstable,
            output_limit, sort_column.nulls_direction, permutation);

    Columns result_columns;
    result_columns.reserve(columns.size());
    for (auto & column : columns)
        result_columns.push_back(column->permute(permutation, output_limit));

    return Chunk(std::move(result_columns), output_limit);
}

}
