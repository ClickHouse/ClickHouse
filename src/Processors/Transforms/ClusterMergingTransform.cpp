#include <Processors/Transforms/ClusterMergingTransform.h>

#include <Columns/ColumnAggregateFunction.h>
#include <Columns/IColumn.h>
#include <Common/Arena.h>
#include <Processors/Port.h>

#include <algorithm>
#include <numeric>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ClusterMergingTransform::ClusterMergingTransform(
    SharedHeader header_,
    AggregatingTransformParamsPtr params_,
    String cluster_key_name_,
    Float64 cluster_distance_)
    : IAccumulatingTransform(header_, header_)
    , params(std::move(params_))
    , cluster_key_name(std::move(cluster_key_name_))
    , cluster_distance(cluster_distance_)
    , aggregates_mask(getAggregatesMask(input.getHeader(), params->params.aggregates))
{
}

void ClusterMergingTransform::consume(Chunk chunk)
{
    consumed_chunks.emplace_back(std::move(chunk));
}

Chunk ClusterMergingTransform::generate()
{
    if (generated)
        return {};

    generated = true;

    if (consumed_chunks.empty())
        return {};

    /// Concatenate all chunks into a single block
    const auto & header = input.getHeader();
    size_t num_columns = header.columns();

    MutableColumns merged_columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        merged_columns[i] = header.getByPosition(i).column->cloneEmpty();

    size_t total_rows = 0;
    for (auto & chunk : consumed_chunks)
    {
        auto chunk_columns = chunk.detachColumns();
        for (size_t i = 0; i < num_columns; ++i)
            merged_columns[i]->insertRangeFrom(*chunk_columns[i], 0, chunk_columns[i]->size());
        total_rows += chunk_columns[0]->size();
    }
    consumed_chunks.clear();

    if (total_rows == 0)
        return {};

    /// Find column positions
    size_t cluster_key_pos = header.getPositionByName(cluster_key_name);

    /// Build a list of all key column positions (non-aggregate columns)
    std::vector<size_t> all_key_positions;
    std::vector<size_t> non_cluster_key_positions;
    for (size_t i = 0; i < num_columns; ++i)
    {
        if (!aggregates_mask[i])
        {
            all_key_positions.push_back(i);
            if (i != cluster_key_pos)
                non_cluster_key_positions.push_back(i);
        }
    }

    /// Create a permutation: sort by non-cluster keys first, then cluster key
    IColumn::Permutation perm(total_rows);
    std::iota(perm.begin(), perm.end(), 0);

    std::sort(perm.begin(), perm.end(), [&](size_t a, size_t b)
    {
        /// Compare non-cluster keys first
        for (size_t pos : non_cluster_key_positions)
        {
            int cmp = merged_columns[pos]->compareAt(a, b, *merged_columns[pos], 1);
            if (cmp != 0)
                return cmp < 0;
        }
        /// Then compare by cluster key
        return merged_columns[cluster_key_pos]->compareAt(a, b, *merged_columns[cluster_key_pos], 1) < 0;
    });

    /// Apply permutation to all columns
    for (size_t i = 0; i < num_columns; ++i)
    {
        auto permuted = merged_columns[i]->permute(perm, 0);
        merged_columns[i] = IColumn::mutate(std::move(permuted));
    }

    /// Now scan sorted data and merge adjacent rows within distance.
    /// We use a "leader" approach: for each group of adjacent rows with the same
    /// non-cluster keys and cluster key values within distance, we merge into the first row.

    /// Mark which rows to keep (leaders of clusters)
    std::vector<bool> keep(total_rows, true);
    /// For each row, track which leader it belongs to
    std::vector<size_t> leader(total_rows);
    std::iota(leader.begin(), leader.end(), 0);

    for (size_t i = 1; i < total_rows; ++i)
    {
        size_t prev_leader = leader[i - 1];

        /// Check if non-cluster keys match the previous leader
        bool same_non_cluster = true;
        for (size_t pos : non_cluster_key_positions)
        {
            if (merged_columns[pos]->compareAt(i, prev_leader, *merged_columns[pos], 1) != 0)
            {
                same_non_cluster = false;
                break;
            }
        }

        if (!same_non_cluster)
            continue;

        /// Check if cluster key is within distance of the PREVIOUS row (not the leader).
        /// This implements the "chain" semantics: each element is within distance of its neighbor.
        Float64 prev_value = merged_columns[cluster_key_pos]->getFloat64(i - 1);
        Float64 curr_value = merged_columns[cluster_key_pos]->getFloat64(i);
        Float64 diff = curr_value - prev_value;
        if (diff < 0)
            diff = -diff;

        if (diff <= cluster_distance)
        {
            /// Merge row i into the leader
            leader[i] = prev_leader;
            keep[i] = false;

            /// Merge aggregate states
            for (size_t col_idx = 0; col_idx < num_columns; ++col_idx)
            {
                if (!aggregates_mask[col_idx])
                    continue;

                auto * agg_col = typeid_cast<ColumnAggregateFunction *>(merged_columns[col_idx].get());
                if (!agg_col)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected ColumnAggregateFunction");

                auto & data = agg_col->getData();
                const auto & func = agg_col->getAggregateFunction();

                /// Merge state of row i into the leader's state
                func->merge(data[prev_leader], data[i], /*arena=*/nullptr);
            }
        }
    }

    /// Build result columns keeping only leader rows
    size_t result_rows = std::count(keep.begin(), keep.end(), true);
    MutableColumns result_columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        result_columns[i] = merged_columns[i]->cloneEmpty();

    for (size_t i = 0; i < total_rows; ++i)
    {
        if (keep[i])
        {
            for (size_t col_idx = 0; col_idx < num_columns; ++col_idx)
                result_columns[col_idx]->insertFrom(*merged_columns[col_idx], i);
        }
    }

    Chunk result(std::move(result_columns), result_rows);

    /// Finalize aggregate functions
    finalizeChunk(result, aggregates_mask);

    return result;
}

}
