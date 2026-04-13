#include <Processors/Transforms/ClusterMergingTransform.h>

#include <Columns/ColumnAggregateFunction.h>
#include <Columns/IColumn.h>
#include <Common/Arena.h>
#include <Common/SipHash.h>
#include <Processors/Port.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <numeric>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// State of one bucket during the bucket-reduction phase.
struct BucketState
{
    size_t leader_row_index;   /// Index in merged_columns where aggregate states live
    Int64 bucket_id;
    Float64 min_cluster_key;
    Float64 max_cluster_key;
    bool alive = true;         /// False if merged into another bucket
};

/// Merge aggregate states of row `src` into row `dst` in merged_columns.
void mergeAggregateStates(
    MutableColumns & merged_columns,
    const ColumnsMask & aggregates_mask,
    size_t dst,
    size_t src)
{
    for (size_t col_idx = 0; col_idx < merged_columns.size(); ++col_idx)
    {
        if (!aggregates_mask[col_idx])
            continue;

        auto * agg_col = typeid_cast<ColumnAggregateFunction *>(merged_columns[col_idx].get());
        if (!agg_col)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected ColumnAggregateFunction");

        auto & data = agg_col->getData();
        const auto & func = agg_col->getAggregateFunction();

        func->merge(data[dst], data[src], /*arena=*/nullptr);
    }
}

/// Compute a 64-bit hash of (non_cluster_key_values..., bucket_id) for a given row.
UInt64 computeBucketHash(
    const MutableColumns & merged_columns,
    const std::vector<size_t> & non_cluster_key_positions,
    size_t row,
    Int64 bucket_id)
{
    SipHash hash;
    for (size_t pos : non_cluster_key_positions)
        merged_columns[pos]->updateHashWithValue(row, hash);
    hash.update(bucket_id);
    return hash.get64();
}

}

ClusterMergingTransform::ClusterMergingTransform(
    SharedHeader header_,
    AggregatingTransformParamsPtr params_,
    String cluster_key_name_,
    Float64 cluster_distance_)
    : IAccumulatingTransform(header_, std::make_shared<const Block>(params_->getHeader()))
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

    /// Build a list of key column positions (non-aggregate columns)
    std::vector<size_t> non_cluster_key_positions;
    for (size_t i = 0; i < num_columns; ++i)
    {
        if (!aggregates_mask[i] && i != cluster_key_pos)
            non_cluster_key_positions.push_back(i);
    }

    /// --- Phase A: Bucket reduction ---
    /// For distance == 0, each unique value is its own bucket (no merging across values).
    /// For distance > 0, bucket_id = floor(cluster_key / distance) groups nearby values.
    /// All values within the same bucket differ by at most distance-1 < distance,
    /// so they unconditionally belong to the same cluster.

    std::vector<BucketState> buckets;
    std::unordered_map<UInt64, size_t> bucket_map; /// hash → index in buckets vector

    for (size_t i = 0; i < total_rows; ++i)
    {
        Float64 cluster_val = merged_columns[cluster_key_pos]->getFloat64(i);

        Int64 bucket_id;
        if (cluster_distance > 0)
            bucket_id = static_cast<Int64>(std::floor(cluster_val / cluster_distance));
        else
        {
            /// distance == 0: each distinct value is its own bucket.
            /// Use memcpy to reinterpret Float64 bits as Int64,
            /// so identical values get the same bucket_id.
            static_assert(sizeof(Float64) == sizeof(Int64));
            std::memcpy(&bucket_id, &cluster_val, sizeof(Int64));
        }

        UInt64 hash = computeBucketHash(merged_columns, non_cluster_key_positions, i, bucket_id);

        auto it = bucket_map.find(hash);
        if (it != bucket_map.end())
        {
            /// Existing bucket: merge aggregate states, update min/max
            auto & bucket = buckets[it->second];
            mergeAggregateStates(merged_columns, aggregates_mask, bucket.leader_row_index, i);
            bucket.min_cluster_key = std::min(bucket.min_cluster_key, cluster_val);
            bucket.max_cluster_key = std::max(bucket.max_cluster_key, cluster_val);
        }
        else
        {
            /// New bucket
            size_t idx = buckets.size();
            buckets.push_back({i, bucket_id, cluster_val, cluster_val, true});
            bucket_map[hash] = idx;
        }
    }

    /// --- Phase B: Sort buckets and merge adjacent ones ---
    /// Sort by (non_cluster_keys, bucket_id)
    std::vector<size_t> bucket_order(buckets.size());
    std::iota(bucket_order.begin(), bucket_order.end(), 0);

    std::sort(bucket_order.begin(), bucket_order.end(), [&](size_t a, size_t b)
    {
        size_t row_a = buckets[a].leader_row_index;
        size_t row_b = buckets[b].leader_row_index;

        /// Compare non-cluster keys first
        for (size_t pos : non_cluster_key_positions)
        {
            int cmp = merged_columns[pos]->compareAt(row_a, row_b, *merged_columns[pos], 1);
            if (cmp != 0)
                return cmp < 0;
        }
        /// Then compare by bucket_id
        return buckets[a].bucket_id < buckets[b].bucket_id;
    });

    /// Adjacent bucket merging is only needed when distance > 0.
    /// With distance == 0, Phase A already merged all identical values,
    /// and no cross-bucket merging should occur.
    if (cluster_distance > 0)
    {
        /// Linear scan: merge adjacent buckets where non_cluster_keys match
        /// and max_cluster_key of the leader is within distance of min_cluster_key of the next.
        for (size_t i = 1; i < bucket_order.size(); ++i)
        {
            size_t curr_idx = bucket_order[i];
            auto & curr = buckets[curr_idx];

            /// Find the previous alive bucket
            size_t prev_alive_pos = i - 1;
            while (prev_alive_pos < bucket_order.size() && !buckets[bucket_order[prev_alive_pos]].alive)
            {
                if (prev_alive_pos == 0)
                    break;
                --prev_alive_pos;
            }

            if (prev_alive_pos >= bucket_order.size() || !buckets[bucket_order[prev_alive_pos]].alive)
                continue;

            size_t prev_idx = bucket_order[prev_alive_pos];
            auto & prev = buckets[prev_idx];

            /// Check if non-cluster keys match
            bool same_non_cluster = true;
            for (size_t pos : non_cluster_key_positions)
            {
                if (merged_columns[pos]->compareAt(prev.leader_row_index, curr.leader_row_index, *merged_columns[pos], 1) != 0)
                {
                    same_non_cluster = false;
                    break;
                }
            }

            if (!same_non_cluster)
                continue;

            /// Check if buckets should merge: max of previous cluster is within distance of min of current
            if (prev.max_cluster_key + cluster_distance >= curr.min_cluster_key)
            {
                /// Merge current bucket into previous
                mergeAggregateStates(merged_columns, aggregates_mask, prev.leader_row_index, curr.leader_row_index);
                prev.max_cluster_key = std::max(prev.max_cluster_key, curr.max_cluster_key);
                prev.min_cluster_key = std::min(prev.min_cluster_key, curr.min_cluster_key);
                curr.alive = false;
            }
        }
    }

    /// --- Phase C: Build result columns from surviving bucket leaders ---
    size_t result_rows = 0;
    for (auto & bucket : buckets)
        if (bucket.alive)
            ++result_rows;

    MutableColumns result_columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        result_columns[i] = merged_columns[i]->cloneEmpty();

    for (size_t idx : bucket_order)
    {
        if (buckets[idx].alive)
        {
            for (size_t col_idx = 0; col_idx < num_columns; ++col_idx)
                result_columns[col_idx]->insertFrom(*merged_columns[col_idx], buckets[idx].leader_row_index);
        }
    }

    Chunk result(std::move(result_columns), result_rows);

    /// Finalize aggregate functions
    finalizeChunk(result, aggregates_mask);

    return result;
}

}
