#include <Processors/Transforms/ClusterMergingTransform.h>

#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnTuple.h>
#include <Columns/IColumn.h>
#include <Common/Arena.h>
#include <Common/SipHash.h>
#include <Common/levenshteinDistance.h>
#include <DataTypes/IDataType.h>
#include <Processors/Port.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Core/AccurateComparison.h>

#include <absl/container/inlined_vector.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <numeric>
#include <span>
#include <unordered_map>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Cast `floor(v)` to `Int64`, throwing if the result would be out of `Int64` range
/// or if the input is non-finite. Uses `accurate::convertNumeric` which avoids the UB
/// of a direct `static_cast<Int64>` on out-of-range `Float64`. The `isFinite` guard
/// matches the pre-check pattern used by `FunctionsConversion` for float→int.
Int64 safeFloorToInt64(Float64 v)
{
    if (!std::isfinite(v))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "GROUP BY ... WITH CLUSTER: cluster key is not finite (value = {})", v);

    Int64 result;
    if (!accurate::convertNumeric<Float64, Int64, /*strict=*/false>(std::floor(v), result))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "GROUP BY ... WITH CLUSTER: cluster key produces an out-of-range bucket id "
            "(value = {}); use a larger distance or a narrower key type", v);
    return result;
}


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

/// Compile-time switch: true = bucket optimization, false = old sort-based algorithm.
static constexpr bool USE_BUCKET_OPTIMIZATION = true;

}

ClusterMergingTransform::ClusterMergingTransform(
    SharedHeader header_,
    AggregatingTransformParamsPtr params_,
    Names cluster_key_names_,
    Float64 cluster_distance_,
    size_t dimensions_)
    : IAccumulatingTransform(header_, std::make_shared<const Block>(params_->getHeader()))
    , params(std::move(params_))
    , cluster_key_names(std::move(cluster_key_names_))
    , cluster_distance(cluster_distance_)
    , dimensions(dimensions_)
    , aggregates_mask(getAggregatesMask(input.getHeader(), params->params.aggregates))
{
    if (dimensions != 1 && dimensions != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ClusterMergingTransform supports only 1 or 2 dimensions, got {}", dimensions);
    if (cluster_key_names.size() != dimensions)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "ClusterMergingTransform expects {} key names for {}D, got {}",
            dimensions, dimensions, cluster_key_names.size());
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

    if (dimensions == 2)
        return generate2D();

    /// 1D: dispatch by cluster-key column type. String / FixedString → Levenshtein,
    /// numeric → existing bucket-based path.
    const auto & header = input.getHeader();
    size_t cluster_key_pos = header.getPositionByName(cluster_key_names[0]);
    const auto & col_type = header.getByPosition(cluster_key_pos).type;
    if (isStringOrFixedString(col_type))
        return generateString();

    return generate1D();
}

Chunk ClusterMergingTransform::generate1D()
{
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
    size_t cluster_key_pos = header.getPositionByName(cluster_key_names[0]);

    /// Build a list of key column positions (non-aggregate columns)
    std::vector<size_t> non_cluster_key_positions;
    for (size_t i = 0; i < num_columns; ++i)
    {
        if (!aggregates_mask[i] && i != cluster_key_pos)
            non_cluster_key_positions.push_back(i);
    }

    if constexpr (!USE_BUCKET_OPTIMIZATION)
    {
        /// Old sort-based algorithm: O(n log n)
        /// Sort all rows by (non_cluster_keys, cluster_key), then linear merge.
        std::vector<size_t> row_order(total_rows);
        std::iota(row_order.begin(), row_order.end(), 0);

        std::sort(row_order.begin(), row_order.end(), [&](size_t a, size_t b)
        {
            for (size_t pos : non_cluster_key_positions)
            {
                int cmp = merged_columns[pos]->compareAt(a, b, *merged_columns[pos], 1);
                if (cmp != 0)
                    return cmp < 0;
            }
            return merged_columns[cluster_key_pos]->compareAt(a, b, *merged_columns[cluster_key_pos], 1) < 0;
        });

        /// Linear scan: merge adjacent rows within distance
        std::vector<bool> alive(total_rows, true);
        std::vector<Float64> max_cluster(total_rows);
        for (size_t i = 0; i < total_rows; ++i)
            max_cluster[i] = merged_columns[cluster_key_pos]->getFloat64(row_order[i]);

        for (size_t i = 1; i < total_rows; ++i)
        {
            /// Find previous alive row
            size_t prev = i - 1;
            while (prev < total_rows && !alive[prev])
            {
                if (prev == 0) break;
                --prev;
            }
            if (prev >= total_rows || !alive[prev])
                continue;

            size_t prev_row = row_order[prev];
            size_t curr_row = row_order[i];

            /// Check non-cluster keys match
            bool same = true;
            for (size_t pos : non_cluster_key_positions)
            {
                if (merged_columns[pos]->compareAt(prev_row, curr_row, *merged_columns[pos], 1) != 0)
                {
                    same = false;
                    break;
                }
            }
            if (!same) continue;

            Float64 curr_val = merged_columns[cluster_key_pos]->getFloat64(curr_row);
            if (max_cluster[prev] + cluster_distance >= curr_val)
            {
                mergeAggregateStates(merged_columns, aggregates_mask, prev_row, curr_row);
                max_cluster[prev] = std::max(max_cluster[prev], curr_val);
                alive[i] = false;
            }
        }

        /// Build result
        size_t result_rows = 0;
        for (bool a : alive)
            if (a) ++result_rows;

        MutableColumns result_columns(num_columns);
        for (size_t i = 0; i < num_columns; ++i)
            result_columns[i] = merged_columns[i]->cloneEmpty();

        for (size_t i = 0; i < total_rows; ++i)
        {
            if (alive[i])
            {
                for (size_t col_idx = 0; col_idx < num_columns; ++col_idx)
                    result_columns[col_idx]->insertFrom(*merged_columns[col_idx], row_order[i]);
            }
        }

        Chunk result(std::move(result_columns), result_rows);
        finalizeChunk(result, aggregates_mask);
        return result;
    }

    /// --- Phase A: Bucket reduction ---
    /// For distance == 0, each unique value is its own bucket (no merging across values).
    /// For distance > 0, bucket_id = floor(cluster_key / distance) groups nearby values.
    /// All values within the same bucket differ by at most distance-1 < distance,
    /// so they unconditionally belong to the same cluster.

    std::vector<BucketState> buckets;
    /// hash → list of bucket indices sharing that hash. Collision-safe: on a hash hit
    /// we still verify `bucket_id` and the non-cluster key values match before merging.
    /// `InlinedVector<_, 1>` keeps the typical single-candidate case heap-free.
    std::unordered_map<UInt64, absl::InlinedVector<size_t, 1>> bucket_map;

    for (size_t i = 0; i < total_rows; ++i)
    {
        Float64 cluster_val = merged_columns[cluster_key_pos]->getFloat64(i);

        Int64 bucket_id;
        if (cluster_distance > 0)
            bucket_id = safeFloorToInt64(cluster_val / cluster_distance);
        else
        {
            /// distance == 0: each distinct value is its own bucket.
            /// Use memcpy to reinterpret Float64 bits as Int64,
            /// so identical values get the same bucket_id.
            static_assert(sizeof(Float64) == sizeof(Int64));
            std::memcpy(&bucket_id, &cluster_val, sizeof(Int64));
        }

        UInt64 hash = computeBucketHash(merged_columns, non_cluster_key_positions, i, bucket_id);

        auto & candidates = bucket_map[hash];
        size_t found = std::numeric_limits<size_t>::max();
        for (size_t cand : candidates)
        {
            const auto & cand_bucket = buckets[cand];
            if (cand_bucket.bucket_id != bucket_id)
                continue;
            bool same = true;
            for (size_t pos : non_cluster_key_positions)
            {
                if (merged_columns[pos]->compareAt(cand_bucket.leader_row_index, i, *merged_columns[pos], 1) != 0)
                {
                    same = false;
                    break;
                }
            }
            if (same)
            {
                found = cand;
                break;
            }
        }

        if (found != std::numeric_limits<size_t>::max())
        {
            /// Existing bucket: merge aggregate states, update min/max
            auto & bucket = buckets[found];
            mergeAggregateStates(merged_columns, aggregates_mask, bucket.leader_row_index, i);
            bucket.min_cluster_key = std::min(bucket.min_cluster_key, cluster_val);
            bucket.max_cluster_key = std::max(bucket.max_cluster_key, cluster_val);
        }
        else
        {
            /// New bucket
            size_t idx = buckets.size();
            buckets.push_back({i, bucket_id, cluster_val, cluster_val, true});
            candidates.push_back(idx);
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

namespace
{

/// Union-find with path compression + union-by-size.
struct DisjointSetUnion
{
    std::vector<size_t> parent;
    std::vector<size_t> size;

    explicit DisjointSetUnion(size_t n) : parent(n), size(n, 1)
    {
        std::iota(parent.begin(), parent.end(), size_t{0});
    }

    size_t find(size_t x)
    {
        while (parent[x] != x)
        {
            parent[x] = parent[parent[x]];
            x = parent[x];
        }
        return x;
    }

    void unite(size_t a, size_t b)
    {
        a = find(a);
        b = find(b);
        if (a == b)
            return;
        if (size[a] < size[b])
            std::swap(a, b);
        parent[b] = a;
        size[a] += size[b];
    }
};

/// State of one grid cell during 2D clustering.
struct CellState
{
    size_t leader_row_index;          /// Row in merged_columns that accumulates aggregate states.
    Int64 cx;
    Int64 cy;
    std::vector<size_t> row_indices;  /// All rows assigned to this cell (for neighbor distance checks).
    Float64 min_x;
    Float64 max_x;
    Float64 min_y;
    Float64 max_y;
};

/// Hash `(non_cluster_key_values..., cx, cy)` for a given row in merged_columns.
UInt64 computeCellHash(
    const MutableColumns & merged_columns,
    const std::vector<size_t> & non_cluster_key_positions,
    size_t row,
    Int64 cx,
    Int64 cy)
{
    SipHash hash;
    for (size_t pos : non_cluster_key_positions)
        merged_columns[pos]->updateHashWithValue(row, hash);
    hash.update(cx);
    hash.update(cy);
    return hash.get64();
}

}

Chunk ClusterMergingTransform::generate2D()
{
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

    /// In 2D, the upstream `Aggregating` step flattens `(x, y)` into two
    /// separate scalar aggregation keys. Read both column positions.
    size_t x_pos = header.getPositionByName(cluster_key_names[0]);
    size_t y_pos = header.getPositionByName(cluster_key_names[1]);

    const IColumn & x_col = *merged_columns[x_pos];
    const IColumn & y_col = *merged_columns[y_pos];

    std::vector<size_t> non_cluster_key_positions;
    for (size_t i = 0; i < num_columns; ++i)
    {
        if (!aggregates_mask[i] && i != x_pos && i != y_pos)
            non_cluster_key_positions.push_back(i);
    }

    /// Edge case: distance == 0 — no merging at all; each distinct (x, y) per non-cluster key
    /// is already a separate group coming out of the upstream GROUP BY.
    if (cluster_distance <= 0)
    {
        MutableColumns result_columns(num_columns);
        for (size_t i = 0; i < num_columns; ++i)
            result_columns[i] = merged_columns[i]->cloneEmpty();
        for (size_t i = 0; i < total_rows; ++i)
            for (size_t col_idx = 0; col_idx < num_columns; ++col_idx)
                result_columns[col_idx]->insertFrom(*merged_columns[col_idx], i);
        Chunk result(std::move(result_columns), total_rows);
        finalizeChunk(result, aggregates_mask);
        return result;
    }

    const Float64 d = cluster_distance;
    const Float64 d_sq = d * d;
    const Float64 a = d / std::sqrt(2.0);   /// Cell side: diagonal == d.

    /// --- Phase A: Cell reduction (O(n)) ---
    /// Hash each row by (non_cluster_keys..., cx, cy). Same-bucket rows are
    /// unconditionally within d of each other (diagonal == d), merge them.

    std::vector<CellState> cells;
    /// hash → list of cell indices sharing that hash. Collision-safe: on a hash hit
    /// we still verify `(cx, cy)` and the non-cluster key values match before merging.
    /// `InlinedVector<_, 1>` keeps the typical single-candidate case heap-free.
    std::unordered_map<UInt64, absl::InlinedVector<size_t, 1>> cell_map;

    /// Find a cell with the requested `(cx, cy)` whose non-cluster keys equal those
    /// of `probe_row`. Returns size_t(-1) if no such cell exists.
    auto lookup_cell = [&](Int64 cx, Int64 cy, size_t probe_row, UInt64 h) -> size_t
    {
        auto it = cell_map.find(h);
        if (it == cell_map.end())
            return std::numeric_limits<size_t>::max();
        for (size_t cand : it->second)
        {
            const auto & cell = cells[cand];
            if (cell.cx != cx || cell.cy != cy)
                continue;
            bool same = true;
            for (size_t pos : non_cluster_key_positions)
            {
                if (merged_columns[pos]->compareAt(cell.leader_row_index, probe_row, *merged_columns[pos], 1) != 0)
                {
                    same = false;
                    break;
                }
            }
            if (same)
                return cand;
        }
        return std::numeric_limits<size_t>::max();
    };

    for (size_t i = 0; i < total_rows; ++i)
    {
        Float64 xv = x_col.getFloat64(i);
        Float64 yv = y_col.getFloat64(i);
        Int64 cx = safeFloorToInt64(xv / a);
        Int64 cy = safeFloorToInt64(yv / a);

        UInt64 h = computeCellHash(merged_columns, non_cluster_key_positions, i, cx, cy);

        size_t found = lookup_cell(cx, cy, i, h);
        if (found != std::numeric_limits<size_t>::max())
        {
            auto & cell = cells[found];
            mergeAggregateStates(merged_columns, aggregates_mask, cell.leader_row_index, i);
            cell.row_indices.push_back(i);
            cell.min_x = std::min(cell.min_x, xv);
            cell.max_x = std::max(cell.max_x, xv);
            cell.min_y = std::min(cell.min_y, yv);
            cell.max_y = std::max(cell.max_y, yv);
        }
        else
        {
            size_t idx = cells.size();
            cells.push_back({i, cx, cy, {i}, xv, xv, yv, yv});
            cell_map[h].push_back(idx);
        }
    }

    /// --- Phase B: Adjacency graph over cells (O(b) probes * neighbor cell size) ---
    /// For each cell, probe 12 "forward" neighbors in the 5x5 neighborhood
    /// (|dx|,|dy| <= 2, lexicographically greater than (0,0)). For each
    /// matching neighbor cell, brute-force check if any pair of points
    /// is within distance d; if yes, unite them in DSU.

    static constexpr std::array<std::pair<int, int>, 12> forward_offsets = {{
        {0, 1}, {0, 2},
        {1, -2}, {1, -1}, {1, 0}, {1, 1}, {1, 2},
        {2, -2}, {2, -1}, {2, 0}, {2, 1}, {2, 2},
    }};

    DisjointSetUnion dsu(cells.size());

    for (size_t ci = 0; ci < cells.size(); ++ci)
    {
        const auto & A = cells[ci];
        size_t leader_A = A.leader_row_index;

        for (auto [dx, dy] : forward_offsets)
        {
            Int64 ncx = A.cx + dx;
            Int64 ncy = A.cy + dy;

            UInt64 h = computeCellHash(merged_columns, non_cluster_key_positions, leader_A, ncx, ncy);
            size_t cj = lookup_cell(ncx, ncy, leader_A, h);
            if (cj == std::numeric_limits<size_t>::max())
                continue;
            if (cj == ci)
                continue;   /// Should not happen — forward_offsets exclude (0,0) — defensive.
            const auto & B = cells[cj];

            /// Axis-aligned early reject: if the bounding boxes are already > d apart
            /// on either axis, no pair can satisfy the distance constraint.
            Float64 gap_x = std::max({0.0, A.min_x - B.max_x, B.min_x - A.max_x});
            Float64 gap_y = std::max({0.0, A.min_y - B.max_y, B.min_y - A.max_y});
            if (gap_x * gap_x + gap_y * gap_y > d_sq)
                continue;

            /// Brute-force pairwise check.
            bool connected = false;
            for (size_t ra : A.row_indices)
            {
                Float64 ax = x_col.getFloat64(ra);
                Float64 ay = y_col.getFloat64(ra);
                for (size_t rb : B.row_indices)
                {
                    Float64 dxv = ax - x_col.getFloat64(rb);
                    Float64 dyv = ay - y_col.getFloat64(rb);
                    if (dxv * dxv + dyv * dyv <= d_sq)
                    {
                        connected = true;
                        break;
                    }
                }
                if (connected)
                    break;
            }

            if (connected)
                dsu.unite(ci, cj);
        }
    }

    /// --- Phase C: Merge aggregate states within each component ---
    /// For each cell, find its DSU root; merge non-root cells into the root's leader.

    std::vector<bool> is_root(cells.size(), false);
    for (size_t ci = 0; ci < cells.size(); ++ci)
        if (dsu.find(ci) == ci)
            is_root[ci] = true;

    for (size_t ci = 0; ci < cells.size(); ++ci)
    {
        size_t root = dsu.find(ci);
        if (root == ci)
            continue;
        mergeAggregateStates(merged_columns, aggregates_mask, cells[root].leader_row_index, cells[ci].leader_row_index);
    }

    /// --- Phase D: Build result ---
    size_t result_rows = 0;
    for (bool r : is_root)
        if (r)
            ++result_rows;

    MutableColumns result_columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        result_columns[i] = merged_columns[i]->cloneEmpty();

    for (size_t ci = 0; ci < cells.size(); ++ci)
    {
        if (!is_root[ci])
            continue;
        for (size_t col_idx = 0; col_idx < num_columns; ++col_idx)
            result_columns[col_idx]->insertFrom(*merged_columns[col_idx], cells[ci].leader_row_index);
    }

    Chunk result(std::move(result_columns), result_rows);
    finalizeChunk(result, aggregates_mask);
    return result;
}

Chunk ClusterMergingTransform::generateString()
{
    /// Concatenate all chunks into a single block.
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

    size_t cluster_key_pos = header.getPositionByName(cluster_key_names[0]);
    const IColumn & key_col = *merged_columns[cluster_key_pos];

    std::vector<size_t> non_cluster_key_positions;
    for (size_t i = 0; i < num_columns; ++i)
    {
        if (!aggregates_mask[i] && i != cluster_key_pos)
            non_cluster_key_positions.push_back(i);
    }

    /// Distance for String keys is interpreted as a non-negative integer edit distance.
    /// Reject NaN/Inf, negative, and fractional values explicitly; an unchecked cast
    /// would be UB for out-of-range floats and would silently truncate `1.9 → 1`.
    if (!std::isfinite(cluster_distance))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "GROUP BY ... WITH CLUSTER on String: distance must be finite, got {}", cluster_distance);

    size_t max_edits;
    if (!accurate::convertNumeric<Float64, size_t, /*strict=*/true>(cluster_distance, max_edits))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "GROUP BY ... WITH CLUSTER on String: distance must be a non-negative integer, got {}",
            cluster_distance);

    /// d == 0: only exact-match merging — already done by the upstream `Aggregator`.
    /// Trivial path: copy input rows to output unchanged.
    if (max_edits == 0)
    {
        MutableColumns result_columns(num_columns);
        for (size_t i = 0; i < num_columns; ++i)
            result_columns[i] = merged_columns[i]->cloneEmpty();
        for (size_t i = 0; i < total_rows; ++i)
            for (size_t col_idx = 0; col_idx < num_columns; ++col_idx)
                result_columns[col_idx]->insertFrom(*merged_columns[col_idx], i);
        Chunk result(std::move(result_columns), total_rows);
        finalizeChunk(result, aggregates_mask);
        return result;
    }

    /// Cache string views to avoid repeated `getDataAt()` calls.
    std::vector<std::string_view> strings;
    strings.reserve(total_rows);
    for (size_t i = 0; i < total_rows; ++i)
        strings.emplace_back(key_col.getDataAt(i));

    DisjointSetUnion dsu(total_rows);

    /// Verify a pair (i, j): length filter → non-cluster keys → same-component
    /// skip → exact byte-level Levenshtein. Unites in DSU on match.
    auto verify_pair = [&](size_t i, size_t j) -> void
    {
        const auto & si = strings[i];
        const auto & sj = strings[j];

        const size_t len_diff = (si.size() > sj.size()) ? (si.size() - sj.size()) : (sj.size() - si.size());
        if (len_diff > max_edits)
            return;

        for (size_t pos : non_cluster_key_positions)
        {
            if (merged_columns[pos]->compareAt(i, j, *merged_columns[pos], 1) != 0)
                return;
        }

        if (dsu.find(i) == dsu.find(j))
            return;

        const size_t dist = levenshteinDistance<char>(
            std::span<const char>(si.data(), si.size()),
            std::span<const char>(sj.data(), sj.size()));
        if (dist <= max_edits)
            dsu.unite(i, j);
    };

    /// Q-gram filter: for two strings within edit distance d, the number of
    /// shared q-grams is at least `max(|a|, |b|) - q + 1 - q*d` (Ukkonen, 1992).
    /// We build an inverted index `qgram → [row_ids]` and walk it per row.
    /// q = 3 is a literature standard. Threshold: below 10k rows the naive
    /// O(N^2) loop with the length filter is faster than building the index.

    constexpr size_t Q = 3;
    constexpr size_t QGRAM_THRESHOLD = 10000;

    if (total_rows < QGRAM_THRESHOLD)
    {
        /// Naive O(N^2) pairwise sweep. UTF-8 strings are treated as raw bytes.
        for (size_t i = 0; i < total_rows; ++i)
            for (size_t j = i + 1; j < total_rows; ++j)
                verify_pair(i, j);
    }
    else
    {
        /// Pack 3 bytes into a UInt32 — fits the whole 24-bit q-gram space.
        auto pack_qgram = [](const char * p) -> UInt32
        {
            return (static_cast<UInt32>(static_cast<uint8_t>(p[0])) << 16)
                 | (static_cast<UInt32>(static_cast<uint8_t>(p[1])) << 8)
                 |  static_cast<UInt32>(static_cast<uint8_t>(p[2]));
        };

        /// Build inverted index over rows of length ≥ Q. Each q-gram occurrence
        /// adds the row id to the posting list (multiset semantics, required for
        /// the Ukkonen bound to hold).
        ///
        /// We also track `small_rows` — rows for which the Ukkonen lower bound
        /// `max(qgrams_i, qgrams_j) - Q * max_edits` collapses to zero. For these
        /// the filter is non-informative and pairs with zero shared q-grams may
        /// still be within edit distance, so they need a pairwise fallback.
        std::unordered_map<UInt32, std::vector<size_t>> index;
        std::vector<size_t> short_rows;
        std::vector<size_t> small_rows;
        const size_t small_qgrams_bound = Q * max_edits;
        for (size_t i = 0; i < total_rows; ++i)
        {
            const auto & s = strings[i];
            if (s.size() < Q)
            {
                short_rows.push_back(i);
                continue;
            }
            const size_t qgrams = s.size() - Q + 1;
            for (size_t k = 0; k < qgrams; ++k)
                index[pack_qgram(s.data() + k)].push_back(i);
            if (qgrams <= small_qgrams_bound)
                small_rows.push_back(i);
        }

        /// Reusable counter buffer: counter[j] = matched q-grams between current i and row j.
        /// Tracked through `dirty` to keep clearing O(touched) instead of O(total_rows).
        std::vector<size_t> counter(total_rows, 0);
        std::vector<size_t> dirty;

        for (size_t i = 0; i < total_rows; ++i)
        {
            const auto & si = strings[i];

            /// Short strings (no q-grams) bypass the index: pairwise verify with all j > i.
            if (si.size() < Q)
            {
                for (size_t j = i + 1; j < total_rows; ++j)
                    verify_pair(i, j);
                continue;
            }

            /// Long string i: collect candidates from inverted index.
            const size_t qgrams_i = si.size() - Q + 1;
            for (size_t k = 0; k < qgrams_i; ++k)
            {
                auto it = index.find(pack_qgram(si.data() + k));
                if (it == index.end())
                    continue;
                for (size_t j : it->second)
                {
                    if (j <= i)
                        continue;
                    if (counter[j] == 0)
                        dirty.push_back(j);
                    ++counter[j];
                }
            }

            /// Apply Ukkonen lower bound per pair, then verify.
            /// Counter reset is deferred until after the small_rows fallback below
            /// so that pass can use `counter[j] > 0` as a "already handled" marker.
            for (size_t j : dirty)
            {
                const auto & sj = strings[j];
                const size_t qgrams_j = sj.size() - Q + 1;
                const size_t max_qg = std::max(qgrams_i, qgrams_j);
                const size_t threshold = (max_qg > Q * max_edits) ? (max_qg - Q * max_edits) : 0;
                if (counter[j] >= threshold)
                    verify_pair(i, j);
            }

            /// Pair i with all short rows j > i (they're not in the index).
            for (size_t j : short_rows)
                if (j > i)
                    verify_pair(i, j);

            /// Ukkonen-bound fallback: when `qgrams_i <= Q * max_edits` the lower
            /// bound on shared q-grams collapses to zero for pairs `(i, j ∈ small_rows)`,
            /// so such pairs can satisfy `edit-distance <= max_edits` while sharing
            /// zero q-grams — they never land in `dirty`. Verify them directly here,
            /// skipping those that *did* land in `dirty` (counter[j] > 0) to avoid
            /// a redundant `verify_pair` for false-matches the q-gram filter already
            /// passed but Levenshtein rejected.
            if (qgrams_i <= small_qgrams_bound)
            {
                for (size_t j : small_rows)
                    if (j > i && counter[j] == 0)
                        verify_pair(i, j);
            }

            /// Reset counters before the next iteration.
            for (size_t j : dirty)
                counter[j] = 0;
            dirty.clear();
        }
    }

    /// Merge aggregate states by component root.
    std::vector<bool> is_root(total_rows, false);
    for (size_t i = 0; i < total_rows; ++i)
        if (dsu.find(i) == i)
            is_root[i] = true;

    for (size_t i = 0; i < total_rows; ++i)
    {
        size_t root = dsu.find(i);
        if (root == i)
            continue;
        mergeAggregateStates(merged_columns, aggregates_mask, root, i);
    }

    size_t result_rows = 0;
    for (bool r : is_root)
        if (r)
            ++result_rows;

    MutableColumns result_columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
        result_columns[i] = merged_columns[i]->cloneEmpty();

    for (size_t i = 0; i < total_rows; ++i)
    {
        if (!is_root[i])
            continue;
        for (size_t col_idx = 0; col_idx < num_columns; ++col_idx)
            result_columns[col_idx]->insertFrom(*merged_columns[col_idx], i);
    }

    Chunk result(std::move(result_columns), result_rows);
    finalizeChunk(result, aggregates_mask);
    return result;
}

}
