#include <Processors/Transforms/WindowTopKPrefilterTransform.h>

#include <Columns/ColumnReplicated.h>
#include <Columns/IColumn.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashMap.h>
#include <Common/SipHash.h>
#include <Common/VectorWithMemoryTracking.h>

#include <algorithm>
#include <limits>

namespace DB
{

namespace
{

using PartitionMap = HashMap<UInt128, size_t, UInt128TrivialHash>;

constexpr size_t no_partition = std::numeric_limits<size_t>::max();

}

WindowTopKPrefilterTransform::WindowTopKPrefilterTransform(
    SharedHeader header_,
    const SortDescription & partition_description_,
    const SortDescription & order_description_,
    UInt64 top_k_)
    : ISimpleTransform(header_, header_, false)
    , partition_description(partition_description_)
    , order_description(order_description_)
    , top_k(top_k_)
    , profitability_window(2 * top_k_)
{
    chassert(top_k > 0);
    chassert(!order_description.empty());

    partition_positions.reserve(partition_description.size());
    for (const auto & column_description : partition_description)
        partition_positions.push_back(header_->getPositionByName(column_description.column_name));

    order_positions.reserve(order_description.size());
    for (const auto & column_description : order_description)
        order_positions.push_back(header_->getPositionByName(column_description.column_name));
}

void WindowTopKPrefilterTransform::transform(Chunk & chunk)
{
    const size_t num_rows = chunk.getNumRows();
    if (frozen || num_rows == 0)
        return;

    if (chunks_to_skip > 0)
    {
        --chunks_to_skip;
        return;
    }

    const Columns & columns = chunk.getColumns();

    /// These copies are only read here; the filter below is applied to the chunk's own columns.
    Columns materialized;
    materialized.reserve(partition_positions.size() + order_positions.size());
    auto take = [&](size_t position) -> const IColumn *
    {
        materialized.push_back(convertToFullColumnIfReplicationNotUseful(columns[position]));
        return materialized.back().get();
    };

    ColumnRawPtrs partition_columns;
    partition_columns.reserve(partition_positions.size());
    for (size_t position : partition_positions)
        partition_columns.push_back(take(position));

    ColumnRawPtrs order_columns;
    order_columns.reserve(order_positions.size());
    for (size_t position : order_positions)
        order_columns.push_back(take(position));

    /// `direction * compareAt` is bit for bit the comparator the sort itself applies.
    auto is_better = [&](size_t lhs, size_t rhs) -> bool
    {
        for (size_t i = 0, size = order_columns.size(); i < size; ++i)
        {
            const int res = order_description[i].direction
                * order_columns[i]->compareAt(lhs, rhs, *order_columns[i], order_description[i].nulls_direction);
            if (res != 0)
                return res < 0;
        }
        return false;
    };

    auto same_partition = [&](size_t lhs, size_t rhs) -> bool
    {
        for (size_t i = 0, size = partition_columns.size(); i < size; ++i)
        {
            if (partition_columns[i]->compareAt(lhs, rhs, *partition_columns[i], partition_description[i].nulls_direction)
                != 0)
                return false;
        }
        return true;
    };

    /// A hash collision can only over-forward, so the partition key type needs no restriction: a row whose key
    /// differs from its bucket's first row is forwarded, and one key split across two buckets leaves each bucket
    /// keeping the best `top_k` of a subset.
    PartitionMap hash_to_partition;
    PaddedPODArray<size_t> row_partition(num_rows);
    PaddedPODArray<size_t> partition_first_row;
    PaddedPODArray<size_t> partition_rows;
    /// Rows that arrived to a partition already holding `top_k` rows: only these can be dropped.
    size_t over_bound_rows = 0;

    for (size_t row = 0; row < num_rows; ++row)
    {
        SipHash hash;
        for (const auto * column : partition_columns)
            column->updateHashWithValue(row, hash);

        PartitionMap::LookupResult bucket = nullptr;
        bool inserted = false;
        hash_to_partition.emplace(hash.get128(), bucket, inserted);
        if (inserted)
        {
            bucket->getMapped() = partition_first_row.size();
            partition_first_row.push_back(row);
            partition_rows.push_back(0);

            /// Still in the first quarter, the heaps of the partitions seen could hold 90% of the chunk and under 10% of
            /// its rows so far could be dropped: forward the rest unchanged and skip the next chunks for an interval
            /// that doubles each time.
            if (row + 1 >= profitability_window && 4 * (row + 1) <= num_rows
                && 10 * partition_first_row.size() * top_k >= 9 * num_rows && 10 * over_bound_rows < row + 1)
            {
                chunks_to_skip = skip_interval;
                skip_interval = std::max<UInt64>(1, 2 * skip_interval);
                return;
            }
        }

        const size_t partition = bucket->getMapped();
        if (!inserted && !same_partition(row, partition_first_row[partition]))
        {
            row_partition[row] = no_partition;
            continue;
        }

        row_partition[row] = partition;
        if (++partition_rows[partition] > top_k)
            ++over_bound_rows;
    }

    skip_interval = 0;

    /// A partition with at most `top_k` rows here keeps all of them, so only the larger ones need a heap.
    size_t kept_rows = num_rows;
    if (over_bound_rows > 0)
    {
        /// Row indices into the live chunk, arranged as a heap whose front is the worst of the best `top_k`.
        VectorWithMemoryTracking<VectorWithMemoryTracking<size_t>> heaps(partition_rows.size());

        filter.resize(num_rows);
        kept_rows = 0;

        for (size_t row = 0; row < num_rows; ++row)
        {
            const size_t partition = row_partition[row];
            UInt8 keep = 1;

            if (partition != no_partition && partition_rows[partition] > top_k)
            {
                auto & heap = heaps[partition];
                if (heap.size() < top_k)
                {
                    heap.push_back(row);
                    std::push_heap(heap.begin(), heap.end(), is_better);
                }
                else if (is_better(row, heap.front()))
                {
                    std::pop_heap(heap.begin(), heap.end(), is_better);
                    heap.back() = row;
                    std::push_heap(heap.begin(), heap.end(), is_better);
                }
                else if (is_better(heap.front(), row))
                {
                    keep = 0;
                }
                /// Otherwise the row ties with the heap's worst entry, so it shares that entry's rank and has to be
                /// forwarded - but it must not displace anything.
            }

            filter[row] = keep;
            kept_rows += keep;
        }
    }

    observed_rows += num_rows;
    skipped_rows += num_rows - kept_rows;
    if (observed_rows >= profitability_window
        && static_cast<Float64>(skipped_rows) / static_cast<Float64>(observed_rows) < 0.1)
        frozen = true;

    if (kept_rows == num_rows)
        return;

    Columns filtered;
    filtered.reserve(columns.size());
    for (const auto & column : columns)
        filtered.push_back(column->filter(filter, kept_rows));

    chunk.setColumns(std::move(filtered), kept_rows);
}

}
