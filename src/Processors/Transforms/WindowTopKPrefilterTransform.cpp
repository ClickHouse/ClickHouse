#include <Processors/Transforms/WindowTopKPrefilterTransform.h>

#include <Columns/ColumnReplicated.h>
#include <Columns/IColumn.h>
#include <Common/HashTable/Hash.h>
#include <Common/HashTable/HashMap.h>
#include <Common/SipHash.h>

#include <algorithm>

namespace DB
{

namespace
{

using PartitionMap = HashMap<UInt128, UInt32, UInt128TrivialHash>;

/// Row indices into the live chunk, arranged as a heap whose front is the worst of the best `top_k`.
struct PartitionHeap
{
    std::vector<UInt32> rows;
    /// The row that created the bucket. Every later row is verified against it with `compareAt`, so a
    /// hash collision cannot make two logically different keys share one heap.
    UInt32 first_row = 0;
};

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

    const Columns & columns = chunk.getColumns();

    /// Materialize `ColumnReplicated` key columns so comparisons do not pay the index indirection, as
    /// `PartialSortingTransform` does. These copies are only read here: the chunk keeps its own columns
    /// and the filter below is applied to those.
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

    /// `direction * compareAt` over the ORDER BY columns: bit for bit the comparator the sort itself uses,
    /// so "better" means "earlier in the window's order".
    auto is_better = [&](UInt32 lhs, UInt32 rhs) -> bool
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

    auto same_partition = [&](UInt32 lhs, UInt32 rhs) -> bool
    {
        for (size_t i = 0, size = partition_columns.size(); i < size; ++i)
        {
            if (partition_columns[i]->compareAt(lhs, rhs, *partition_columns[i], partition_description[i].nulls_direction)
                != 0)
                return false;
        }
        return true;
    };

    PartitionMap partition_to_heap;
    std::vector<PartitionHeap> heaps;

    filter.resize(num_rows);
    size_t kept_rows = 0;

    for (UInt32 row = 0; row < num_rows; ++row)
    {
        SipHash hash;
        for (const auto * column : partition_columns)
            column->updateHashWithValue(row, hash);

        PartitionMap::LookupResult bucket;
        bool inserted = false;
        partition_to_heap.emplace(hash.get128(), bucket, inserted);
        if (inserted)
        {
            bucket->getMapped() = static_cast<UInt32>(heaps.size());
            heaps.emplace_back(PartitionHeap{.rows = {}, .first_row = row});
        }

        auto & heap = heaps[bucket->getMapped()];
        UInt8 keep = 1;

        /// A collision between two different keys is caught here and both rows forwarded; one logical key
        /// split across two buckets would leave each keeping the best `top_k` of a subset. Either way the
        /// forwarded set only grows, which is why the partition key type needs no restriction.
        if (!same_partition(row, heap.first_row))
        {
            filter[row] = 1;
            ++kept_rows;
            continue;
        }

        if (heap.rows.size() < top_k)
        {
            heap.rows.push_back(row);
            std::push_heap(heap.rows.begin(), heap.rows.end(), is_better);
        }
        else if (is_better(row, heap.rows.front()))
        {
            std::pop_heap(heap.rows.begin(), heap.rows.end(), is_better);
            heap.rows.back() = row;
            std::push_heap(heap.rows.begin(), heap.rows.end(), is_better);
        }
        else if (is_better(heap.rows.front(), row))
        {
            /// `top_k` distinct rows of this partition are strictly better, so this row's rank is above the
            /// bound whatever the rest of the input looks like.
            keep = 0;
        }
        /// Otherwise the row ties with the heap's worst entry, so it shares that entry's rank and has to be
        /// forwarded - but it must not displace anything.

        filter[row] = keep;
        kept_rows += keep;
    }

    observed_rows += num_rows;
    skipped_rows += num_rows - kept_rows;
    /// Hashing and heap building in chunk after chunk of mostly unique partitions costs CPU for nothing.
    /// The threshold is `TopKAggregationHeapBase::shouldFreeze`'s, but the window is not: a chunk-local
    /// heap's skip rate is decided within one chunk, so one chunk of evidence is enough, and waiting for
    /// that heap's 65536-row window would only make more chunks pay full cost.
    if (observed_rows >= profitability_window
        && static_cast<Float64>(skipped_rows) / static_cast<Float64>(observed_rows) < 0.1)
        frozen = true;

    if (kept_rows == num_rows)
        return;

    if (kept_rows == 0)
    {
        /// `ISimpleTransform::work` restores the header's column count for an emptied chunk.
        chunk.clear();
        return;
    }

    Columns filtered;
    filtered.reserve(columns.size());
    for (const auto & column : columns)
        filtered.push_back(column->filter(filter, kept_rows));

    chunk.setColumns(std::move(filtered), kept_rows);
}

}
