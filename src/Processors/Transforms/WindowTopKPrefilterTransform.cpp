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

using PartitionMap = HashMap<UInt128, size_t, UInt128TrivialHash>;

/// Row indices into the live chunk, arranged as a heap whose front is the worst of the best `top_k`.
struct PartitionHeap
{
    std::vector<size_t> rows;
    /// The row that created the bucket; every later row is verified against it with `compareAt`.
    size_t first_row = 0;
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

    PartitionMap partition_to_heap;
    std::vector<PartitionHeap> heaps;

    filter.resize(num_rows);
    size_t kept_rows = 0;

    for (size_t row = 0; row < num_rows; ++row)
    {
        SipHash hash;
        for (const auto * column : partition_columns)
            column->updateHashWithValue(row, hash);

        PartitionMap::LookupResult bucket = nullptr;
        bool inserted = false;
        partition_to_heap.emplace(hash.get128(), bucket, inserted);
        if (inserted)
        {
            bucket->getMapped() = heaps.size();
            heaps.emplace_back(PartitionHeap{.rows = {}, .first_row = row});
        }

        auto & heap = heaps[bucket->getMapped()];
        UInt8 keep = 1;

        /// A hash collision can only over-forward, so the partition key type needs no restriction: two
        /// different keys in one bucket are caught here and both rows forwarded, and one key split across
        /// two buckets leaves each bucket keeping the best `top_k` of a subset.
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
            keep = 0;
        }
        /// Otherwise the row ties with the heap's worst entry, so it shares that entry's rank and has to be
        /// forwarded - but it must not displace anything.

        filter[row] = keep;
        kept_rows += keep;
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
