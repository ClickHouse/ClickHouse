#include <Storages/MergeTree/SparseOffsetsShare.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/Serializations/SerializationSparse.h>

#include <algorithm>
#include <mutex>
#include <shared_mutex>


namespace DB
{

void SparseOffsetsShare::insert(
    const std::string & part_name,
    const std::string & column_name,
    MarkRange range,
    size_t start_row_in_part,
    size_t total_rows,
    ColumnPtr offsets)
{
    std::unique_lock lock(mutex);
    auto & bucket = store[part_name][column_name];
    bucket.ranges.push_back(SparseOffsetsRange{range, start_row_in_part, total_rows, std::move(offsets)});
}

const SparseOffsetsShare::Bucket *
SparseOffsetsShare::findBucket(const std::string & part_name, const std::string & column_name) const
{
    std::shared_lock lock(mutex);

    auto part_it = store.find(part_name);
    if (part_it == store.end())
        return nullptr;

    auto col_it = part_it->second.find(column_name);
    if (col_it == part_it->second.end())
        return nullptr;

    return &col_it->second;
}

std::unique_ptr<SubstreamsCacheSparseOffsetsElement>
SparseOffsetsShare::sliceFromBucket(
    const Bucket & bucket,
    size_t abs_row_start,
    size_t rows_offset,
    size_t limit,
    size_t frame_prev_size)
{
    const auto & ranges = bucket.ranges;
    const size_t abs_row_end = abs_row_start + rows_offset + limit;

    /// Scan `readRows` calls stay within one `MarkRange` (the analyzer's storage unit),
    /// so the contains-relation is the common case. A request crossing two stored ranges
    /// returns nullptr and the consumer falls back to a normal disk read for that call.
    const SparseOffsetsRange * found = nullptr;
    for (const auto & r : ranges)
    {
        const size_t end_row_in_part = r.start_row_in_part + r.total_rows;
        if (r.start_row_in_part <= abs_row_start && abs_row_end <= end_row_in_part)
        {
            found = &r;
            break;
        }
    }
    if (!found)
        return nullptr;

    /// `[skip_start_rel, skip_end_rel)` is the rows_offset zone (non-defaults here count
    /// as `skipped_values_rows`). `[skip_end_rel, produce_end_rel)` is the produce zone
    /// (non-defaults here are emitted to the offsets column).
    const size_t skip_start_rel = abs_row_start - found->start_row_in_part;
    const size_t skip_end_rel = skip_start_rel + rows_offset;
    const size_t produce_end_rel = skip_end_rel + limit;

    /// Stored offsets are sorted ascending; `lower_bound` finds the boundaries in log
    /// time, then we copy only the produce window into a fresh column sized exactly to fit.
    const auto & src_offsets = assert_cast<const ColumnUInt64 &>(*found->offsets).getData();
    const auto * begin = src_offsets.data();
    const auto * end = begin + src_offsets.size();

    /// Fast path for the common `rows_offset == 0` case: `skip_end_rel == skip_start_rel`,
    /// so the second `lower_bound` would return the first's iterator and `skipped` is 0.
    /// Skip the redundant search.
    const auto * produce_zone_begin = std::lower_bound(begin, end, skip_end_rel);
    size_t skipped = 0;
    if (rows_offset != 0)
    {
        const auto * skip_zone_begin = std::lower_bound(begin, produce_zone_begin, skip_start_rel);
        skipped = produce_zone_begin - skip_zone_begin;
    }
    const auto * produce_zone_end = std::lower_bound(produce_zone_begin, end, produce_end_rel);

    const size_t produce_count = produce_zone_end - produce_zone_begin;

    /// Use `create()` + `resize` rather than `create(N)`: when the constructor's `N` is
    /// passed by const-reference through `COW::create`, the compiler stashes it on the
    /// stack and reloads the loop bound on every iteration, dropping the SSE2 paddq
    /// vectorisation of the shifted copy. The resize-then-loop pattern keeps the size
    /// visible enough to stay vectorised.
    auto sliced_column = ColumnUInt64::create();
    auto & sliced_data = sliced_column->getData();
    sliced_data.resize(produce_count);
    for (size_t i = 0; i < produce_count; ++i)
        sliced_data[i] = produce_zone_begin[i] - skip_end_rel + frame_prev_size;

    return std::make_unique<SubstreamsCacheSparseOffsetsElement>(
        std::move(sliced_column),
        /*old_size_=*/0,
        /*read_rows_=*/limit,
        /*skipped_values_rows_=*/skipped);
}

}
