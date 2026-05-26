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

std::unique_ptr<SubstreamsCacheSparseOffsetsElement>
SparseOffsetsShare::slice(
    const std::string & part_name,
    const std::string & column_name,
    size_t abs_row_start,
    size_t rows_offset,
    size_t limit,
    size_t frame_prev_size) const
{
    std::shared_lock lock(mutex);

    auto part_it = store.find(part_name);
    if (part_it == store.end())
        return nullptr;

    auto col_it = part_it->second.find(column_name);
    if (col_it == part_it->second.end())
        return nullptr;

    const auto & ranges = col_it->second.ranges;
    const size_t total_rows = rows_offset + limit;
    const size_t abs_row_end = abs_row_start + total_rows;

    /// We do not span multiple stored ranges in one slice. Scan `readRows` calls stay
    /// within one `MarkRange` (the analyzer's storage unit), so the contains-relation
    /// is the common case. A request crossing two stored ranges returns nullptr and
    /// the consumer falls back to a normal disk read for that call.
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

    /// `[skip_start_rel, skip_end_rel)` is the rows_offset zone (non-defaults here
    /// count as skipped_values_rows). `[skip_end_rel, produce_end_rel)` is the
    /// produce zone (non-defaults here are emitted to the offsets column).
    const size_t skip_start_rel = abs_row_start - found->start_row_in_part;
    const size_t skip_end_rel = skip_start_rel + rows_offset;
    const size_t produce_end_rel = skip_end_rel + limit;

    /// Stored offsets are sorted ascending, so `lower_bound` finds the boundaries in
    /// log time. This is the hot path: with one analyzer-range per part holding ~M
    /// offsets and ~K scan calls per query, the previous linear walk was O(K*M); the
    /// binary searches plus a tight copy reduce that to O(K * (log M + produce_count)).
    const auto & src_offsets = assert_cast<const ColumnUInt64 &>(*found->offsets).getData();
    const auto * begin = src_offsets.data();
    const auto * end = begin + src_offsets.size();

    const auto * skip_zone_begin = std::lower_bound(begin, end, skip_start_rel);
    const auto * produce_zone_begin = std::lower_bound(skip_zone_begin, end, skip_end_rel);
    const auto * produce_zone_end = std::lower_bound(produce_zone_begin, end, produce_end_rel);

    const size_t skipped = produce_zone_begin - skip_zone_begin;
    const size_t produce_count = produce_zone_end - produce_zone_begin;

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
