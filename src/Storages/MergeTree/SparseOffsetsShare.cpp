#include <Storages/MergeTree/SparseOffsetsShare.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/Serializations/SerializationSparse.h>


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
    std::lock_guard lock(mutex);
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
    std::lock_guard lock(mutex);

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

    /// Translate the call's row window into positions relative to the stored range.
    /// `[skip_start_rel, skip_end_rel)` is the rows_offset zone (non-defaults here
    /// count as skipped_values_rows). `[skip_end_rel, produce_end_rel)` is the
    /// produce zone (non-defaults here are emitted to the offsets column).
    const size_t skip_start_rel = abs_row_start - found->start_row_in_part;
    const size_t skip_end_rel = skip_start_rel + rows_offset;
    const size_t produce_end_rel = skip_end_rel + limit;

    const auto & src_offsets = assert_cast<const ColumnUInt64 &>(*found->offsets).getData();

    auto sliced_column = ColumnUInt64::create();
    auto & sliced_data = sliced_column->getData();
    sliced_data.reserve(src_offsets.size());

    size_t skipped = 0;
    for (UInt64 pos : src_offsets)
    {
        if (pos < skip_start_rel)
            continue;
        if (pos < skip_end_rel)
        {
            ++skipped;
            continue;
        }
        if (pos >= produce_end_rel)
            break;
        /// `deserializeOffsets` writes produce-zone positions starting from `start` (the
        /// caller's `prev_size`) using `start_of_group + group_size - tmp_offset`. The
        /// first produced position can be `>= start`, but with multiple deserialization
        /// runs the value grows with `start`. We reproduce the same final positions:
        /// shift each produce-zone position into `[frame_prev_size, frame_prev_size + limit)`.
        sliced_data.push_back(pos - skip_end_rel + frame_prev_size);
    }

    return std::make_unique<SubstreamsCacheSparseOffsetsElement>(
        std::move(sliced_column),
        /*old_size_=*/0,
        /*read_rows_=*/limit,
        /*skipped_values_rows_=*/skipped);
}

}
