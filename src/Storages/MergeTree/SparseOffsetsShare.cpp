#include <Storages/MergeTree/SparseOffsetsShare.h>

#include <Columns/ColumnVector.h>
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
    size_t num_rows,
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
    const size_t abs_row_end = abs_row_start + num_rows;

    /// We do not span multiple stored ranges in one slice. Scan `readRows` calls stay
    /// within one `MarkRange` (the analyzer's storage unit), so the contains-relation
    /// is the common case. A request crossing two stored ranges returns nullptr and
    /// the scan falls back to a normal disk read for that call.
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

    const size_t relative_start = abs_row_start - found->start_row_in_part;
    const size_t relative_end = relative_start + num_rows;

    /// Stored offsets are positions in [0, range.total_rows) relative to
    /// `found->start_row_in_part`. The consumer (`readOrGetCachedSparseOffsets`) reads
    /// positions from the cached column verbatim into its result column, so we emit
    /// positions in [frame_prev_size, frame_prev_size + num_rows) to land in the
    /// consumer's accumulating frame.
    const auto & src_offsets = assert_cast<const ColumnUInt64 &>(*found->offsets).getData();

    auto sliced_column = ColumnUInt64::create();
    auto & sliced_data = sliced_column->getData();
    sliced_data.reserve(src_offsets.size());

    size_t skipped = 0;
    for (UInt64 pos : src_offsets)
    {
        if (pos < relative_start)
        {
            ++skipped;
            continue;
        }
        if (pos >= relative_end)
            break;
        sliced_data.push_back(pos - relative_start + frame_prev_size);
    }

    return std::make_unique<SubstreamsCacheSparseOffsetsElement>(
        std::move(sliced_column),
        /*old_size_=*/0,
        /*read_rows_=*/num_rows,
        /*skipped_values_rows_=*/skipped);
}

}
