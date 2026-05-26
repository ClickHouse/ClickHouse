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

    /// Find the entry that covers `abs_row_start`. Entries are inserted in order of
    /// ascending `start_row_in_part`, so a linear scan picks the right starting chunk
    /// in the analyzer's `O(num_chunks)` (~16 entries on a single part). For workloads
    /// with many parts, the per-reader bucket cache in `IMergeTreeReader` makes this
    /// O(1) after the first call.
    const SparseOffsetsRange * start = nullptr;
    size_t start_idx = 0;
    for (size_t i = 0; i < ranges.size(); ++i)
    {
        const auto & r = ranges[i];
        const size_t r_end = r.start_row_in_part + r.total_rows;
        if (r.start_row_in_part <= abs_row_start && abs_row_start < r_end)
        {
            start = &r;
            start_idx = i;
            break;
        }
    }
    if (!start)
        return nullptr;

    const size_t start_end_row = start->start_row_in_part + start->total_rows;
    const bool single_chunk = abs_row_end <= start_end_row;

    /// `[skip_start_rel, skip_end_rel)` is the rows_offset zone (non-defaults here count
    /// as `skipped_values_rows`). `[skip_end_rel, produce_end_rel)` is the produce zone
    /// (non-defaults here are emitted to the offsets column). Both are relative to the
    /// first chunk's `start_row_in_part`.
    const size_t skip_start_rel = abs_row_start - start->start_row_in_part;
    const size_t skip_end_rel = skip_start_rel + rows_offset;

    if (single_chunk)
    {
        /// Fast path: scan window lies inside one stored chunk. Return a deferred-slice
        /// descriptor that points into the chunk's offsets; the consumer appends
        /// `src[i] + shift` directly into its persistent offsets column, avoiding both
        /// an intermediate allocation and a later `insertRangeFrom` copy.
        const size_t produce_end_rel = skip_end_rel + limit;
        const auto & src_offsets = assert_cast<const ColumnUInt64 &>(*start->offsets).getData();
        const auto * begin = src_offsets.data();
        const auto * end = begin + src_offsets.size();

        const auto * produce_zone_begin = std::lower_bound(begin, end, skip_end_rel);
        size_t skipped = 0;
        if (rows_offset != 0)
        {
            const auto * skip_zone_begin = std::lower_bound(begin, produce_zone_begin, skip_start_rel);
            skipped = produce_zone_begin - skip_zone_begin;
        }
        const auto * produce_zone_end = std::lower_bound(produce_zone_begin, end, produce_end_rel);

        const UInt64 shift = static_cast<UInt64>(frame_prev_size) - static_cast<UInt64>(skip_end_rel);
        return std::make_unique<SubstreamsCacheSparseOffsetsElement>(
            produce_zone_begin,
            static_cast<size_t>(produce_zone_end - produce_zone_begin),
            shift,
            /*read_rows_=*/limit,
            /*skipped_values_rows_=*/skipped);
    }

    /// Straddle: the scan window crosses one or more chunk boundaries. Walk the chunks
    /// in order, slice each one, and stitch the pieces into a fresh column. The disk
    /// fallback would be incorrect here because the scan's `DeserializeStateSparse` was
    /// never advanced through the SparseOffsets stream (previous calls were cache hits),
    /// so we must always produce a result. This path is rare (chunk_marks >> scan
    /// max_block_size), so the extra alloc is acceptable.
    auto stitched = ColumnUInt64::create();
    auto & stitched_data = stitched->getData();

    /// Compute size first to avoid `push_back` growth and to enable a single allocation.
    size_t skipped_total = 0;
    size_t produce_total = 0;
    for (size_t i = start_idx; i < ranges.size(); ++i)
    {
        const auto & r = ranges[i];
        const size_t r_start = r.start_row_in_part;
        const size_t r_end = r_start + r.total_rows;
        if (r_start >= abs_row_end)
            break;
        if (r_end <= abs_row_start)
            continue;

        const auto & rd = assert_cast<const ColumnUInt64 &>(*r.offsets).getData();
        const auto * b = rd.data();
        const auto * e = b + rd.size();

        const size_t skip_lo_abs = std::max(abs_row_start, r_start);
        const size_t skip_hi_abs = std::min(abs_row_start + rows_offset, r_end);
        const size_t produce_lo_abs = std::max(abs_row_start + rows_offset, r_start);
        const size_t produce_hi_abs = std::min(abs_row_end, r_end);

        if (skip_hi_abs > skip_lo_abs)
        {
            const size_t lo = skip_lo_abs - r_start;
            const size_t hi = skip_hi_abs - r_start;
            const auto * a = std::lower_bound(b, e, lo);
            const auto * c = std::lower_bound(a, e, hi);
            skipped_total += c - a;
        }
        if (produce_hi_abs > produce_lo_abs)
        {
            const size_t lo = produce_lo_abs - r_start;
            const size_t hi = produce_hi_abs - r_start;
            const auto * a = std::lower_bound(b, e, lo);
            const auto * c = std::lower_bound(a, e, hi);
            produce_total += c - a;
        }
    }
    stitched_data.resize(produce_total);

    size_t out_pos = 0;
    for (size_t i = start_idx; i < ranges.size(); ++i)
    {
        const auto & r = ranges[i];
        const size_t r_start = r.start_row_in_part;
        const size_t r_end = r_start + r.total_rows;
        if (r_start >= abs_row_end)
            break;
        if (r_end <= abs_row_start)
            continue;

        const size_t produce_lo_abs = std::max(abs_row_start + rows_offset, r_start);
        const size_t produce_hi_abs = std::min(abs_row_end, r_end);
        if (produce_hi_abs <= produce_lo_abs)
            continue;

        const auto & rd = assert_cast<const ColumnUInt64 &>(*r.offsets).getData();
        const auto * b = rd.data();
        const auto * e = b + rd.size();
        const size_t lo = produce_lo_abs - r_start;
        const size_t hi = produce_hi_abs - r_start;
        const auto * a = std::lower_bound(b, e, lo);
        const auto * c = std::lower_bound(a, e, hi);

        /// Each source position `p` is relative to `r_start`. To put it in the consumer's
        /// frame, shift to `(p + r_start) - (abs_row_start + rows_offset) + frame_prev_size`.
        const UInt64 shift = static_cast<UInt64>(r_start)
            + static_cast<UInt64>(frame_prev_size)
            - static_cast<UInt64>(abs_row_start + rows_offset);
        const size_t n = c - a;
        UInt64 * __restrict__ dst = stitched_data.data() + out_pos;
        const UInt64 * __restrict__ src = a;
        for (size_t k = 0; k < n; ++k)
            dst[k] = src[k] + shift;
        out_pos += n;
    }

    return std::make_unique<SubstreamsCacheSparseOffsetsElement>(
        ColumnPtr(std::move(stitched)),
        /*old_size_=*/0,
        /*read_rows_=*/limit,
        /*skipped_values_rows_=*/skipped_total);
}

}
