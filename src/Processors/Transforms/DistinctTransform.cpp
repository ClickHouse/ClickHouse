#include <Processors/Transforms/DistinctTransform.h>

#include <Common/MemoryTrackerUtils.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

namespace DB
{

DistinctTransform::DistinctTransform(
    SharedHeader header_,
    const SizeLimits & set_size_limits_,
    const UInt64 limit_hint_,
    const Names & columns_,
    const UInt64 max_bytes_before_pass_through_)
    : ISimpleTransform(header_, header_, true)
    , distinct_set(*header_, columns_, set_size_limits_)
    , limit_hint(limit_hint_)
    , max_bytes_before_pass_through(max_bytes_before_pass_through_)
{
}

void DistinctTransform::transform(Chunk & chunk)
{
    if (unlikely(!chunk.hasRows()))
        return;

    /// The set was dropped under memory pressure, the chunk flows through unchanged.
    if (pass_through)
        return;

    /// Special case - only const columns, return single row.
    if (unlikely(!distinct_set.hasKeyColumns()))
    {
        removeSpecialColumnRepresentations(chunk);
        convertToFullIfConst(chunk);

        auto columns = chunk.detachColumns();
        for (auto & column : columns)
            column = column->cut(0, 1);

        chunk.setColumns(std::move(columns), 1);
        stopReading();
        return;
    }

    chunk = distinct_set.filter(std::move(chunk));

    /// A preliminary DISTINCT is only an optimization, its result does not have to be exact. When the
    /// memory usage of the query exceeds the threshold, free the set and let the final DISTINCT (which is
    /// able to spill to disk) deal with the duplicates. This check does not depend on the chunk: the set
    /// must be freed under memory pressure even when the chunks stop producing new rows.
    if (max_bytes_before_pass_through && getCurrentQueryMemoryUsage() > static_cast<Int64>(max_bytes_before_pass_through))
    {
        LOG_DEBUG(
            getLogger("DistinctTransform"),
            "Query memory usage exceeded the threshold ({}), preliminary DISTINCT switches to pass-through",
            formatReadableSizeWithBinarySuffix(max_bytes_before_pass_through));

        distinct_set.clear();
        pass_through = true;
        return;
    }

    /// Nothing new in this chunk (or the new rows were dropped by the size limits with the 'break'
    /// overflow mode).
    if (!chunk.hasRows())
        return;

    /// Stop reading if we already reached the limit.
    if (limit_hint && distinct_set.getTotalRowCount() >= limit_hint)
        stopReading();
}

}
