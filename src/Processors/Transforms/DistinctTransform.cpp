#include <Processors/Transforms/DistinctTransform.h>

#include <Common/MemoryTrackerUtils.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

namespace DB
{

void DeduplicationAbandonController::update(size_t num_rows, size_t num_unique_rows, size_t set_bytes)
{
    if (abandoned)
        return;

    ++chunks_observed;
    rows_observed += num_rows;
    unique_rows_observed += num_unique_rows;

    if (chunks_observed < OBSERVATION_CHUNK_COUNT && set_bytes < MAX_OBSERVATION_SET_BYTES)
        return;

    double unique_rate = static_cast<double>(unique_rows_observed) / static_cast<double>(rows_observed);
    abandoned = unique_rate >= UNIQUE_RATE_THRESHOLD;
}

DistinctTransform::DistinctTransform(
    SharedHeader header_,
    const SizeLimits & set_size_limits_,
    const UInt64 limit_hint_,
    const Names & columns_,
    bool allow_abandoning_,
    bool skip_null_keys_,
    const UInt64 max_bytes_before_pass_through_)
    : ISimpleTransform(header_, header_, true)
    , distinct_set(*header_, columns_, set_size_limits_, skip_null_keys_)
    , limit_hint(limit_hint_)
    , max_bytes_before_pass_through(max_bytes_before_pass_through_)
{
    if (allow_abandoning_)
        abandon_controller.emplace();
}

void DistinctTransform::transform(Chunk & chunk)
{
    if (unlikely(!chunk.hasRows()))
        return;

    /// The set was dropped: under memory pressure (pass_through) or because the deduplication was not
    /// removing enough rows (abandoned); the chunk flows through unchanged.
    if (pass_through || (abandon_controller && abandon_controller->isAbandoned()))
        return;

    /// A constant NULL key component makes every key contain a NULL, so a consumer that skips NULL
    /// keys drops all rows; emit nothing and stop the input.
    if (distinct_set.hasConstNullKey())
    {
        chunk.setColumns(chunk.cloneEmptyColumns(), 0);
        stopReading();
        return;
    }

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

    const size_t num_rows = chunk.getNumRows();
    chunk = distinct_set.filter(std::move(chunk));

    /// In case of overflow_mode = 'break' the limits check inside the filter does not throw. Stop
    /// reading, but still emit the new rows of the current chunk (their keys are already in the set):
    /// 'break' means return a partial result as if the source data ran out, not discard it.
    if (distinct_set.isLimitReached())
        stopReading();

    if (abandon_controller)
    {
        /// The rate is measured against the rows the transform received: the rows dropped as NULL keys
        /// (in the skip_null_keys mode, inside the filter) count as removed by the deduplication, so a
        /// stream that mostly consists of NULL keys keeps the transform even when the non-NULL part is
        /// unique - dropping the NULL rows is exactly the reduction the consumer benefits from.
        abandon_controller->update(num_rows, chunk.getNumRows(), distinct_set.getTotalByteCount());
        if (abandon_controller->isAbandoned())
        {
            /// The new rows of the current chunk are still emitted (the following chunks flow
            /// through unfiltered).
            distinct_set.clear();
            return;
        }
    }

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

    /// Nothing new in this chunk.
    if (!chunk.hasRows())
        return;

    /// Stop reading if we already reached the limit.
    if (limit_hint && distinct_set.getTotalRowCount() >= limit_hint)
        stopReading();
}

}
