#include <Processors/Transforms/DistinctTransform.h>

#include <Common/MemoryTrackerUtils.h>
#include <Common/ProfileEvents.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
    extern const Event DistinctTransformsAbandonedDeduplication;
    extern const Event DistinctTransformsSwitchedToPassThrough;
}

namespace DB
{

bool DeduplicationAbandonController::update(size_t num_rows, size_t num_unique_rows, size_t set_bytes)
{
    ++chunks_observed;
    rows_observed += num_rows;
    unique_rows_observed += num_unique_rows;

    if (chunks_observed < OBSERVATION_CHUNK_COUNT && set_bytes < MAX_OBSERVATION_SET_BYTES)
        return false;

    double unique_rate = static_cast<double>(unique_rows_observed) / static_cast<double>(rows_observed);
    return unique_rate >= UNIQUE_RATE_THRESHOLD;
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
    , distinct_set(std::in_place, *header_, columns_, set_size_limits_, skip_null_keys_)
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

    /// Releasing the filter permanently switches subsequent chunks to pass-through.
    if (!distinct_set)
        return;

    /// A constant `NULL` key component makes every key contain a `NULL`, so a consumer that skips `NULL`
    /// keys drops all rows; emit nothing and stop the input.
    if (distinct_set->hasConstNullKey())
    {
        chunk.setColumns(chunk.cloneEmptyColumns(), 0);
        stopReading();
        return;
    }

    /// Special case - only const columns, return single row.
    if (unlikely(!distinct_set->hasKeyColumns()))
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

    if (max_bytes_before_pass_through)
    {
        distinct_set->prepareForInsert(chunk);
        const auto available = getMostStrictAvailableSystemMemory();
        if (available && distinct_set->estimateGrowthMemory(chunk.getNumRows()) > *available)
        {
            distinct_set.reset();
            ProfileEvents::increment(ProfileEvents::DistinctTransformsSwitchedToPassThrough);
            return;
        }
    }

    const size_t num_rows = chunk.getNumRows();
    chunk = distinct_set->filter(std::move(chunk));

    /// Return the current chunk and stop before releasing the set if a size limit or the hint is reached.
    if (distinct_set->isLimitReached() || (limit_hint && distinct_set->getTotalRowCount() >= limit_hint))
    {
        stopReading();
        return;
    }

    if (abandon_controller)
    {
        /// The rate is measured against the rows the transform received: the rows dropped as `NULL` keys
        /// (in the `skip_null_keys` mode, inside the filter) count as removed by the deduplication, so a
        /// stream that mostly consists of `NULL` keys keeps the transform even when the non-`NULL` part is
        /// unique - dropping the `NULL` rows is exactly the reduction the consumer benefits from.
        if (abandon_controller->update(num_rows, chunk.getNumRows(), distinct_set->getTotalByteCount()))
        {
            /// The new rows of the current chunk are still emitted (the following chunks flow
            /// through unfiltered).
            distinct_set.reset();
            ProfileEvents::increment(ProfileEvents::DistinctTransformsAbandonedDeduplication);
            return;
        }
    }

    /// Preliminary hashing can release its set under memory pressure because a downstream step
    /// deduplicates the output exactly. This also gives up any remaining local limit hint. The set
    /// can be released even when the current chunk produces no new rows.
    if (max_bytes_before_pass_through && getCurrentQueryMemoryUsage() > static_cast<Int64>(max_bytes_before_pass_through))
    {
        LOG_DEBUG(
            getLogger("DistinctTransform"),
            "Query memory usage exceeded the threshold ({}), preliminary DISTINCT switches to pass-through",
            formatReadableSizeWithBinarySuffix(max_bytes_before_pass_through));

        distinct_set.reset();
        ProfileEvents::increment(ProfileEvents::DistinctTransformsSwitchedToPassThrough);
        return;
    }
}

}
