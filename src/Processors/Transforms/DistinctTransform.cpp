#include <Processors/Transforms/DistinctTransform.h>

#include <algorithm>
#include <utility>

#include <Interpreters/Squashing.h>
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
    const UInt64 max_bytes_before_pass_through_,
    DistinctSetMemoryTracker::SharedCounter shared_set_bytes_,
    UInt64 coalesce_rows_,
    bool allow_spilling_)
    : ISimpleTransform(header_, header_, true)
    , set_memory(shared_set_bytes_)
    , distinct_set(std::in_place, *header_, columns_, set_size_limits_, skip_null_keys_)
    , limit_hint(limit_hint_)
    , max_bytes_before_pass_through(max_bytes_before_pass_through_)
    , coalesce_rows(coalesce_rows_)
    , allow_spilling(allow_spilling_)
{
    chassert(!shared_set_bytes_ || (!allow_abandoning_ && !set_size_limits_.hasLimits() && max_bytes_before_pass_through == 0));
    /// Coalescing would delay the memory snapshots attached to the output chunks.
    chassert(!shared_set_bytes_ || coalesce_rows == 0);
    if (allow_abandoning_)
        abandon_controller.emplace();
}

IProcessor::Status DistinctTransform::prepare()
{
    /// Emit the held back rows once the input is exhausted.
    if (!pending_chunks.empty() && !has_input && !has_output && input.isFinished())
    {
        output_data.chunk = flushPendingChunks();
        has_output = true;
    }

    return ISimpleTransform::prepare();
}

Chunk DistinctTransform::flushPendingChunks()
{
    pending_rows = 0;
    pending_input_rows = 0;
    auto chunks = std::exchange(pending_chunks, {});
    if (chunks.size() == 1)
        return std::move(chunks.front());
    return Squashing::squashWithoutChunkInfo(std::move(chunks));
}

void DistinctTransform::transform(Chunk & input_chunk, Chunk & output_chunk)
{
    /// A `ChunkInfo` can carry metadata for the consumer (e.g. the watermarks of a streaming read, also in
    /// rowless marker chunks), which squashing would lose. Such a chunk is not coalesced: the held back rows
    /// are emitted first (keeping the input for the next call), then the chunk is processed on its own.
    if (coalesce_rows && !input_chunk.getChunkInfos().empty())
    {
        if (!pending_chunks.empty())
        {
            output_chunk = flushPendingChunks();
            forward_input_after_flush = true;
            return;
        }

        forward_input_after_flush = false;
        deduplicate(input_chunk);
        output_chunk.swap(input_chunk);
        return;
    }

    transform(input_chunk);
    output_chunk.swap(input_chunk);
}

ProcessorMemoryStats DistinctTransform::getMemoryStats() const
{
    ProcessorMemoryStats res;
    if (allow_spilling && distinct_set)
    {
        res.spillable_memory_bytes = distinct_set->getTotalByteCount();
        res.need_reserved_memory_bytes = res.spillable_memory_bytes;
    }
    return res;
}

size_t DistinctTransform::spill(size_t /*at_least_bytes*/)
{
    const size_t bytes = getMemoryStats().spillable_memory_bytes;
    if (!bytes)
        return 0;

    /// The downstream consumer still deduplicates exactly, including keys already emitted here.
    distinct_set.reset();
    ProfileEvents::increment(ProfileEvents::DistinctTransformsSwitchedToPassThrough);
    return bytes;
}

void DistinctTransform::transform(Chunk & chunk)
{
    if (!coalesce_rows)
    {
        deduplicate(chunk);
        return;
    }

    pending_input_rows += chunk.getNumRows();
    deduplicate(chunk);
    if (chunk.hasRows())
    {
        pending_rows += chunk.getNumRows();
        pending_chunks.push_back(std::move(chunk));
    }
    chunk = {};

    /// Also flush when the input is stopped or deduplication is released: the following chunks pass through.
    if (!pending_chunks.empty()
        && (pending_rows >= coalesce_rows || pending_input_rows / COALESCE_INPUT_BLOCKS >= coalesce_rows || no_more_data_needed
            || !distinct_set))
        chunk = flushPendingChunks();
}

void DistinctTransform::deduplicate(Chunk & chunk)
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
        set_memory.report(chunk, getOutputPort().getHeader(), 0);
        stopReading();
        return;
    }

    if (max_bytes_before_pass_through)
    {
        distinct_set->prepareForInsert(chunk);

        /// Preliminary hashing shares the query's remaining spill-threshold budget with the final
        /// transform and other operators.
        const UInt64 query_memory_usage = std::max<Int64>(0, getCurrentQueryMemoryUsage());
        const UInt64 available_memory = max_bytes_before_pass_through - std::min(max_bytes_before_pass_through, query_memory_usage);

        const size_t filtering_memory = distinct_set->estimateFilteringMemory(chunk);
        const size_t growth_memory = distinct_set->estimateGrowthMemory(chunk);
        if (filtering_memory > available_memory || growth_memory > available_memory - filtering_memory)
        {
            LOG_TRACE(getLogger("DistinctTransform"),
                "Switching preliminary DISTINCT to pass-through: {} "
                "(query memory: {}, spill threshold: {}, "
                "estimated peak extra memory for growth: {}, filtering workspace: {})",
                query_memory_usage > max_bytes_before_pass_through
                    ? "query memory exceeded the spill threshold"
                    : "projected allocations exceed the remaining spill-threshold budget",
                formatReadableSizeWithBinarySuffix(query_memory_usage),
                formatReadableSizeWithBinarySuffix(max_bytes_before_pass_through),
                formatReadableSizeWithBinarySuffix(growth_memory),
                formatReadableSizeWithBinarySuffix(filtering_memory));

            distinct_set.reset();
            ProfileEvents::increment(ProfileEvents::DistinctTransformsSwitchedToPassThrough);
            return;
        }
    }

    const size_t num_rows = chunk.getNumRows();
    chunk = distinct_set->filter(std::move(chunk));

    set_memory.report(chunk, getOutputPort().getHeader(), distinct_set->getTotalByteCount());

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
            LOG_TRACE(getLogger("DistinctTransform"),
                "Switching DISTINCT to pass-through: input is mostly unique (retained keys: {}, set memory: {})",
                distinct_set->getTotalRowCount(), formatReadableSizeWithBinarySuffix(distinct_set->getTotalByteCount()));

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
    if (max_bytes_before_pass_through)
    {
        const Int64 query_memory_usage = getCurrentQueryMemoryUsage();
        if (query_memory_usage > static_cast<Int64>(max_bytes_before_pass_through))
        {
            LOG_TRACE(getLogger("DistinctTransform"),
                "Switching preliminary DISTINCT to pass-through: query memory exceeded the spill threshold after insertion "
                "(query memory: {}, spill threshold: {})",
                formatReadableSizeWithBinarySuffix(query_memory_usage),
                formatReadableSizeWithBinarySuffix(max_bytes_before_pass_through));

            distinct_set.reset();
            ProfileEvents::increment(ProfileEvents::DistinctTransformsSwitchedToPassThrough);
            return;
        }
    }
}

}
