#include <Processors/Transforms/DistinctTransform.h>

#include <algorithm>

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

namespace ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

namespace
{

/// Retained set bytes added since the previous output chunk. The chunk's rows are the new keys.
struct DistinctSetSizeDelta : public ChunkInfoCloneable<DistinctSetSizeDelta>
{
    explicit DistinctSetSizeDelta(UInt64 bytes_)
        : bytes(bytes_)
    {
    }
    UInt64 bytes;
};

}

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
    bool report_set_size_)
    : ISimpleTransform(header_, header_, true)
    , distinct_set(std::in_place, *header_, columns_, set_size_limits_, skip_null_keys_)
    , limit_hint(limit_hint_)
    , max_bytes_before_pass_through(max_bytes_before_pass_through_)
    , report_set_size(report_set_size_)
{
    chassert(!report_set_size || (!allow_abandoning_ && !set_size_limits_.hasLimits() && max_bytes_before_pass_through == 0));
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
        if (report_set_size)
            chunk.getChunkInfos().add(std::make_shared<DistinctSetSizeDelta>(0));
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

    if (report_set_size)
    {
        /// Only retained allocations count towards the byte limit. The reported increments add up across the
        /// disjoint sets of the parallel final transforms.
        const UInt64 set_bytes = distinct_set->getTotalByteCount();
        chassert(set_bytes >= reported_set_bytes);
        chunk.getChunkInfos().add(std::make_shared<DistinctSetSizeDelta>(set_bytes - reported_set_bytes));
        reported_set_bytes = set_bytes;
    }

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

DistinctLimitTransform::DistinctLimitTransform(const SharedHeader & header, const SizeLimits & size_limits_, size_t num_streams)
    : IProcessor(InputPorts(num_streams, header), OutputPorts(num_streams, header))
    , size_limits(size_limits_)
{
    port_pairs.reserve(num_streams);
    port_to_pair.reserve(2 * num_streams);
    auto output = outputs.begin();
    for (auto & input : inputs)
    {
        auto & pair = port_pairs.emplace_back(input, *output++);
        port_to_pair.emplace(&pair.input, &pair);
        port_to_pair.emplace(&pair.output, &pair);
    }
}

IProcessor::Status DistinctLimitTransform::prepare(const UpdatedInputPorts & updated_inputs, const UpdatedOutputPorts & updated_outputs)
{
    bool has_full_port = false;
    auto prepare_ports = [&](const auto & updated_ports)
    {
        for (const auto * port : updated_ports)
        {
            /// `BREAK` emits the chunk that reaches the limit and stops processing further port updates.
            if (limit_reached)
                break;

            auto & pair = *port_to_pair.at(port);
            const auto status = preparePair(pair);
            if (status == Status::Finished && !pair.is_finished)
            {
                pair.is_finished = true;
                ++num_finished_port_pairs;
            }
            has_full_port |= status == Status::PortFull;
        }
    };

    prepare_ports(updated_inputs);
    prepare_ports(updated_outputs);

    if (limit_reached)
    {
        for (auto & input : inputs)
            input.close();
        for (auto & output : outputs)
            output.finish();
        return Status::Finished;
    }

    if (num_finished_port_pairs == port_pairs.size())
        return Status::Finished;

    return has_full_port ? Status::PortFull : Status::NeedData;
}

IProcessor::Status DistinctLimitTransform::prepare()
{
    chassert(port_pairs.size() == 1);
    return prepare({&port_pairs.front().input}, {&port_pairs.front().output});
}

IProcessor::Status DistinctLimitTransform::preparePair(PortPair & pair)
{
    auto & input = pair.input;
    auto & output = pair.output;

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;

    auto data_chunk = input.pullData(true);
    if (data_chunk.chunk.hasRows())
    {
        auto set_size_delta = data_chunk.chunk.getChunkInfos().extract<DistinctSetSizeDelta>();
        chassert(set_size_delta);
        rows += data_chunk.chunk.getNumRows();
        bytes += set_size_delta->bytes;
        limit_reached = !size_limits.check(rows, bytes, "DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
    }
    output.pushData(std::move(data_chunk));
    return Status::PortFull;
}

}
