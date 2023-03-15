#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>

#include <map>

#include <Common/logger_useful.h>
#include <base/scope_guard.h>
#include <Common/Stopwatch.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/MergeTree/IntersectionsIndexes.h>


namespace DB
{

class ParallelReplicasReadingCoordinator::Impl
{
public:
    using PartitionReadRequestPtr = std::unique_ptr<PartitionReadRequest>;
    using PartToMarkRanges = std::map<PartToRead::PartAndProjectionNames, HalfIntervals>;

    struct PartitionReading
    {
        PartSegments part_ranges;
        PartToMarkRanges mark_ranges_in_part;
    };

    using PartitionToBlockRanges = std::map<String, PartitionReading>;
    PartitionToBlockRanges partitions;

    std::mutex mutex;

    PartitionReadResponse handleRequest(PartitionReadRequest request);
};


PartitionReadResponse ParallelReplicasReadingCoordinator::Impl::handleRequest(PartitionReadRequest request)
{
    auto * log = &Poco::Logger::get("ParallelReplicasReadingCoordinator");
    Stopwatch watch;

    String request_description = request.toString();
    std::lock_guard lock(mutex);

    auto partition_it = partitions.find(request.partition_id);

    PartToRead::PartAndProjectionNames part_and_projection
    {
        .part = request.part_name,
        .projection = request.projection_name
    };

    /// We are the first who wants to process parts in partition
    if (partition_it == partitions.end())
    {
        PartitionReading partition_reading;

        PartToRead part_to_read;
        part_to_read.range = request.block_range;
        part_to_read.name = part_and_projection;

        partition_reading.part_ranges.addPart(std::move(part_to_read));

        /// As this query is first in partition, we will accept all ranges from it.
        /// We need just to update our state.
        auto request_ranges = HalfIntervals::initializeFromMarkRanges(request.mark_ranges);
        auto mark_ranges_index = HalfIntervals::initializeWithEntireSpace();
        mark_ranges_index.intersect(request_ranges.negate());

        partition_reading.mark_ranges_in_part.insert({part_and_projection, std::move(mark_ranges_index)});
        partitions.insert({request.partition_id, std::move(partition_reading)});

        LOG_TRACE(log, "Request is first in partition, accepted in {} ns: {}", watch.elapsed(), request_description);
        return {.denied = false, .mark_ranges = std::move(request.mark_ranges)};
    }

    auto & partition_reading = partition_it->second;

    PartToRead part_to_read;
    part_to_read.range = request.block_range;
    part_to_read.name = part_and_projection;

    auto part_intersection_res = partition_reading.part_ranges.getIntersectionResult(part_to_read);

    switch (part_intersection_res)
    {
        case PartSegments::IntersectionResult::REJECT:
        {
            LOG_TRACE(log, "Request rejected in {} ns: {}", watch.elapsed(), request_description);
            return {.denied = true, .mark_ranges = {}};
        }
        case PartSegments::IntersectionResult::EXACTLY_ONE_INTERSECTION:
        {
            auto marks_it = partition_reading.mark_ranges_in_part.find(part_and_projection);

            auto & intervals_to_do = marks_it->second;
            auto result = HalfIntervals::initializeFromMarkRanges(request.mark_ranges);
            result.intersect(intervals_to_do);

            /// Update intervals_to_do
            intervals_to_do.intersect(HalfIntervals::initializeFromMarkRanges(std::move(request.mark_ranges)).negate());

            auto result_ranges = result.convertToMarkRangesFinal();
            const bool denied = result_ranges.empty();

            if (denied)
                LOG_TRACE(log, "Request rejected due to intersection in {} ns: {}", watch.elapsed(), request_description);
            else
                LOG_TRACE(log, "Request accepted partially in {} ns: {}", watch.elapsed(), request_description);

            return {.denied = denied, .mark_ranges = std::move(result_ranges)};
        }
        case PartSegments::IntersectionResult::NO_INTERSECTION:
        {
            partition_reading.part_ranges.addPart(std::move(part_to_read));

            auto mark_ranges_index = HalfIntervals::initializeWithEntireSpace().intersect(
            HalfIntervals::initializeFromMarkRanges(request.mark_ranges).negate()
            );
            partition_reading.mark_ranges_in_part.insert({part_and_projection, std::move(mark_ranges_index)});

            LOG_TRACE(log, "Request accepted in {} ns: {}", watch.elapsed(), request_description);
            return {.denied = false, .mark_ranges = std::move(request.mark_ranges)};
        }
    }

    UNREACHABLE();
}

PartitionReadResponse ParallelReplicasReadingCoordinator::handleRequest(PartitionReadRequest request)
{
    return pimpl->handleRequest(std::move(request));
}

ParallelReplicasReadingCoordinator::ParallelReplicasReadingCoordinator()
{
    pimpl = std::make_unique<ParallelReplicasReadingCoordinator::Impl>();
}

ParallelReplicasReadingCoordinator::~ParallelReplicasReadingCoordinator() = default;

}
