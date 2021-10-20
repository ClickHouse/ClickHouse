#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>

#include <algorithm>
#include <vector>
#include <compare>
#include <numeric>
#include <unordered_map>
#include <map>
#include <iostream>
#include <set>
#include <cassert>


#include <base/logger_useful.h>
#include <base/types.h>
#include <base/scope_guard.h>
#include "IO/WriteBufferFromString.h"
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/IntersectionsIndexes.h>

namespace DB
{

class ParallelReplicasReadingCoordinator::Impl
{
public:

    using PartitionReadRequestPtr = std::unique_ptr<PartitionReadRequest>;

    using PartToMarkRanges = std::map<String, MarkRangesIntersectionsIndex>;

    struct PartitionReading
    {
        PartRangesIntersectionsIndex part_ranges;
        PartToMarkRanges mark_ranges_in_part;
    };

    using PartitionToBlockRanges = std::map<String, PartitionReading>;
    PartitionToBlockRanges partitions;

    std::mutex mutex;

    PartitionReadResponce handleRequest(PartitionReadRequest request);

private:

    void checkConsistencyOrThrow();

};


void ParallelReplicasReadingCoordinator::Impl::checkConsistencyOrThrow()
{
    for (const auto & partition : partitions)
    {
        partition.second.part_ranges.checkConsistencyOrThrow();
        for (const auto & ranges : partition.second.mark_ranges_in_part)
            ranges.second.checkConsistencyOrThrow();
    }
}


PartitionReadResponce ParallelReplicasReadingCoordinator::Impl::handleRequest(PartitionReadRequest request)
{
    std::lock_guard lock(mutex);

    checkConsistencyOrThrow();

    // WriteBufferFromOwnString wb;
    // request.serializeToJSON(wb);
    // LOG_TRACE(&Poco::Logger::get("ParallelReplicasReadingCoordinator"), "Got request {}", wb.str());

    auto partition_it = partitions.find(request.partition_id);

    SCOPE_EXIT({
        String result;
        for (const auto & partition : partitions)
        {
            result += partition.first;
            result += partition.second.part_ranges.describe();
            for (const auto & ranges : partition.second.mark_ranges_in_part)
                result += fmt::format("{} {} \n", ranges.first, ranges.second.describe());
        }

        LOG_TRACE(&Poco::Logger::get("ParallelReplicasReadingCoordinator"), "Current state {}", result);
    });

    /// We are the first who wants to process parts in partition
    if (partition_it == partitions.end())
    {
        // LOG_TRACE(&Poco::Logger::get("ParallelReplicasReadingCoordinator"), "First to process partition");
        auto part_and_projection = request.part_name + "#" + request.projection_name;

        PartitionReading partition_reading;

        PartToRead part_to_read;
        part_to_read.range = request.block_range;
        part_to_read.name = part_and_projection;

        partition_reading.part_ranges.addPart(std::move(part_to_read));

        MarkRangesIntersectionsIndex mark_ranges_index;
        mark_ranges_index.addRanges(request.mark_ranges);

        partition_reading.mark_ranges_in_part.insert({part_and_projection, std::move(mark_ranges_index)});
        partitions.insert({request.partition_id, std::move(partition_reading)});

        return {.denied = false, .mark_ranges = std::move(request.mark_ranges)};
    }


    auto & partition_reading = partition_it->second;

    auto part_and_projection = request.part_name + "#" + request.projection_name;
    PartToRead part_to_read;
    part_to_read.range = request.block_range;
    part_to_read.name = part_and_projection;

    auto number_of_part_intersection = partition_reading.part_ranges.numberOfIntersectionsWith(part_to_read.range);

    /// We are intersecting with another parts, probably replicas have different sets of parts
    /// Or maybe there is a bug in merges assigning...
    /// Nothing to update in our state
    if (number_of_part_intersection >= 2)
    {
        // LOG_FATAL(&Poco::Logger::get("ParallelReplicasReadingCoordinator"), "Denied 1");
        return {.denied = true, .mark_ranges = {}};
    }


    if (number_of_part_intersection == 1)
    {
        if (!partition_reading.part_ranges.checkPartIsSuitable(part_to_read))
        {
            // LOG_FATAL(&Poco::Logger::get("ParallelReplicasReadingCoordinator"), "Denied 2");
            return {.denied = true, .mark_ranges = {}};
        }


        auto marks_it = partition_reading.mark_ranges_in_part.find(part_and_projection);

        /// FIXME
        if (marks_it == partition_reading.mark_ranges_in_part.end())
            throw std::runtime_error("aaa");

        MarkRanges result;
        for (auto & range : request.mark_ranges)
        {
            auto number_of_mark_intersection = marks_it->second.numberOfIntersectionsWith(range);
            if (number_of_mark_intersection == 0)
                result.push_back(range);
            else
            {
                auto new_ranges = marks_it->second.getNewRanges(range);

                for (const auto & new_range : new_ranges)
                    result.push_back(new_range);
            }
        }

        if (result.empty())
        {
            // LOG_FATAL(&Poco::Logger::get("ParallelReplicasReadingCoordinator"), "Denied 3");
            return {.denied = true, .mark_ranges = {}};
        }


        marks_it->second.addRanges(result);

        return {.denied = false, .mark_ranges = std::move(result)};
    }


    partition_reading.part_ranges.addPart(std::move(part_to_read));

    MarkRangesIntersectionsIndex mark_ranges_index;
    mark_ranges_index.addRanges(request.mark_ranges);
    partition_reading.mark_ranges_in_part.insert({part_and_projection, std::move(mark_ranges_index)});

    return {.denied = false, .mark_ranges = std::move(request.mark_ranges)};
}



/**
 *
 *
 *  [..)  [.......) [..) [...) [......) [.......)       [....) [.......) [........)
 *  |                                           |       |                         |
 *  |     ^ mark ranges                         |       |                         |
 *  |                                           |       |                         |
 *  |                                           |       |                         |
 *  [        part_name#projection_name          ]  ...  [part_name#projection_name]
 *
 *  ^ boundary (Left)           boundary(Right) ^       ^ boundary (Left)
 *
 *  [                              partition                                      ]
 */


PartitionReadResponce ParallelReplicasReadingCoordinator::handleRequest(PartitionReadRequest request)
{
    return pimpl->handleRequest(std::move(request));
}

ParallelReplicasReadingCoordinator::ParallelReplicasReadingCoordinator()
{
    pimpl = std::make_unique<ParallelReplicasReadingCoordinator::Impl>();
}

ParallelReplicasReadingCoordinator::~ParallelReplicasReadingCoordinator() = default;

}
