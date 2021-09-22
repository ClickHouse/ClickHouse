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

#include <common/types.h>
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

};



PartitionReadResponce ParallelReplicasReadingCoordinator::Impl::handleRequest(PartitionReadRequest request)
{
    std::lock_guard lock(mutex);

    // WriteBufferFromOwnString wb;
    // request.serialize(wb);
    // std::cout << wb.str() << std::endl;

    auto partition_it = partitions.find(request.partition_id);

    /// We are the first who wants to process parts in partition
    if (partition_it == partitions.end())
    {
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
    if (number_of_part_intersection > 2)
        return {.denied = true, .mark_ranges = {}};

    // std::cout << "number_of_part_intersection " << number_of_part_intersection << std::endl;

    if (number_of_part_intersection == 1)
    {
        if (!partition_reading.part_ranges.checkPartIsSuitable(part_to_read))
            return {.denied = true, .mark_ranges = {}};

        auto marks_it = partition_reading.mark_ranges_in_part.find(part_and_projection);

        /// FIXME
        if (marks_it == partition_reading.mark_ranges_in_part.end())
            throw std::runtime_error("aaa");

        MarkRanges result;
        for (auto & range : request.mark_ranges)
        {
            if (marks_it->second.numberOfIntersectionsWith(range) == 0)
                result.push_back(range);
        }

        if (result.empty())
            return {.denied = true, .mark_ranges = {}};

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
