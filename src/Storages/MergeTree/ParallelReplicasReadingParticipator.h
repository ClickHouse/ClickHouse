#pragma once

#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>


namespace DB
{


MergeTreeReadTaskPtr modifyTaskAccordingToParallelReplicasReading(MergeTreeReadTaskPtr selected_task)
{
    auto task = std::move(selected_task);

    String partition_id = task->data_part->info.partition_id;
    String part_name;
    String projection_name;

    if (task->data_part->isProjectionPart())
    {
        part_name = task->data_part->getParentPart()->name;
        projection_name  = task->data_part->name;
    }
    else
    {
        part_name = task->data_part->name;
        projection_name = "";
    }


    PartBlockRange block_range
    {
        .begin = task->data_part->info.min_block,
        .end = task->data_part->info.max_block
    };

    PartitionReadRequest request
    {
        .partition_id = std::move(partition_id),
        .part_name = std::move(part_name),
        .projection_name = std::move(projection_name),
        .block_range = std::move(block_range),
        .mark_ranges = std::move(task->mark_ranges)
    };


    /// Send...

    PartitionReadResponce responce;

    task->mark_ranges = std::move(responce.mark_ranges);

    return task;
}


}
