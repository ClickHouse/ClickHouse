#include <Storages/MergeTree/Streaming/PartitionsClassification.h>
#include <Storages/MergeTree/Streaming/ReadState.h>

namespace DB
{

ClassifiedPartitions classifyPartitions(const ReadState & state, const std::map<String, Int64> & safe_block_numbers, const StreamSettings & stream_settings)
{
    ClassifiedPartitions classification;
    for (const auto & [partition_id, safe_block_number] : safe_block_numbers)
    {
        if (state.getPartitionCursor(partition_id).block_number <= safe_block_number)
            classification.changed_partitions.insert(partition_id);
        else if (state.isPartitionIdle(partition_id, stream_settings))
            classification.idle_partitions.insert(partition_id);
        else
            classification.unchanged_partitions.insert(partition_id);
    }

    return classification;
}

}
