#include <Storages/MergeTree/Streaming/ReadState.h>
#include <Storages/MergeTree/Streaming/PartitionsClassification.h>
#include <Storages/MergeTree/Streaming/Cursors/CursorUtils.h>

#include <Interpreters/Streaming/Utils.h>

#include <utility>

namespace DB
{

ReadState::ReadState(const StreamSettings & stream_settings)
    : partition_cursors(buildMergeTreeCursor(stream_settings.cursor))
{
}

void ReadState::startReadRound(const ClassifiedPartitions & partitions, const std::map<std::string, Int64> & safe_block_numbers)
{
    const auto now = std::chrono::steady_clock::now();

    round_in_progress = true;
    reading_up_to_block_numbers.clear();
    emitted_source_idle = false;
    reported_idle_partitions = partitions.idle_partitions;

    for (const auto & partition_id : partitions.changed_partitions)
    {
        reading_up_to_block_numbers[partition_id] = safe_block_numbers.at(partition_id);
        partition_last_read_time[partition_id] = now;
    }
}

void ReadState::finalizeReadRound()
{
    for (const auto & [partition_id, safe_block_number] : reading_up_to_block_numbers)
    {
        auto & position = partition_cursors[partition_id];
        position.block_number = safe_block_number + 1;
        position.block_offset = -1;
    }

    reading_up_to_block_numbers.clear();
    round_in_progress = false;
}

bool ReadState::readRoundInProgress() const
{
    return round_in_progress;
}

void ReadState::updatePartitionCursor(const std::string & partition, PartitionCursor cursor)
{
    partition_last_read_time[partition] = std::chrono::steady_clock::now();
    partition_cursors[partition] = cursor;
}

void ReadState::updatePartitionWatermark(const std::string & partition, Field watermark)
{
    partition_last_read_time[partition] = std::chrono::steady_clock::now();

    auto & current = partition_watermarks[partition];
    if (watermark > current)
        current = std::move(watermark);
}

void ReadState::updateGlobalWatermark(const Field & watermark)
{
    if (watermark > last_emitted_watermark)
        last_emitted_watermark = watermark;
}

void ReadState::updatePartitionSet(const ClassifiedPartitions & partitions)
{
    std::set<std::string> table_partitions;
    table_partitions.insert_range(partitions.changed_partitions);
    table_partitions.insert_range(partitions.unchanged_partitions);
    table_partitions.insert_range(partitions.idle_partitions);

    std::erase_if(partition_cursors, [&](const auto & entry) { return !table_partitions.contains(entry.first); });
    std::erase_if(partition_last_read_time, [&](const auto & entry) { return !table_partitions.contains(entry.first); });
    std::erase_if(partition_watermarks, [&](const auto & entry) { return !table_partitions.contains(entry.first); });
    std::erase_if(reported_idle_partitions, [&](const auto & partition_id) { return !table_partitions.contains(partition_id); });

    const auto now = std::chrono::steady_clock::now();
    for (const auto & partition_id : table_partitions)
    {
        const bool added_cursor = partition_cursors.try_emplace(partition_id, PartitionCursor{}).second;
        const bool added_read_time = partition_last_read_time.try_emplace(partition_id, now).second;
        const bool added_watermark = partition_watermarks.try_emplace(partition_id, last_emitted_watermark).second;

        if (added_cursor || added_read_time || added_watermark)
            emitted_source_idle = false;
    }
}

void ReadState::markSourceIdle()
{
    emitted_source_idle = true;
}

bool ReadState::hasWork(const ClassifiedPartitions & partitions) const
{
    if (!partitions.changed_partitions.empty())
        return true;

    /// An idle partition being excluded from the watermark calculation - so global watermark may be extended.
    for (const auto & idle_partition : partitions.idle_partitions)
        if (!reported_idle_partitions.contains(idle_partition))
            return true;

    return false;
}

Int64 ReadState::calculateTimeToNextIdle(const StreamSettings & stream_settings) const
{
    const auto & watermark = stream_settings.watermark;
    const auto now = std::chrono::steady_clock::now();

    if (!watermark || watermark->idle_timeout.count() == 0 || partition_last_read_time.empty())
        return -1;

    std::chrono::time_point<std::chrono::steady_clock> next_idle_time;
    for (const auto & [_, last_read_time] : partition_last_read_time)
    {
        const auto deadline = last_read_time + watermark->idle_timeout;
        if (deadline < now)
            continue;

        if (next_idle_time.time_since_epoch().count() == 0 || next_idle_time > deadline)
            next_idle_time = deadline;
    }

    if (next_idle_time.time_since_epoch().count() == 0)
        return -1;

    return std::chrono::duration_cast<std::chrono::milliseconds>(next_idle_time - now).count();
}

bool ReadState::isSourceMarkedIdle() const
{
    return emitted_source_idle;
}

PartitionCursor ReadState::getPartitionCursor(const std::string & partition) const
{
    auto it = partition_cursors.find(partition);
    return it == partition_cursors.end() ? PartitionCursor{} : it->second;
}

Field ReadState::getPartitionWatermark(const std::string & partition) const
{
    auto it = partition_watermarks.find(partition);
    return it == partition_watermarks.end() ? Field{} : it->second;
}

bool ReadState::isPartitionIdle(const std::string & partition, const StreamSettings & stream_settings) const
{
    const auto now = std::chrono::steady_clock::now();

    if (!stream_settings.watermark)
        return false;

    auto it = partition_last_read_time.find(partition);
    return it != partition_last_read_time.end() && isIdleExpired(now, it->second, stream_settings.watermark);
}

}
