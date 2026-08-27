#pragma once

#include <Storages/MergeTree/Streaming/Cursors/CursorUtils.h>
#include <Storages/MergeTree/Streaming/Subscription/MergeTreeBoundsSubscription.h>

#include <Core/Streaming/Settings.h>

#include <Processors/Chunk.h>

#include <chrono>
#include <map>
#include <set>

namespace DB
{

struct ClassifiedPartitions;

/// Dynamically changed state of data reading from the read round loop.
class StreamReadState
{
public:
    explicit StreamReadState(const StreamSettings & stream_settings);

    void startReadRound(const ClassifiedPartitions & partitions, const std::map<std::string, Int64> & safe_block_numbers);
    void finalizeReadRound();
    bool readRoundInProgress() const;

    void updatePartitionCursor(const std::string & partition, PartitionCursor cursor);
    void updatePartitionWatermark(const std::string & partition, Field watermark);
    void updateGlobalWatermark(const Field & watermark);
    void updatePartitionSet(const ClassifiedPartitions & partitions);
    void markSourceIdle();

    bool hasWork(const ClassifiedPartitions & partitions) const;
    Int64 calculateTimeToNextIdle(const StreamSettings & stream_settings) const;
    bool isSourceMarkedIdle() const;

    PartitionCursor getPartitionCursor(const std::string & partition) const;
    Field getPartitionWatermark(const std::string & partition) const;
    bool isPartitionIdle(const std::string & partition, const StreamSettings & stream_settings) const;

private:
    /// Read position.
    std::map<std::string, PartitionCursor> partition_cursors;
    std::map<std::string, Int64> reading_up_to_block_numbers;
    bool round_in_progress = false;

    /// Watermarks
    std::map<std::string, Field> partition_watermarks;
    Field last_emitted_watermark;

    /// Idle
    std::map<std::string, std::chrono::steady_clock::time_point> partition_last_read_time;
    std::set<std::string> reported_idle_partitions;
    bool emitted_source_idle = false;
};

}
