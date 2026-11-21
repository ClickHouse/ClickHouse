#pragma once

#include <Core/Block.h>
#include <Core/StreamingHandleErrorMode.h>
#include <Core/Streaming/Settings.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Processors/ISource.h>
#include <Storages/StorageSnapshot.h>
#include <Common/Logger.h>
#include <Common/PipeFDs.h>
#include <Common/Stopwatch.h>

#include <cppkafka/configuration.h>
#include <cppkafka/consumer.h>

#include <map>
#include <memory>
#include <optional>


namespace DB
{

/// Reads the topics of a `Kafka` table for a streaming query (`SELECT ... FROM kafka_table STREAM`).
///
/// Unlike a direct `SELECT` from the table, it does not consume the messages on behalf of the consumer group:
/// it does not join the group, does not use the offsets committed by the group and never commits.
/// Instead, it assigns all partitions of the topics to its own consumer, starting at the cursor of the query:
///     {'<topic>': {'<partition>': <offset of the last read message>, ...}, ...}
/// The cursor splits its keys by dots, so a dot in a topic name is written as `%2E` (and `%` as `%25`).
/// A partition absent in the cursor is read from the beginning. An offset outside of a partition is an error.
/// The cursor of the read can be reconstructed from the `_topic`, `_partition` and `_offset` virtual columns.
///
/// A bounded stream (`STREAM BOUNDED`) finishes once it reaches the end of every partition (at the start of the read).
/// For an incremental refresh of a materialized view, it reports the cursor it advanced to through `Context::getStreamingCursor`.
/// An unbounded stream checks the topics for new partitions periodically.
///
/// When there are no messages, the source waits for the consumer queue asynchronously (the queue writes to a pipe when
/// it gets a new event), so that it does not occupy a thread of the pipeline.
class KafkaStreamSource final : public ISource
{
public:
    struct Params
    {
        cppkafka::Configuration consumer_configuration;
        Names topics;
        String format_name;
        StreamingHandleErrorMode handle_error_mode = StreamingHandleErrorMode::DEFAULT;
        size_t schema_registry_skip_bytes = 0;
        size_t poll_max_batch_size = 0;
        size_t poll_timeout_ms = 0;
        size_t max_block_size = 0;
        StorageID storage_id = StorageID::createEmpty();
    };

    KafkaStreamSource(
        Params params_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        const Names & columns,
        const StreamSettings & stream_settings_,
        LoggerPtr log_);

    ~KafkaStreamSource() override;

    String getName() const override { return "KafkaStreamSource"; }

    Status prepare() override;
    void work() override;
    int schedule() override;
#if defined(OS_LINUX) || defined(OS_DARWIN)
    std::tuple<int, uint32_t, Int64> scheduleForEvent() override;
#endif

    Chunk generate() override;
    std::optional<Chunk> tryGenerate() override;

private:
    struct PartitionState
    {
        /// The offset of the last message read (or of the cursor), -1 if nothing was read.
        Int64 last_offset = -1;
        /// The end of the partition is reached (for a bounded stream).
        bool reached_end = false;
        /// The high watermark at the start of a bounded stream.
        Int64 end_offset = 0;
    };

    using TopicPartition = std::pair<String, Int32>;

    void start();
    /// Adds the partitions of the topics that are not assigned yet, returns true if there are new ones.
    bool discoverPartitions();
    void assignPartitions();
    void drainWakeupPipe();
    bool allPartitionsReachedEnd() const;
    void finishIfAllPartitionsReachedEnd();
    void reportFinalCursor() const;

    Params params;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    const StreamSettings stream_settings;
    LoggerPtr log;

    const Block non_virtual_header;
    const Block virtual_header;

    /// The offsets of the cursor of the query, including the partitions that do not exist yet.
    std::map<TopicPartition, Int64> cursor_offsets;

    /// The consumer queue writes to it when it gets an event while the source is idle. Declared before the consumer,
    /// so that it is closed after the consumer is destroyed.
    LazyPipeFDs wakeup_pipe;
    std::unique_ptr<cppkafka::Consumer> consumer;
    std::map<TopicPartition, PartitionState> partitions;
    /// For an unbounded stream: the time since the last check for new partitions.
    Stopwatch since_partitions_discovery;
    /// The last poll returned nothing: wait for the wakeup pipe.
    bool idle = false;
    bool stream_finished = false;
};

}
