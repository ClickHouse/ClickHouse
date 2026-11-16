#pragma once

#include <Core/Block.h>
#include <Core/StreamingHandleErrorMode.h>
#include <Core/Streaming/Settings.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Processors/ISource.h>
#include <Storages/StorageSnapshot.h>
#include <Common/Logger.h>

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
/// A partition absent in the cursor is read from the beginning.
/// The cursor of the read can be reconstructed from the `_topic`, `_partition` and `_offset` virtual columns.
///
/// A bounded stream (`STREAM BOUNDED`) finishes once it reaches the end of every partition (at the start of the read).
/// For an incremental refresh of a materialized view, it reports the cursor it advanced to through `Context::getStreamingCursor`.
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
    bool allPartitionsReachedEnd() const;
    void reportFinalCursor() const;

    Params params;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    const StreamSettings stream_settings;
    LoggerPtr log;

    const Block non_virtual_header;
    const Block virtual_header;

    std::unique_ptr<cppkafka::Consumer> consumer;
    std::map<TopicPartition, PartitionState> partitions;
    bool stream_finished = false;
};

}
