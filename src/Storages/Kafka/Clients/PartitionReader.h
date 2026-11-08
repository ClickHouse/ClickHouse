#pragma once

#include <Storages/Kafka/Clients/Client.h>

#include <chrono>
#include <vector>

namespace Kafka
{

/// Stateful convenience over Client::fetch for one topic-partition: tracks the
/// position, buffers the overflow when max_records caps a poll, advances past
/// control batches. Maps 1:1 onto the StorageKafka2 consumption pattern
/// (KafkaConsumer2::consume). Not thread-safe — use one instance per consuming
/// thread, like every ClickHouse consumer object.
class PartitionReader
{
public:
    PartitionReader(Client & client_, TopicPartition tp_, Int64 start_offset_);
    /// Resolves earliest/latest/by-timestamp into the start position first.
    PartitionReader(Client & client_, TopicPartition tp_, OffsetSpec start, std::chrono::milliseconds timeout);

    /// Records from the current position, at most max_records. Returns
    /// buffered records immediately; otherwise long-polls the broker for up to
    /// max_wait (plus a round-trip margin). An empty result means "nothing new
    /// within max_wait".
    std::vector<Record> poll(std::chrono::milliseconds max_wait, size_t max_records = SIZE_MAX);

    /// Next offset the caller has not seen yet.
    Int64 position() const { return pos; }
    void seek(Int64 offset);

    const TopicPartition & topicPartition() const { return tp; }
    /// High watermark observed by the most recent fetch, -1 before the first.
    Int64 highWatermark() const { return high_watermark; }
    /// Per-fetch partition byte budget (default 1 MiB).
    void setFetchMaxBytes(Int32 max_bytes) { fetch_max_bytes = max_bytes; }

private:
    void serve(std::vector<Record> & out, size_t max_records);

    Client & client;
    TopicPartition tp;
    Int64 pos = 0;
    Int64 high_watermark = -1;
    Int32 fetch_max_bytes = 1 << 20;
    /// Fetched-but-not-yet-served records plus where the last fetch ended
    /// (which can be past the last record: trailing control batches).
    std::vector<Record> buffer;
    size_t buffer_pos = 0;
    Int64 fetched_next_offset = -1;
};

}
