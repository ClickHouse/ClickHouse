#pragma once

#include <Storages/Kafka/Clients/Client.h>

#include <chrono>
#include <memory>
#include <vector>

namespace Kafka
{

/// Pipelined ("async") producer for one topic-partition — zero threads, like
/// everything else here: progress happens only inside send()/poll()/flush()
/// on the caller's thread; between calls, pending acks sit in the kernel
/// socket buffer.
///
/// send() encodes the records into wire batches (auto-split under the
/// broker's batch limit), writes them on a held connection and returns
/// WITHOUT waiting for acks, keeping at most max_in_flight produce requests
/// outstanding (it blocks on the oldest ack when the window is full).
/// flush() drains every outstanding ack.
///
/// Semantics:
///  * Broker errors surface late — at the send()/poll()/flush() call that
///    drains the failing ack. After such a throw the producer stays usable;
///    the remaining in-flight batches are still drainable (call flush again).
///  * A connection failure fails ALL outstanding batches (the throw says how
///    many); the next send() reconnects. There is no internal re-send (no
///    idempotence in v1) — a caller that retries may produce duplicates.
///  * A flush()/poll() timeout that fires while waiting for the first byte of
///    an ack is non-destructive: the connection and the window stay intact.
///  * Not thread-safe: one instance per producing thread.
class PartitionProducer
{
public:
    struct Options
    {
        /// -1 = all ISR, 1 = leader only, 0 = fire-and-forget (no acks, no
        /// tracking; poll/flush return nothing).
        Int16 acks = -1;
        /// Produce requests kept in flight before send() blocks on an ack.
        size_t max_in_flight = 5;
        /// Wire-size threshold for splitting records into batches. Must stay
        /// under the broker's message.max.bytes (default 1 MiB). Applies to the
        /// uncompressed record data.
        size_t max_batch_bytes = 900UL * 1024;
        Compression compression = Compression::None;
        /// -1 = the codec's default level.
        int compression_level = -1;
    };

    /// One acknowledged produce request.
    struct Ack
    {
        Int64 base_offset = -1;
        Int64 log_append_time_ms = -1;
        size_t record_count = 0;
    };

    PartitionProducer(Client & client, TopicPartition tp); // default Options
    PartitionProducer(Client & client, TopicPartition tp, Options options);
    /// Internal/testing: bind directly to a cluster.
    PartitionProducer(Cluster & cluster, TopicPartition tp, Options options);
    ~PartitionProducer();
    PartitionProducer(const PartitionProducer &) = delete;
    PartitionProducer & operator=(const PartitionProducer &) = delete;

    /// Pipeline the records. Record offsets are assigned by the broker; set
    /// key/value/headers/timestamp_ms (-1 = now). Blocks only while the
    /// in-flight window is full.
    void send(const std::vector<Record> & records, std::chrono::milliseconds timeout);

    /// Collect acks without blocking for new ones: drains whatever responses
    /// already arrived, returns everything collected since the last call.
    std::vector<Ack> poll();

    /// Wait for every outstanding ack; returns all acks collected since the
    /// last poll()/flush(). Throws on the first failed batch (see above).
    std::vector<Ack> flush(std::chrono::milliseconds timeout);

    /// Produce requests currently awaiting an ack.
    size_t inFlight() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

}
