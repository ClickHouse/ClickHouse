#pragma once

#include <Storages/Kafka/Clients/Client.h>

#include <chrono>
#include <memory>
#include <vector>

namespace Kafka
{

/// Producer for one topic: routes records across partitions like the Java
/// client's default partitioner — keyed records by murmur2(key) % partitions
/// (stable across clients), unkeyed records to a sticky partition that
/// re-sticks after ~max_batch_bytes of data (KIP-480-style batching).
/// A thin router over per-partition PartitionProducers; same pipelining,
/// error and thread-safety semantics (not thread-safe, one per thread).
class TopicProducer
{
public:
    struct Options
    {
        Int16 acks = -1;
        size_t max_in_flight = 5; /// per partition
        size_t max_batch_bytes = 900UL * 1024;
        Compression compression = Compression::None;
        int compression_level = -1; /// -1 = the codec's default
    };

    /// One acknowledged produce request of one partition.
    struct Ack
    {
        Int32 partition = 0;
        Int64 base_offset = -1;
        Int64 log_append_time_ms = -1;
        size_t record_count = 0;
    };

    TopicProducer(Client & client, String topic); // default Options
    TopicProducer(Client & client, String topic, Options options);
    /// Internal/testing: bind directly to a cluster.
    TopicProducer(Cluster & cluster, String topic, Options options);
    ~TopicProducer();
    TopicProducer(const TopicProducer &) = delete;
    TopicProducer & operator=(const TopicProducer &) = delete;

    /// Route and pipeline the records (one call may fan out to several
    /// partitions). Blocks only when a target partition's window is full.
    void send(const std::vector<Record> & records, std::chrono::milliseconds timeout);

    /// Non-blocking drain of arrived acks across all partitions.
    std::vector<Ack> poll();

    /// Wait for every outstanding ack (all partitions). When one partition's
    /// flush throws, the exception propagates; the acks collected so far and
    /// the other partitions' outstanding batches stay drainable by a later
    /// poll()/flush().
    std::vector<Ack> flush(std::chrono::milliseconds timeout);

    size_t inFlight() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl;
};

}
