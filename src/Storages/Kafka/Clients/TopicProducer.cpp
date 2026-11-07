#include <Storages/Kafka/Clients/TopicProducer.h>

#include <Storages/Kafka/Clients/Cluster.h>
#include <Storages/Kafka/Clients/PartitionProducer.h>
#include <Storages/Kafka/Clients/Partitioner.h>

#include <map>
#include <utility>

namespace Kafka
{

using Net::Deadline;

namespace
{

/// Rough record payload size (key + value + headers) that drives the sticky
/// re-stick accounting; close enough to the wire size for a batching heuristic.
size_t approximateRecordSize(const Record & record)
{
    size_t size = 0;
    if (record.key)
        size += record.key->size();
    if (record.value)
        size += record.value->size();
    for (const auto & header : record.headers)
        size += header.key.size() + (header.value ? header.value->size() : 0);
    return size;
}

}

struct TopicProducer::Impl
{
    Cluster & cluster;
    String topic;
    Options options;
    Int32 partition_count = 0;
    /// Lazily created, one per partition that ever received a record.
    /// PartitionProducer is neither copyable nor movable; try_emplace
    /// constructs it in place inside the map node.
    std::map<Int32, PartitionProducer> producers;
    StickyPartitioner sticky;
    size_t unkeyed_bytes_since_stick = 0;
    /// Acks drained from the per-partition producers and not returned yet;
    /// kept here so a throwing poll()/flush() loses nothing — the next call
    /// returns them.
    std::vector<Ack> collected;

    Impl(Cluster & cluster_, String topic_, Options options_)
        : cluster(cluster_)
        , topic(std::move(topic_))
        , options(options_)
    {
        const auto metadata = cluster.topicMetadata(topic, Deadline::after(cluster.getConfig().request_timeout));
        if (metadata.partitions.empty())
            throw Exception(ErrorKind::Config, "Topic '{}' has no partitions", topic);
        partition_count = static_cast<Int32>(metadata.partitions.size());
    }

    PartitionProducer::Options partitionOptions() const
    {
        PartitionProducer::Options mapped;
        mapped.acks = options.acks;
        mapped.max_in_flight = options.max_in_flight;
        mapped.max_batch_bytes = options.max_batch_bytes;
        mapped.compression = options.compression;
        mapped.compression_level = options.compression_level;
        return mapped;
    }

    PartitionProducer & producerFor(Int32 partition)
    {
        const auto it = producers.find(partition);
        if (it != producers.end())
            return it->second;
        return producers.try_emplace(partition, cluster, TopicPartition{topic, partition}, partitionOptions()).first->second;
    }

    void collect(Int32 partition, const std::vector<PartitionProducer::Ack> & acks)
    {
        for (const auto & ack : acks)
            collected.push_back({partition, ack.base_offset, ack.log_append_time_ms, ack.record_count});
    }

    void send(const std::vector<Record> & records, std::chrono::milliseconds timeout)
    {
        if (records.empty())
            return;

        /// Re-stick only between send() calls, so one call's unkeyed records
        /// stay on one partition (they batch together); once the accumulated
        /// unkeyed data exceeds max_batch_bytes, the NEXT send picks a fresh
        /// sticky partition.
        if (unkeyed_bytes_since_stick > options.max_batch_bytes)
        {
            sticky.reset();
            unkeyed_bytes_since_stick = 0;
        }

        const auto deadline = Deadline::after(timeout);

        /// Group by target partition, preserving the record order per group.
        std::map<Int32, std::vector<Record>> groups;
        for (const auto & record : records)
        {
            Int32 partition;
            /// An empty string is a key; only a missing key is unkeyed.
            if (record.key)
            {
                partition = partitionForKey(*record.key, partition_count);
            }
            else
            {
                partition = sticky.partition(partition_count);
                unkeyed_bytes_since_stick += approximateRecordSize(record);
            }
            groups[partition].push_back(record);
        }

        for (const auto & [partition, group] : groups)
            producerFor(partition).send(group, deadline.remainingClamped(timeout));
    }

    std::vector<Ack> poll()
    {
        for (auto & [partition, producer] : producers)
            collect(partition, producer.poll());
        return std::exchange(collected, {});
    }

    std::vector<Ack> flush(std::chrono::milliseconds timeout)
    {
        const auto deadline = Deadline::after(timeout);
        /// A throwing per-partition flush propagates; the acks collected so
        /// far stay in `collected` and the other partitions' batches stay
        /// drainable by a later poll()/flush().
        for (auto & [partition, producer] : producers)
            collect(partition, producer.flush(deadline.remainingClamped(timeout)));
        return std::exchange(collected, {});
    }

    size_t inFlight() const
    {
        size_t total = 0;
        for (const auto & [partition, producer] : producers)
            total += producer.inFlight();
        return total;
    }
};

TopicProducer::TopicProducer(Client & client, String topic) : TopicProducer(client, std::move(topic), Options{})
{
}

TopicProducer::TopicProducer(Client & client, String topic, Options options)
    : impl(std::make_unique<Impl>(client.getCluster(), std::move(topic), options))
{
}

TopicProducer::TopicProducer(Cluster & cluster, String topic, Options options)
    : impl(std::make_unique<Impl>(cluster, std::move(topic), options))
{
}

TopicProducer::~TopicProducer() = default;

void TopicProducer::send(const std::vector<Record> & records, std::chrono::milliseconds timeout)
{
    impl->send(records, timeout);
}

std::vector<TopicProducer::Ack> TopicProducer::poll()
{
    return impl->poll();
}

std::vector<TopicProducer::Ack> TopicProducer::flush(std::chrono::milliseconds timeout)
{
    return impl->flush(timeout);
}

size_t TopicProducer::inFlight() const
{
    return impl->inFlight();
}

}
