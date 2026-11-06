#include <Storages/Kafka/Clients/PartitionProducer.h>

#include <Storages/Kafka/Clients/Cluster.h>
#include <Storages/Kafka/Clients/Protocol/BrokerError.h>
#include <Storages/Kafka/Clients/Protocol/Messages/Produce.h>
#include <Storages/Kafka/Clients/Protocol/Records.h>

#include <deque>

namespace Kafka
{

using Net::Deadline;

struct PartitionProducer::Impl
{
    Impl(Cluster & cluster_, TopicPartition tp_, Options options_)
        : cluster(cluster_)
        , tp(std::move(tp_))
        , options(options_)
    {
        if (options.acks != -1 && options.acks != 0 && options.acks != 1)
            throw Exception(ErrorKind::Config, "Acks must be -1, 0 or 1");
        if (options.max_in_flight == 0)
            throw Exception(ErrorKind::Config, "Option max_in_flight must be at least 1");
    }

    struct PendingBatch
    {
        BrokerConnection::PendingResponse response;
        size_t record_count = 0;
    };

    Cluster & cluster;
    TopicPartition tp;
    Options options;
    std::optional<Cluster::Lease> lease;
    std::deque<PendingBatch> pending;
    std::vector<Ack> acks;

    void ensureLease(const Deadline & deadline)
    {
        if (lease)
            return;
        const auto leader = cluster.leaderFor(tp, deadline);
        lease.emplace(cluster.acquire(leader.node_id, leader.address, deadline));
    }

    BrokerConnection & connection()
    {
        if (!lease)
            throw Exception(ErrorKind::Connection, "Producer has no connection");
        return **lease;
    }

    /// Connection gone: every outstanding batch is lost.
    [[noreturn]] void failAll(const Exception & cause)
    {
        const size_t lost = pending.size();
        pending.clear();
        lease.reset();
        cluster.invalidateTopic(tp.topic);
        throw Exception(
            cause.getKind(),
            PreformattedMessage::create(
                "Produce pipeline to {}[{}] failed with {} batch(es) outstanding: {}", tp.topic, tp.partition, lost, cause.message()),
            cause.getBrokerCode());
    }

    /// Receive the oldest ack. Resumable timeouts (no bytes consumed yet)
    /// propagate without losing the pipeline; everything else fails it.
    void receiveOne(const Deadline & deadline)
    {
        Protocol::ProduceResponse response;
        try
        {
            response = connection().receiveResponse<Protocol::ProduceRequest>(pending.front().response, deadline);
        }
        catch (const Exception & e)
        {
            if (!connection().isPoisoned())
                throw; // resumable: connection and window intact
            failAll(e);
        }
        const PendingBatch batch = pending.front();
        pending.pop_front();

        const Protocol::ProducePartitionResponse * p = nullptr;
        for (const auto & topic : response.responses)
            if (topic.name == tp.topic)
                for (const auto & part : topic.partition_responses)
                    if (part.index == tp.partition)
                        p = &part;
        if (!p)
            Protocol::throwProtocol("Produce response misses the requested partition");
        if (p->error_code != BrokerError::NONE)
        {
            /// The connection stays clean (the response was fully consumed);
            /// later batches remain drainable by subsequent calls.
            throw Exception(BrokerCode{p->error_code}, "Produce to {}[{}] failed for a batch of {} records{}",
                tp.topic,
                tp.partition,
                batch.record_count,
                p->error_message ? ": " + p->error_message.value() : String{});
        }
        acks.push_back({p->base_offset, p->log_append_time_ms, batch.record_count});
    }

    void drainAvailable(const Deadline & deadline)
    {
        while (!pending.empty() && connection().responseAvailable())
            receiveOne(deadline);
    }

    /// Split records into wire batches under max_batch_bytes.
    std::vector<std::pair<String, size_t>> buildBlobs(const std::vector<Record> & records) const
    {
        const Int64 now_ms
            = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

        std::vector<std::pair<String, size_t>> blobs;
        std::vector<Protocol::BatchRecord> batch_records;
        std::vector<Int64> timestamps;
        size_t batch_bytes = Protocol::record_batch_header_size;

        const auto finalize = [&]
        {
            if (batch_records.empty())
                return;
            const Int64 first_timestamp = timestamps.front();
            Int64 max_timestamp = first_timestamp;
            for (size_t i = 0; i < batch_records.size(); ++i)
            {
                batch_records[i].timestamp_delta = timestamps[i] - first_timestamp;
                max_timestamp = std::max(max_timestamp, timestamps[i]);
            }
            const size_t count = batch_records.size();
            auto batch = Protocol::RecordBatch::make(0, std::move(batch_records));
            batch.first_timestamp = first_timestamp;
            batch.max_timestamp = max_timestamp;
            blobs.emplace_back(
                Protocol::encodeBatches(
                    {batch}, static_cast<Protocol::CompressionType>(options.compression), options.compression_level),
                count);
            batch_records = {};
            timestamps.clear();
            batch_bytes = Protocol::record_batch_header_size;
        };

        for (const auto & record : records)
        {
            Protocol::BatchRecord wire;
            wire.offset_delta = static_cast<Int32>(batch_records.size());
            wire.key = record.key;
            wire.value = record.value;
            wire.headers = record.headers;
            /// Size estimate uses delta 0; the real deltas assigned later can
            /// only change the varint width by a few bytes — covered by the
            /// margin between max_batch_bytes and the broker limit.
            const size_t wire_size = Protocol::recordWireSize(wire);
            if (!batch_records.empty() && batch_bytes + wire_size > options.max_batch_bytes)
            {
                finalize();
                wire.offset_delta = 0;
            }
            batch_bytes += wire_size;
            batch_records.push_back(std::move(wire));
            timestamps.push_back(record.timestamp_ms >= 0 ? record.timestamp_ms : now_ms);
        }
        finalize();
        return blobs;
    }

    void send(const std::vector<Record> & records, const Deadline & deadline)
    {
        if (records.empty())
            return;
        const auto blobs = buildBlobs(records);
        ensureLease(deadline);
        for (const auto & [blob, record_count] : blobs)
        {
            /// Collect whatever acks already arrived, then respect the window.
            drainAvailable(deadline);
            while (pending.size() >= options.max_in_flight)
                receiveOne(deadline);

            Protocol::ProduceRequest request;
            request.acks = options.acks;
            request.timeout_ms
                = static_cast<Int32>(std::max<Int64>(1'000, deadline.remainingClamped(std::chrono::milliseconds{30'000}).count()));
            Protocol::ProducePartitionData partition;
            partition.index = tp.partition;
            partition.records = blob;
            request.topic_data = {{tp.topic, {partition}}};

            try
            {
                if (options.acks == 0)
                    connection().callNoResponse(request, deadline);
                else
                    pending.push_back({connection().sendRequest(request, deadline), record_count});
            }
            catch (const Exception & e)
            {
                if (lease && !connection().isPoisoned() && e.getKind() != ErrorKind::Connection)
                    throw;
                failAll(e);
            }
        }
    }

    std::vector<Ack> flush(const Deadline & deadline)
    {
        if (lease)
            while (!pending.empty())
                receiveOne(deadline);
        return std::exchange(acks, {});
    }

    std::vector<Ack> poll()
    {
        if (lease)
            drainAvailable(Deadline::after(cluster.getConfig().request_timeout));
        return std::exchange(acks, {});
    }
};

PartitionProducer::PartitionProducer(Client & client, TopicPartition tp) : PartitionProducer(client, std::move(tp), Options{})
{
}

PartitionProducer::PartitionProducer(Client & client, TopicPartition tp, Options options)
    : impl(std::make_unique<Impl>(client.getCluster(), std::move(tp), options))
{
}

PartitionProducer::PartitionProducer(Cluster & cluster, TopicPartition tp, Options options)
    : impl(std::make_unique<Impl>(cluster, std::move(tp), options))
{
}

PartitionProducer::~PartitionProducer() = default;

void PartitionProducer::send(const std::vector<Record> & records, std::chrono::milliseconds timeout)
{
    impl->send(records, Deadline::after(timeout));
}

std::vector<PartitionProducer::Ack> PartitionProducer::poll()
{
    return impl->poll();
}

std::vector<PartitionProducer::Ack> PartitionProducer::flush(std::chrono::milliseconds timeout)
{
    return impl->flush(Deadline::after(timeout));
}

size_t PartitionProducer::inFlight() const
{
    return impl->pending.size();
}

}
