#include <gtest/gtest.h>

#include <Storages/Kafka/Clients/Cluster.h>
#include <Storages/Kafka/Clients/PartitionProducer.h>
#include <Storages/Kafka/Clients/Protocol/BrokerError.h>
#include <Storages/Kafka/Clients/Protocol/Messages/ApiVersions.h>
#include <Storages/Kafka/Clients/Protocol/Messages/Metadata.h>
#include <Storages/Kafka/Clients/Protocol/Messages/Produce.h>
#include <Storages/Kafka/Clients/Protocol/Records.h>
#include <Storages/Kafka/Clients/Protocol/RequestHeader.h>

#include "ScriptedTransport.h"

#include <deque>
#include <functional>
#include <memory>

using namespace Kafka;
using namespace Kafka::Protocol;
using Kafka::Tests::ScriptedTransport;

namespace
{

const auto short_timeout = std::chrono::milliseconds{200};

String frameResponse(Int32 correlation_id, bool flexible_header, const std::function<void(Writer &)> & body)
{
    return buildFrame([&](Writer & w)
    {
        ResponseHeader{correlation_id}.encode(w, flexible_header);
        body(w);
    });
}

String apiVersionsFrame()
{
    ApiVersionsResponse response;
    response.api_keys = {
        {toWire(ApiKey::ApiVersions), 0, 4},
        {toWire(ApiKey::Metadata), 0, 13},
        {toWire(ApiKey::Produce), 3, 12},
    };
    return frameResponse(1, false, [&](Writer & w) { response.encode(w, 3); });
}

String metadataFrame(Int32 correlation_id)
{
    MetadataResponse response;
    response.brokers = {{1, "leader-host", 11111, std::nullopt}};
    response.cluster_id = "cid";
    response.controller_id = 1;
    MetadataTopic topic;
    topic.name = "t";
    MetadataPartition partition;
    partition.partition_index = 0;
    partition.leader_id = 1;
    partition.leader_epoch = 1;
    topic.partitions = {partition};
    response.topics = {topic};
    return frameResponse(correlation_id, true, [&](Writer & w) { response.encode(w, 12); });
}

String produceAckFrame(Int32 correlation_id, Int64 base_offset, Int16 error = 0)
{
    ProduceResponse response;
    ProducePartitionResponse partition;
    partition.index = 0;
    partition.error_code = error;
    partition.base_offset = base_offset;
    response.responses = {{"t", {partition}}};
    return frameResponse(correlation_id, true, [&](Writer & w) { response.encode(w, 9); });
}

struct Rig
{
    std::shared_ptr<std::deque<std::unique_ptr<ScriptedTransport>>> pending;
    std::vector<ScriptedTransport *> transports;
    Cluster cluster;

    explicit Rig(std::vector<std::vector<String>> scripts, bool leader_readable = true)
        : pending(std::make_shared<std::deque<std::unique_ptr<ScriptedTransport>>>())
        , cluster(makeConfig(), [this] { return next(); })
    {
        for (size_t i = 0; i < scripts.size(); ++i)
        {
            auto transport = std::make_unique<ScriptedTransport>();
            for (auto & frame : scripts[i])
                transport->script(std::move(frame));
            if (i > 0 && !leader_readable)
                transport->setReadableProbe(false);
            transports.push_back(transport.get());
            pending->push_back(std::move(transport));
        }
    }

    static ClientConfig makeConfig()
    {
        ClientConfig config;
        config.bootstrap_brokers = {"bootstrap:9092"};
        return config;
    }

    std::unique_ptr<Net::Transport> next()
    {
        if (pending->empty())
            throw Exception(ErrorKind::Connection, "Test scripted no further connections");
        auto transport = std::move(pending->front());
        pending->pop_front();
        return transport;
    }
};

std::vector<Record> makeRecords(size_t n, const String & prefix = "v")
{
    std::vector<Record> records(n);
    for (size_t i = 0; i < n; ++i)
    {
        records[i].value = prefix + std::to_string(i);
        records[i].timestamp_ms = 1'000 + static_cast<Int64>(i);
    }
    return records;
}

}

TEST(KafkaClientProducer, ProducerPipelinesSendsWithoutWaitingForAcks)
{
    /// Leader transport reports not-readable: sends must NOT consume acks.
    Rig rig(
        {
            {apiVersionsFrame(), metadataFrame(2)},
            {apiVersionsFrame(), produceAckFrame(2, 0), produceAckFrame(3, 10), produceAckFrame(4, 20)},
        },
        /*leader_readable=*/false);

    PartitionProducer producer(rig.cluster, {"t", 0}, {.max_in_flight = 5});
    producer.send(makeRecords(10), short_timeout);
    producer.send(makeRecords(10), short_timeout);
    producer.send(makeRecords(10), short_timeout);
    EXPECT_EQ(producer.inFlight(), 3);

    /// Three produce frames are on the wire before any ack was read.
    const auto frames = rig.transports[1]->writtenFrames();
    ASSERT_EQ(frames.size(), 4); // ApiVersions + 3 produce
    for (size_t i = 1; i < frames.size(); ++i)
    {
        Reader r(frames[i]);
        const auto header = RequestHeader::decode(r, true);
        EXPECT_EQ(header.api_key, toWire(ApiKey::Produce));
        EXPECT_EQ(header.api_version, 9);
    }

    const auto acks = producer.flush(short_timeout);
    ASSERT_EQ(acks.size(), 3);
    EXPECT_EQ(acks[0].base_offset, 0);
    EXPECT_EQ(acks[1].base_offset, 10);
    EXPECT_EQ(acks[2].base_offset, 20);
    EXPECT_EQ(acks[2].record_count, 10);
    EXPECT_EQ(producer.inFlight(), 0);
}

TEST(KafkaClientProducer, FullWindowBlocksAndADataLessWaitIsAResumableTimeout)
{
    Rig rig(
        {
            {apiVersionsFrame(), metadataFrame(2)},
            {apiVersionsFrame(), produceAckFrame(2, 0)},
        },
        /*leader_readable=*/false);

    PartitionProducer producer(rig.cluster, {"t", 0}, {.max_in_flight = 1});
    producer.send(makeRecords(1), short_timeout);
    EXPECT_EQ(producer.inFlight(), 1);

    /// Window full; the single scripted ack unblocks the second send...
    producer.send(makeRecords(1), short_timeout);
    EXPECT_EQ(producer.inFlight(), 1);

    /// ...but no ack exists for it: flush times out WITHOUT killing the
    /// pipeline (no bytes of the next frame were consumed).
    EXPECT_THROW(producer.flush(short_timeout), Exception);
    EXPECT_EQ(producer.inFlight(), 1);

    /// poll() returns the first ack that was drained by the second send.
    const auto acks = producer.poll();
    ASSERT_EQ(acks.size(), 1);
    EXPECT_EQ(acks[0].base_offset, 0);
}

TEST(KafkaClientProducer, ABrokerErrorFailsItsBatchButKeepsThePipelineDrainable)
{
    Rig rig(
        {
            {apiVersionsFrame(), metadataFrame(2)},
            {apiVersionsFrame(),
             produceAckFrame(2, 0),
             produceAckFrame(3, -1, BrokerError::MESSAGE_TOO_LARGE),
             produceAckFrame(4, 20),
             produceAckFrame(5, 30)},
        },
        /*leader_readable=*/false);

    PartitionProducer producer(rig.cluster, {"t", 0}, {.max_in_flight = 5});
    producer.send(makeRecords(10), short_timeout);
    producer.send(makeRecords(10), short_timeout);
    producer.send(makeRecords(10), short_timeout);

    try
    {
        producer.flush(short_timeout);
        FAIL() << "expected broker error";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Broker);
        EXPECT_EQ(e.getBrokerCode(), BrokerError::MESSAGE_TOO_LARGE);
    }
    /// Batch 3 is still in flight; the producer stays usable.
    EXPECT_EQ(producer.inFlight(), 1);
    producer.send(makeRecords(10), short_timeout);
    const auto acks = producer.flush(short_timeout);
    ASSERT_EQ(acks.size(), 3); // batches 1, 3 and 4
    EXPECT_EQ(acks[0].base_offset, 0);
    EXPECT_EQ(acks[1].base_offset, 20);
    EXPECT_EQ(acks[2].base_offset, 30);
}

TEST(KafkaClientProducer, AConnectionLossFailsAllOutstandingBatchesAndReconnectsOnNextSend)
{
    Rig rig(
        {
            {apiVersionsFrame(), metadataFrame(2)},
            {apiVersionsFrame(), produceAckFrame(2, 0)}, // then EOF with 2 outstanding
            /// Reconnect: the metadata refresh opens this connection to the
            /// leader node first, the producer then reuses it from the pool.
            {apiVersionsFrame(), metadataFrame(2), produceAckFrame(3, 50)},
        },
        /*leader_readable=*/false);
    rig.transports[1]->scriptEof();

    PartitionProducer producer(rig.cluster, {"t", 0}, {.max_in_flight = 5});
    producer.send(makeRecords(5), short_timeout);
    producer.send(makeRecords(5), short_timeout);
    producer.send(makeRecords(5), short_timeout);

    try
    {
        producer.flush(short_timeout);
        FAIL() << "expected connection failure";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Connection);
        EXPECT_TRUE(String(e.message()).contains("2 batch(es) outstanding"));
    }
    EXPECT_EQ(producer.inFlight(), 0);
    /// The first ack survived.
    EXPECT_EQ(producer.poll().size(), 1);

    /// Next send reconnects (fresh metadata + connection) and succeeds.
    producer.send(makeRecords(5), short_timeout);
    const auto acks = producer.flush(short_timeout);
    ASSERT_EQ(acks.size(), 1);
    EXPECT_EQ(acks[0].base_offset, 50);
}

TEST(KafkaClientProducer, RecordsAreSplitIntoBatchesUnderMaxBatchBytes)
{
    Rig rig(
        {
            {apiVersionsFrame(), metadataFrame(2)},
            {apiVersionsFrame(),
             produceAckFrame(2, 0),
             produceAckFrame(3, 2),
             produceAckFrame(4, 4),
             produceAckFrame(5, 6)},
        },
        /*leader_readable=*/false);

    /// 100-byte values -> ~107 B wire per record; with the 61 B batch header a
    /// 300-byte budget fits exactly two records per batch.
    std::vector<Record> records(8);
    for (size_t i = 0; i < records.size(); ++i)
    {
        records[i].value = String(100, static_cast<char>('a' + i));
        records[i].timestamp_ms = 1'000 + static_cast<Int64>(i);
    }
    PartitionProducer producer(rig.cluster, {"t", 0}, {.max_in_flight = 8, .max_batch_bytes = 300});
    producer.send(records, short_timeout);
    EXPECT_EQ(producer.inFlight(), 4);

    const auto frames = rig.transports[1]->writtenFrames();
    ASSERT_EQ(frames.size(), 5);
    size_t total_records = 0;
    for (size_t i = 1; i < frames.size(); ++i)
    {
        Reader r(frames[i]);
        RequestHeader::decode(r, true);
        const auto request = ProduceRequest::decode(r, 9);
        const auto decoded = decodeBatches(*request.topic_data[0].partition_data[0].records);
        ASSERT_EQ(decoded.batches.size(), 1);
        EXPECT_EQ(decoded.batches[0].base_offset, 0);
        total_records += decoded.batches[0].records.size();
        /// Timestamps survive the split as absolute values.
        EXPECT_GE(decoded.batches[0].first_timestamp, 1'000);
    }
    EXPECT_EQ(total_records, 8);

    const auto acks = producer.flush(short_timeout);
    ASSERT_EQ(acks.size(), 4);
    size_t acked_records = 0;
    for (const auto & ack : acks)
        acked_records += ack.record_count;
    EXPECT_EQ(acked_records, 8);
}

TEST(KafkaClientProducer, AcksZeroTracksNothing)
{
    Rig rig(
        {
            {apiVersionsFrame(), metadataFrame(2)},
            {apiVersionsFrame()}, // no responses will ever come
        },
        /*leader_readable=*/false);

    PartitionProducer producer(rig.cluster, {"t", 0}, {.acks = 0});
    producer.send(makeRecords(10), short_timeout);
    EXPECT_EQ(producer.inFlight(), 0);
    EXPECT_TRUE(producer.flush(short_timeout).empty());
    EXPECT_EQ(rig.transports[1]->writtenFrames().size(), 2);
}

TEST(KafkaClientProducer, ProducerOptionsAreValidated)
{
    Rig rig({});
    EXPECT_THROW(PartitionProducer(rig.cluster, {"t", 0}, {.acks = 2}), Exception);
    EXPECT_THROW(PartitionProducer(rig.cluster, {"t", 0}, {.max_in_flight = 0}), Exception);
}
