#include <gtest/gtest.h>

#include <Storages/Kafka/Clients/Cluster.h>
#include <Storages/Kafka/Clients/Partitioner.h>
#include <Storages/Kafka/Clients/Protocol/Messages/ApiVersions.h>
#include <Storages/Kafka/Clients/Protocol/Messages/Metadata.h>
#include <Storages/Kafka/Clients/Protocol/Messages/Produce.h>
#include <Storages/Kafka/Clients/Protocol/Records.h>
#include <Storages/Kafka/Clients/Protocol/RequestHeader.h>
#include <Storages/Kafka/Clients/TopicProducer.h>

#include "ScriptedTransport.h"

#include <algorithm>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <optional>

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

/// Metadata for topic "t" with `partition_count` partitions, all led by node 1.
String metadataFrame(Int32 correlation_id, Int32 partition_count)
{
    MetadataResponse response;
    response.brokers = {{1, "leader-host", 11111, std::nullopt}};
    response.cluster_id = "cid";
    response.controller_id = 1;
    MetadataTopic topic;
    topic.name = "t";
    for (Int32 i = 0; i < partition_count; ++i)
    {
        MetadataPartition partition;
        partition.partition_index = i;
        partition.leader_id = 1;
        partition.leader_epoch = 1;
        topic.partitions.push_back(partition);
    }
    response.topics = {topic};
    return frameResponse(correlation_id, true, [&](Writer & w) { response.encode(w, 12); });
}

String produceAckFrame(Int32 correlation_id, Int32 partition, Int64 base_offset)
{
    ProduceResponse response;
    ProducePartitionResponse presp;
    presp.index = partition;
    presp.base_offset = base_offset;
    response.responses = {{"t", {presp}}};
    return frameResponse(correlation_id, true, [&](Writer & w) { response.encode(w, 9); });
}

struct Rig
{
    std::shared_ptr<std::deque<std::unique_ptr<ScriptedTransport>>> pending;
    std::vector<ScriptedTransport *> transports;
    Cluster cluster;

    explicit Rig(std::vector<std::vector<String>> scripts)
        : pending(std::make_shared<std::deque<std::unique_ptr<ScriptedTransport>>>())
        , cluster(makeConfig(), [this] { return next(); })
    {
        for (auto & frames : scripts)
        {
            auto transport = std::make_unique<ScriptedTransport>();
            for (auto & frame : frames)
                transport->script(std::move(frame));
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

/// The single produce request a leader connection carried: its target
/// partition and the record values, in order.
struct SentProduce
{
    Int32 partition = 0;
    std::vector<String> values;
};

/// Decodes the produce request written on a leader transport; nullopt when
/// the transport never served one (never connected, or handshake only).
std::optional<SentProduce> decodeProduce(const ScriptedTransport & transport)
{
    const auto frames = transport.writtenFrames();
    if (frames.size() < 2)
        return std::nullopt;
    Reader r(frames[1]);
    RequestHeader::decode(r, true);
    const auto request = ProduceRequest::decode(r, 9);
    SentProduce sent;
    sent.partition = request.topic_data[0].partition_data[0].index;
    for (const auto & batch : decodeBatches(*request.topic_data[0].partition_data[0].records).batches)
        for (const auto & record : batch.records)
            sent.values.push_back(record.value.value_or(""));
    return sent;
}

bool contains(const std::vector<String> & values, const String & value)
{
    return std::find(values.begin(), values.end(), value) != values.end();
}

std::vector<Record> makeUnkeyedRecords(size_t n)
{
    std::vector<Record> records(n);
    for (size_t i = 0; i < n; ++i)
    {
        records[i].value = "u" + std::to_string(i);
        records[i].timestamp_ms = 1'000 + static_cast<Int64>(i);
    }
    return records;
}

}

TEST(KafkaClientTopicProducer, KeyedRecordsRouteByMurmur2)
{
    constexpr Int32 partition_count = 3;

    /// Two records per key plus one empty-string key (an empty string is a
    /// key, not "unkeyed"). The expected partitions come from the same public
    /// functions the producer uses; these keys cover all three partitions.
    const std::vector<String> keys = {"k1", "k2", "k3"};
    std::map<Int32, size_t> expected_counts;
    for (const auto & key : keys)
        expected_counts[partitionForKey(key, partition_count)] += 2;
    expected_counts[partitionForKey("", partition_count)] += 1;
    ASSERT_EQ(expected_counts.size(), 3u);

    /// Control connection (metadata), then one leader connection per target
    /// partition — opened in ascending partition order, each acking its
    /// single produce request with a partition-tagged base offset.
    std::vector<std::vector<String>> scripts;
    scripts.push_back({apiVersionsFrame(), metadataFrame(2, partition_count)});
    for (const auto & [partition, count] : expected_counts)
        scripts.push_back({apiVersionsFrame(), produceAckFrame(2, partition, 100 + partition)});
    Rig rig(std::move(scripts));

    TopicProducer producer(rig.cluster, "t", {});
    std::vector<Record> records;
    for (const auto & key : keys)
        for (int i = 0; i < 2; ++i)
        {
            Record record;
            record.key = key;
            record.value = key + "-v" + std::to_string(i);
            record.timestamp_ms = 1'000;
            records.push_back(record);
        }
    Record empty_key;
    empty_key.key = Bytes{};
    empty_key.value = "empty-key";
    empty_key.timestamp_ms = 1'000;
    records.push_back(empty_key);

    producer.send(records, short_timeout);
    EXPECT_EQ(producer.inFlight(), 3u);

    const auto acks = producer.flush(short_timeout);
    ASSERT_EQ(acks.size(), 3u);
    std::map<Int32, size_t> acked_counts;
    for (const auto & ack : acks)
    {
        acked_counts[ack.partition] += ack.record_count;
        EXPECT_EQ(ack.base_offset, 100 + ack.partition);
    }
    EXPECT_EQ(acked_counts, expected_counts);
    EXPECT_EQ(producer.inFlight(), 0u);
}

TEST(KafkaClientTopicProducer, UnkeyedRecordsStickToOnePartition)
{
    constexpr Int32 partition_count = 3;

    /// The sticky partition is random, so its ack cannot be scripted up
    /// front: pre-script only the handshakes, decode the produce request
    /// after send() to learn the chosen partition, then script its ack.
    std::vector<std::vector<String>> scripts;
    scripts.push_back({apiVersionsFrame(), metadataFrame(2, partition_count)});
    for (Int32 i = 0; i < partition_count; ++i)
        scripts.push_back({apiVersionsFrame()});
    Rig rig(std::move(scripts));

    TopicProducer producer(rig.cluster, "t", {});
    const auto records = makeUnkeyedRecords(6);
    producer.send(records, short_timeout);
    EXPECT_EQ(producer.inFlight(), 1u);

    /// Exactly one leader connection carries exactly one produce request
    /// with all six records, to one in-range partition.
    std::vector<SentProduce> sent;
    std::vector<ScriptedTransport *> used;
    for (size_t i = 1; i < rig.transports.size(); ++i)
        if (const auto produce = decodeProduce(*rig.transports[i]))
        {
            sent.push_back(*produce);
            used.push_back(rig.transports[i]);
        }
    ASSERT_EQ(sent.size(), 1u);
    EXPECT_GE(sent[0].partition, 0);
    EXPECT_LT(sent[0].partition, partition_count);
    EXPECT_EQ(sent[0].values.size(), records.size());
    EXPECT_EQ(used[0]->writtenFrames().size(), 2u); // ApiVersions + one produce

    used[0]->script(produceAckFrame(2, sent[0].partition, 42));
    const auto acks = producer.flush(short_timeout);
    ASSERT_EQ(acks.size(), 1u);
    EXPECT_EQ(acks[0].partition, sent[0].partition);
    EXPECT_EQ(acks[0].record_count, records.size());
    EXPECT_EQ(acks[0].base_offset, 42);
}

TEST(KafkaClientTopicProducer, MixedKeysAndUnkeyed)
{
    constexpr Int32 partition_count = 3;
    const String key_a = "k1";
    const String key_b = "k2";
    const Int32 partition_a = partitionForKey(key_a, partition_count);
    const Int32 partition_b = partitionForKey(key_b, partition_count);
    ASSERT_NE(partition_a, partition_b);

    std::vector<std::vector<String>> scripts;
    scripts.push_back({apiVersionsFrame(), metadataFrame(2, partition_count)});
    for (Int32 i = 0; i < partition_count; ++i)
        scripts.push_back({apiVersionsFrame()});
    Rig rig(std::move(scripts));

    TopicProducer producer(rig.cluster, "t", {});
    std::vector<Record> records;
    Record keyed_a;
    keyed_a.key = key_a;
    keyed_a.value = "va";
    keyed_a.timestamp_ms = 1'000;
    records.push_back(keyed_a);
    for (const auto & unkeyed : makeUnkeyedRecords(3))
        records.push_back(unkeyed);
    Record keyed_b;
    keyed_b.key = key_b;
    keyed_b.value = "vb";
    keyed_b.timestamp_ms = 1'000;
    records.push_back(keyed_b);

    producer.send(records, short_timeout);

    /// Partition -> record values seen in produce requests on the wire.
    std::map<Int32, std::vector<String>> wire;
    std::map<Int32, ScriptedTransport *> transport_by_partition;
    for (size_t i = 1; i < rig.transports.size(); ++i)
        if (const auto produce = decodeProduce(*rig.transports[i]))
        {
            wire[produce->partition] = produce->values;
            transport_by_partition[produce->partition] = rig.transports[i];
        }

    /// Keyed records land on their murmur2 partitions...
    ASSERT_TRUE(wire.contains(partition_a));
    ASSERT_TRUE(wire.contains(partition_b));
    EXPECT_TRUE(contains(wire[partition_a], "va"));
    EXPECT_TRUE(contains(wire[partition_b], "vb"));

    /// ...and the unkeyed ones all sit on exactly one partition (it may
    /// coincide with a keyed one — the sticky pick is random).
    Int32 unkeyed_partition = -1;
    size_t unkeyed_seen = 0;
    for (const auto & [partition, values] : wire)
        for (const auto & value : values)
            if (value.starts_with("u"))
            {
                if (unkeyed_partition == -1)
                    unkeyed_partition = partition;
                EXPECT_EQ(partition, unkeyed_partition);
                ++unkeyed_seen;
            }
    ASSERT_NE(unkeyed_partition, -1);
    EXPECT_EQ(unkeyed_seen, 3u);

    /// Ack every produce request and check the tagged flush results.
    for (const auto & [partition, transport] : transport_by_partition)
        transport->script(produceAckFrame(2, partition, 200 + partition));
    const auto acks = producer.flush(short_timeout);
    ASSERT_EQ(acks.size(), wire.size());
    size_t total_records = 0;
    for (const auto & ack : acks)
    {
        EXPECT_EQ(ack.base_offset, 200 + ack.partition);
        EXPECT_EQ(ack.record_count, wire[ack.partition].size());
        total_records += ack.record_count;
    }
    EXPECT_EQ(total_records, records.size());
}

TEST(KafkaClientTopicProducer, EmptyTopicThrows)
{
    Rig rig({
        {apiVersionsFrame(), metadataFrame(2, 0)},
    });

    try
    {
        TopicProducer producer(rig.cluster, "t", {});
        FAIL() << "expected a Config error for a topic with no partitions";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Config);
    }
}
