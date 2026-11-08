#include <gtest/gtest.h>

#include <Storages/Kafka/Clients/Ops.h>
#include <Storages/Kafka/Clients/Protocol/BrokerError.h>
#include <Storages/Kafka/Clients/Protocol/Messages/ApiVersions.h>
#include <Storages/Kafka/Clients/Protocol/Messages/Fetch.h>
#include <Storages/Kafka/Clients/Protocol/Messages/ListOffsets.h>
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

const auto forever = Net::Deadline::never();

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
        {toWire(ApiKey::Fetch), 4, 17},
        {toWire(ApiKey::ListOffsets), 0, 9},
        {toWire(ApiKey::Produce), 3, 12},
        {toWire(ApiKey::FindCoordinator), 0, 6},
        {toWire(ApiKey::OffsetCommit), 2, 9},
        {toWire(ApiKey::OffsetFetch), 1, 9},
    };
    return frameResponse(1, false, [&](Writer & w) { response.encode(w, 3); });
}

Uuid topicUuid()
{
    Uuid id;
    id.data[15] = 0xAB;
    return id;
}

MetadataResponse metadataResponse(Int32 leader_id, size_t partition_count = 1)
{
    MetadataResponse response;
    response.brokers = {{1, "host-one", 11111, std::nullopt}, {2, "host-two", 22222, std::nullopt}};
    response.cluster_id = "cid";
    response.controller_id = 1;
    MetadataTopic topic;
    topic.name = "t";
    topic.topic_id = topicUuid();
    for (size_t i = 0; i < partition_count; ++i)
    {
        MetadataPartition partition;
        partition.partition_index = static_cast<Int32>(i);
        partition.leader_id = leader_id;
        partition.leader_epoch = 5;
        partition.replica_nodes = {1, 2};
        partition.isr_nodes = {1, 2};
        topic.partitions.push_back(partition);
    }
    response.topics = {topic};
    return response;
}

String fetchFrame(Int32 correlation_id, Int16 partition_error, Int64 base_offset, const std::vector<String> & values)
{
    FetchResponse response;
    FetchPartitionResponse partition;
    partition.partition_index = 0;
    partition.error_code = partition_error;
    partition.high_watermark = base_offset + static_cast<Int64>(values.size());
    partition.log_start_offset = 0;
    /// `records` is a view — the blob must outlive the encode below.
    String blob;
    if (partition_error == 0 && !values.empty())
    {
        std::vector<BatchRecord> records;
        for (size_t i = 0; i < values.size(); ++i)
        {
            BatchRecord record;
            record.offset_delta = static_cast<Int32>(i);
            record.timestamp_delta = static_cast<Int64>(i);
            record.value = values[i];
            records.push_back(std::move(record));
        }
        auto batch = RecordBatch::make(base_offset, std::move(records));
        batch.first_timestamp = 1'000;
        batch.max_timestamp = 1'000 + static_cast<Int64>(values.size()) - 1;
        blob = encodeBatches({batch});
    }
    partition.records = blob;
    FetchTopicResponse topic;
    topic.topic_id = topicUuid();
    topic.partitions = {partition};
    response.responses = {topic};
    return frameResponse(correlation_id, true, [&](Writer & w) { response.encode(w, 13); });
}

/// Builds a Cluster whose connections consume pre-scripted transports in
/// creation order.
struct Rig
{
    std::shared_ptr<std::deque<std::unique_ptr<ScriptedTransport>>> pending;
    std::vector<ScriptedTransport *> transports;
    Cluster cluster;

    explicit Rig(std::vector<std::vector<String>> scripts, ClientConfig config = makeConfig())
        : pending(std::make_shared<std::deque<std::unique_ptr<ScriptedTransport>>>())
        , cluster(std::move(config), [this] { return next(); })
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

}

TEST(KafkaClientCluster, ParseBrokerAddress)
{
    const auto a = parseBrokerAddress("broker.example:19092");
    EXPECT_EQ(a.host, "broker.example");
    EXPECT_EQ(a.port, 19092);
    const auto b = parseBrokerAddress("plain-host");
    EXPECT_EQ(b.host, "plain-host");
    EXPECT_EQ(b.port, 9092);
    EXPECT_THROW(parseBrokerAddress("host:not-a-port"), Exception);
    EXPECT_THROW(parseBrokerAddress(":123"), Exception);
}

TEST(KafkaClientCluster, FetchRoutesToThePartitionLeaderAndFlattensRecords)
{
    Rig rig({
        /// bootstrap control connection: handshake + metadata
        {apiVersionsFrame(), frameResponse(2, true, [](Writer & w) { metadataResponse(1).encode(w, 12); })},
        /// leader node 1: handshake + fetch
        {apiVersionsFrame(), fetchFrame(2, 0, 5, {"a", "b"})},
    });

    FetchSpec spec;
    spec.topic = "t";
    spec.partition = 0;
    spec.offset = 5;
    const auto result = Ops::fetch(rig.cluster, spec, forever);

    EXPECT_EQ(rig.transports[0]->connected_host, "bootstrap");
    EXPECT_EQ(rig.transports[1]->connected_host, "host-one");
    EXPECT_EQ(rig.transports[1]->connected_port, 11111);

    ASSERT_EQ(result.records.size(), 2);
    EXPECT_EQ(result.records[0].offset, 5);
    EXPECT_EQ(result.records[0].value, Bytes{"a"});
    EXPECT_EQ(result.records[0].timestamp_ms, 1'000);
    EXPECT_EQ(result.records[1].offset, 6);
    EXPECT_EQ(result.records[1].value, Bytes{"b"});
    EXPECT_EQ(result.next_offset, 7);
    EXPECT_EQ(result.high_watermark, 7);
    EXPECT_FALSE(result.truncated_tail);

    /// The wire request carried the topic id, the leader epoch and the offset.
    const auto frames = rig.transports[1]->writtenFrames();
    ASSERT_EQ(frames.size(), 2);
    Reader r(frames[1]);
    RequestHeader::decode(r, true);
    const auto request = FetchRequest::decode(r, 13);
    ASSERT_EQ(request.topics.size(), 1);
    EXPECT_EQ(request.topics[0].topic_id, topicUuid());
    EXPECT_EQ(request.topics[0].partitions[0].fetch_offset, 5);
    EXPECT_EQ(request.topics[0].partitions[0].current_leader_epoch, 5);
}

TEST(KafkaClientCluster, FetchRetriesThroughALeaderChange)
{
    Rig rig({
        /// bootstrap: handshake + metadata (leader = node 1)
        {apiVersionsFrame(), frameResponse(2, true, [](Writer & w) { metadataResponse(1).encode(w, 12); })},
        /// node 1: handshake + NOT_LEADER error
        {apiVersionsFrame(), fetchFrame(2, BrokerError::NOT_LEADER_OR_FOLLOWER, 0, {})},
        /// node 2 (control rotation lands here): handshake + refreshed
        /// metadata (leader = node 2) + successful fetch on the pooled conn
        {apiVersionsFrame(),
         frameResponse(2, true, [](Writer & w) { metadataResponse(2).encode(w, 12); }),
         fetchFrame(3, 0, 0, {"x"})},
    });

    FetchSpec spec;
    spec.topic = "t";
    spec.partition = 0;
    spec.offset = 0;
    const auto result = Ops::fetch(rig.cluster, spec, Net::Deadline::after(std::chrono::seconds{10}));
    ASSERT_EQ(result.records.size(), 1);
    EXPECT_EQ(result.records[0].value, Bytes{"x"});
    EXPECT_EQ(rig.transports[2]->connected_host, "host-two");
}

TEST(KafkaClientCluster, MetadataIsCachedWithinMetadataMaxAge)
{
    Rig rig({
        {apiVersionsFrame(), frameResponse(2, true, [](Writer & w) { metadataResponse(1).encode(w, 12); })},
    });

    const auto first = Ops::getMetadata(rig.cluster, {"t"}, forever);
    ASSERT_EQ(first.topics.size(), 1);
    EXPECT_EQ(first.topics[0].partitions.size(), 1);
    EXPECT_EQ(first.brokers.size(), 2);
    EXPECT_EQ(first.cluster_id, std::optional<String>{"cid"});

    /// Second call is served from cache — any wire request would hit the
    /// "no further connections" / "no more data" guards.
    const auto second = Ops::getMetadata(rig.cluster, {"t"}, forever);
    EXPECT_EQ(second.topics.size(), 1);
}

TEST(KafkaClientCluster, EnsureTopicPollsUntilPartitionsAppear)
{
    Rig rig({
        /// First refresh goes through the bootstrap address and returns an
        /// auto-created topic with zero partitions.
        {apiVersionsFrame(), frameResponse(2, true, [](Writer & w) { metadataResponse(1, 0).encode(w, 12); })},
        /// The broker list is known now, so the second refresh prefers a real
        /// broker connection (round-robin lands on node 2).
        {apiVersionsFrame(), frameResponse(2, true, [](Writer & w) { metadataResponse(1, 2).encode(w, 12); })},
    });

    Ops::ensureTopic(rig.cluster, "t", Net::Deadline::after(std::chrono::seconds{5}));
    EXPECT_EQ(rig.transports[1]->connected_host, "host-two");

    /// Both metadata requests asked for auto-creation.
    for (const auto * transport : rig.transports)
    {
        const auto frames = transport->writtenFrames();
        ASSERT_EQ(frames.size(), 2);
        Reader r(frames[1]);
        RequestHeader::decode(r, true);
        EXPECT_TRUE(MetadataRequest::decode(r, 12).allow_auto_topic_creation);
    }
}

TEST(KafkaClientCluster, ListOffsetResolvesOnTheLeader)
{
    ListOffsetsResponse response;
    response.topics = {{"t", {{0, 0, -1, 42, 5}}}};
    Rig rig({
        {apiVersionsFrame(), frameResponse(2, true, [](Writer & w) { metadataResponse(1).encode(w, 12); })},
        {apiVersionsFrame(), frameResponse(2, true, [&](Writer & w) { response.encode(w, 7); })},
    });

    const auto offset = Ops::listOffset(rig.cluster, {"t", 0}, OffsetSpec::latest(), forever);
    EXPECT_EQ(offset, 42);

    const auto frames = rig.transports[1]->writtenFrames();
    Reader r(frames[1]);
    RequestHeader::decode(r, true);
    const auto request = ListOffsetsRequest::decode(r, 7);
    EXPECT_EQ(request.topics[0].partitions[0].timestamp, -1);
}

TEST(KafkaClientCluster, FetchRoutesByTopicNameAgainstPre31Brokers)
{
    /// Kafka 2.4-era ranges: Metadata max 9 (no topic UUIDs), Fetch max 11
    /// (non-flexible, name-routed), Produce max 8.
    ApiVersionsResponse old_broker;
    old_broker.api_keys = {
        {toWire(ApiKey::ApiVersions), 0, 3},
        {toWire(ApiKey::Metadata), 0, 9},
        {toWire(ApiKey::Fetch), 4, 11},
        {toWire(ApiKey::ListOffsets), 0, 5},
        {toWire(ApiKey::Produce), 3, 8},
    };
    const String old_api_versions = frameResponse(1, false, [&](Writer & w) { old_broker.encode(w, 3); });

    /// Metadata negotiates v9; the scripted response must match.
    auto md = metadataResponse(1);
    md.topics[0].topic_id = Uuid{}; // not on the wire below v10 anyway

    /// Fetch negotiates v11: name-routed, response header v0 (non-flexible).
    FetchResponse fetch_response;
    FetchPartitionResponse presp;
    presp.partition_index = 0;
    presp.error_code = 0;
    presp.high_watermark = 1;
    presp.log_start_offset = 0;
    BatchRecord record;
    record.value = Bytes{"old-broker"};
    const String blob = encodeBatches({RecordBatch::make(0, {record})}); // outlives the view
    presp.records = blob;
    FetchTopicResponse topic_response;
    topic_response.name = "t";
    topic_response.partitions = {presp};
    fetch_response.responses = {topic_response};

    Rig rig({
        {old_api_versions, frameResponse(2, true, [&](Writer & w) { md.encode(w, 9); })},
        {frameResponse(1, false, [&](Writer & w) { old_broker.encode(w, 3); }),
         frameResponse(2, false, [&](Writer & w) { fetch_response.encode(w, 11); })},
    });

    FetchSpec spec;
    spec.topic = "t";
    spec.partition = 0;
    spec.offset = 0;
    const auto result = Ops::fetch(rig.cluster, spec, forever);
    ASSERT_EQ(result.records.size(), 1);
    EXPECT_EQ(result.records[0].value, Bytes{"old-broker"});

    /// The wire request is a v11 non-flexible Fetch addressed by name.
    const auto frames = rig.transports[1]->writtenFrames();
    ASSERT_EQ(frames.size(), 2);
    Reader r(frames[1]);
    const auto header = RequestHeader::decode(r, false);
    EXPECT_EQ(header.api_version, 11);
    const auto request = FetchRequest::decode(r, 11);
    ASSERT_EQ(request.topics.size(), 1);
    EXPECT_EQ(request.topics[0].name, "t");
    EXPECT_TRUE(request.topics[0].topic_id.isZero());
    EXPECT_TRUE(r.empty());
}

TEST(KafkaClientCluster, ProduceEncodesOneBatchAndReturnsTheAssignedBaseOffset)
{
    ProduceResponse scripted;
    ProducePartitionResponse presp;
    presp.index = 0;
    presp.base_offset = 17;
    presp.log_append_time_ms = -1;
    scripted.responses = {{"t", {presp}}};

    Rig rig({
        {apiVersionsFrame(), frameResponse(2, true, [](Writer & w) { metadataResponse(1).encode(w, 12); })},
        {apiVersionsFrame(), frameResponse(2, true, [&](Writer & w) { scripted.encode(w, 9); })},
    });

    Client::ProduceSpec spec;
    spec.topic = "t";
    spec.partition = 0;
    Record first;
    first.key = Bytes{"k"};
    first.value = Bytes{"v0"};
    first.timestamp_ms = 1'000;
    Record second;
    second.value = Bytes{"v1"};
    second.timestamp_ms = 1'250;
    second.headers = {{"h", Bytes{"x"}}};
    spec.records = {first, second};

    const auto result = Ops::produce(rig.cluster, spec, forever);
    EXPECT_EQ(result.base_offset, 17);

    /// The wire request holds one well-formed uncompressed batch.
    const auto frames = rig.transports[1]->writtenFrames();
    ASSERT_EQ(frames.size(), 2);
    Reader r(frames[1]);
    RequestHeader::decode(r, true);
    const auto request = ProduceRequest::decode(r, 9);
    EXPECT_EQ(request.acks, -1);
    ASSERT_EQ(request.topic_data.size(), 1);
    const auto decoded = decodeBatches(*request.topic_data[0].partition_data[0].records);
    ASSERT_EQ(decoded.batches.size(), 1);
    const auto & batch = decoded.batches[0];
    EXPECT_EQ(batch.first_timestamp, 1'000);
    EXPECT_EQ(batch.max_timestamp, 1'250);
    ASSERT_EQ(batch.records.size(), 2);
    EXPECT_EQ(batch.records[0].key, Bytes{"k"});
    EXPECT_EQ(batch.records[1].timestamp_delta, 250);
    EXPECT_EQ(batch.records[1].headers.size(), 1);
}

TEST(KafkaClientCluster, ProduceWithAcksZeroWritesTheFrameAndNeverReads)
{
    Rig rig({
        {apiVersionsFrame(), frameResponse(2, true, [](Writer & w) { metadataResponse(1).encode(w, 12); })},
        /// Leader transport scripted with NO responses beyond the handshake:
        /// any read attempt would throw "no more data".
        {apiVersionsFrame()},
    });

    Client::ProduceSpec spec;
    spec.topic = "t";
    spec.partition = 0;
    spec.acks = 0;
    Record record;
    record.value = Bytes{"fire-and-forget"};
    spec.records = {record};

    const auto result = Ops::produce(rig.cluster, spec, forever);
    EXPECT_EQ(result.base_offset, -1);
    EXPECT_EQ(rig.transports[1]->writtenFrames().size(), 2);
}

TEST(KafkaClientCluster, ProduceValidatesItsInput)
{
    Rig rig({});
    Client::ProduceSpec spec;
    spec.topic = "t";
    EXPECT_THROW(Ops::produce(rig.cluster, spec, forever), Exception); // no records
    Record record;
    record.value = Bytes{"v"};
    spec.records = {record};
    spec.acks = 2;
    EXPECT_THROW(Ops::produce(rig.cluster, spec, forever), Exception); // bad acks
}

TEST(KafkaClientCluster, ConnectionsArePooledAndReusedPerBroker)
{
    Rig rig({
        {apiVersionsFrame(), frameResponse(2, true, [](Writer & w) { metadataResponse(1).encode(w, 12); })},
        {apiVersionsFrame(), fetchFrame(2, 0, 0, {"a"}), fetchFrame(3, 0, 1, {"b"})},
    });

    FetchSpec spec;
    spec.topic = "t";
    spec.partition = 0;
    spec.offset = 0;
    const auto first = Ops::fetch(rig.cluster, spec, forever);
    spec.offset = first.next_offset;
    const auto second = Ops::fetch(rig.cluster, spec, forever);
    EXPECT_EQ(second.records[0].offset, 1);
    /// Only two connections were ever created (bootstrap + leader).
    EXPECT_TRUE(rig.pending->empty());
}
