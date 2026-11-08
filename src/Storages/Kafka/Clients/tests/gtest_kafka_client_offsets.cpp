#include <gtest/gtest.h>

#include <Storages/Kafka/Clients/Ops.h>
#include <Storages/Kafka/Clients/Protocol/BrokerError.h>
#include <Storages/Kafka/Clients/Protocol/Messages/ApiVersions.h>
#include <Storages/Kafka/Clients/Protocol/Messages/FindCoordinator.h>
#include <Storages/Kafka/Clients/Protocol/Messages/OffsetCommit.h>
#include <Storages/Kafka/Clients/Protocol/Messages/OffsetFetch.h>
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
        {toWire(ApiKey::FindCoordinator), 0, 6},
        {toWire(ApiKey::OffsetCommit), 2, 9},
        {toWire(ApiKey::OffsetFetch), 1, 9},
    };
    return frameResponse(1, false, [&](Writer & w) { response.encode(w, 3); });
}

String findCoordinatorFrame(Int32 correlation_id, const String & group, Int32 node_id, const String & host, Int16 error = 0)
{
    FindCoordinatorResponse response;
    response.coordinators = {{group, node_id, host, 7000 + node_id, error, std::nullopt}};
    return frameResponse(correlation_id, true, [&](Writer & w) { response.encode(w, 4); });
}

String commitFrame(Int32 correlation_id, Int16 partition_error)
{
    OffsetCommitResponse response;
    response.topics = {{"t", {{0, partition_error}}}};
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

}

TEST(KafkaClientOffsets, CommitResolvesTheCoordinatorAndSendsAGroupLessRequest)
{
    Rig rig({
        /// bootstrap control: handshake + FindCoordinator -> node 7
        {apiVersionsFrame(), findCoordinatorFrame(2, "g", 7, "coord-host")},
        /// coordinator: handshake + commit ok
        {apiVersionsFrame(), commitFrame(2, 0)},
    });

    TopicPartitionOffset offset;
    offset.topic = "t";
    offset.partition = 0;
    offset.offset = 42;
    offset.metadata = "meta";
    Ops::commitOffsets(rig.cluster, "g", {offset}, forever);

    EXPECT_EQ(rig.transports[1]->connected_host, "coord-host");
    EXPECT_EQ(rig.transports[1]->connected_port, 7007);

    const auto frames = rig.transports[1]->writtenFrames();
    ASSERT_EQ(frames.size(), 2);
    Reader r(frames[1]);
    const auto header = RequestHeader::decode(r, true);
    EXPECT_EQ(header.api_version, 9); // negotiated within [8,9]
    const auto request = OffsetCommitRequest::decode(r, 9);
    EXPECT_EQ(request.group_id, "g");
    EXPECT_EQ(request.generation_id_or_member_epoch, -1);
    EXPECT_TRUE(request.member_id.empty());
    EXPECT_EQ(request.group_instance_id, std::nullopt);
    ASSERT_EQ(request.topics.size(), 1);
    EXPECT_EQ(request.topics[0].partitions[0].committed_offset, 42);
    EXPECT_EQ(request.topics[0].partitions[0].committed_metadata, std::optional<String>{"meta"});
}

TEST(KafkaClientOffsets, CommitReResolvesTheCoordinatorOnNotCoordinator)
{
    Rig rig({
        /// bootstrap control: handshake + two FindCoordinator responses
        {apiVersionsFrame(), findCoordinatorFrame(2, "g", 7, "coord-a"), findCoordinatorFrame(3, "g", 8, "coord-b")},
        /// first coordinator: NOT_COORDINATOR
        {apiVersionsFrame(), commitFrame(2, BrokerError::NOT_COORDINATOR)},
        /// second coordinator: success
        {apiVersionsFrame(), commitFrame(2, 0)},
    });

    TopicPartitionOffset offset;
    offset.topic = "t";
    offset.partition = 0;
    offset.offset = 1;
    Ops::commitOffsets(rig.cluster, "g", {offset}, Net::Deadline::after(std::chrono::seconds{10}));
    EXPECT_EQ(rig.transports[2]->connected_host, "coord-b");
}

TEST(KafkaClientOffsets, CoordinatorLookupErrorsRetryFreshGroupBootstrapping)
{
    Rig rig({
        {apiVersionsFrame(),
         findCoordinatorFrame(2, "g", -1, "", BrokerError::COORDINATOR_NOT_AVAILABLE),
         findCoordinatorFrame(3, "g", 7, "coord-a")},
        {apiVersionsFrame(), commitFrame(2, 0)},
    });

    TopicPartitionOffset offset;
    offset.topic = "t";
    offset.partition = 0;
    offset.offset = 5;
    Ops::commitOffsets(rig.cluster, "g", {offset}, Net::Deadline::after(std::chrono::seconds{10}));
    EXPECT_EQ(rig.transports[1]->connected_host, "coord-a");
}

TEST(KafkaClientOffsets, FetchCommittedOffsetsMapsResponsesBackToTheRequestOrder)
{
    OffsetFetchResponse response;
    OffsetFetchGroupResponse group;
    group.group_id = "g";
    group.topics = {{"t", {{1, -1, -1, std::nullopt, 0}, {0, 42, 3, std::optional<String>{"m"}, 0}}}};
    response.groups = {group};

    Rig rig({
        {apiVersionsFrame(), findCoordinatorFrame(2, "g", 7, "coord-a")},
        {apiVersionsFrame(), frameResponse(2, true, [&](Writer & w) { response.encode(w, 8); })},
    });

    const auto offsets = Ops::fetchCommittedOffsets(rig.cluster, "g", {{"t", 0}, {"t", 1}}, forever);
    ASSERT_EQ(offsets.size(), 2);
    EXPECT_EQ(offsets[0].partition, 0);
    EXPECT_EQ(offsets[0].offset, 42);
    EXPECT_EQ(offsets[0].leader_epoch, 3);
    EXPECT_EQ(offsets[0].metadata, std::optional<String>{"m"});
    EXPECT_EQ(offsets[1].partition, 1);
    EXPECT_EQ(offsets[1].offset, -1); // nothing committed
}

TEST(KafkaClientOffsets, OffsetsWorkAgainstPre30Brokers)
{
    /// Pre-3.0 ranges: FindCoordinator v3, OffsetFetch v6.
    ApiVersionsResponse old_broker;
    old_broker.api_keys = {
        {toWire(ApiKey::ApiVersions), 0, 3},
        {toWire(ApiKey::FindCoordinator), 0, 3},
        {toWire(ApiKey::OffsetCommit), 2, 8},
        {toWire(ApiKey::OffsetFetch), 1, 6},
    };
    const auto oldApiVersionsFrame = [&] { return frameResponse(1, false, [&](Writer & w) { old_broker.encode(w, 3); }); };

    /// FindCoordinator v3 response: flat, no key echo.
    FindCoordinatorResponse coordinator_response;
    coordinator_response.coordinators = {{"", 7, "coord-old", 7007, 0, std::nullopt}};

    OffsetFetchResponse fetch_response;
    OffsetFetchGroupResponse group;
    group.group_id = ""; // not echoed below v8
    group.topics = {{"t", {{0, 42, -1, std::nullopt, 0}}}};
    fetch_response.groups = {group};

    Rig rig({
        {oldApiVersionsFrame(), frameResponse(2, true, [&](Writer & w) { coordinator_response.encode(w, 3); })},
        {oldApiVersionsFrame(),
         commitFrame(2, 0),
         frameResponse(3, true, [&](Writer & w) { fetch_response.encode(w, 6); })},
    });

    TopicPartitionOffset offset;
    offset.topic = "t";
    offset.partition = 0;
    offset.offset = 42;
    Ops::commitOffsets(rig.cluster, "g", {offset}, forever);
    EXPECT_EQ(rig.transports[1]->connected_host, "coord-old");

    const auto committed = Ops::fetchCommittedOffsets(rig.cluster, "g", {{"t", 0}}, forever);
    ASSERT_EQ(committed.size(), 1);
    EXPECT_EQ(committed[0].offset, 42);

    /// Wire checks: FindCoordinator v3 single key, OffsetCommit clamped to v8,
    /// OffsetFetch v6 single-group form.
    {
        const auto frames = rig.transports[0]->writtenFrames();
        Reader r(frames[1]);
        const auto header = RequestHeader::decode(r, true);
        EXPECT_EQ(header.api_version, 3);
        const auto request = FindCoordinatorRequest::decode(r, 3);
        EXPECT_EQ(request.coordinator_keys, std::vector<String>{"g"});
    }
    {
        const auto frames = rig.transports[1]->writtenFrames();
        Reader commit_reader(frames[1]);
        EXPECT_EQ(RequestHeader::decode(commit_reader, true).api_version, 8);
        Reader fetch_reader(frames[2]);
        EXPECT_EQ(RequestHeader::decode(fetch_reader, true).api_version, 6);
        const auto request = OffsetFetchRequest::decode(fetch_reader, 6);
        ASSERT_EQ(request.groups.size(), 1);
        EXPECT_EQ(request.groups[0].group_id, "g");
    }
}

TEST(KafkaClientOffsets, CoordinatorConnectionIsCachedAcrossOperations)
{
    Rig rig({
        {apiVersionsFrame(), findCoordinatorFrame(2, "g", 7, "coord-a")},
        {apiVersionsFrame(), commitFrame(2, 0), commitFrame(3, 0)},
    });

    TopicPartitionOffset offset;
    offset.topic = "t";
    offset.partition = 0;
    offset.offset = 1;
    Ops::commitOffsets(rig.cluster, "g", {offset}, forever);
    offset.offset = 2;
    Ops::commitOffsets(rig.cluster, "g", {offset}, forever);
    /// One FindCoordinator, one coordinator connection — nothing left over.
    EXPECT_TRUE(rig.pending->empty());
}
