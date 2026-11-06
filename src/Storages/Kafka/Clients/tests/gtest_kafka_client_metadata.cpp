#include <Storages/Kafka/Clients/Protocol/RequestHeader.h>
#include <Storages/Kafka/Clients/Protocol/Messages/Metadata.h>

#include <gtest/gtest.h>

#include <base/types.h>

#include <cstdint>

using namespace Kafka::Protocol;
using Kafka::Uuid;

namespace
{

Uuid uuidFromLowByte(UInt8 low)
{
    Uuid id;
    id.data[15] = low;
    return id;
}

}

TEST(KafkaClientMetadata, MetadataRequestRoundTripsAtEverySupportedVersion)
{
    for (Int16 version : {Int16{9}, Int16{10}, Int16{11}, Int16{12}})
    {
        MetadataRequest req;
        req.topics = std::vector<MetadataRequestTopic>{{Uuid{}, std::optional<String>{"topic-a"}}};
        req.allow_auto_topic_creation = true;
        req.include_topic_authorized_operations = false;

        Writer w;
        req.encode(w, version);
        Reader r(w.view());
        const auto got = MetadataRequest::decode(r, version);
        EXPECT_EQ(got, req);
        EXPECT_TRUE(r.empty());
    }
}

TEST(KafkaClientMetadata, MetadataRequestWithNullTopicsMeansAllTopics)
{
    MetadataRequest req;
    req.topics = std::nullopt;

    Writer w;
    req.encode(w, 12);
    Reader r(w.view());
    const auto got = MetadataRequest::decode(r, 12);
    EXPECT_EQ(got.topics, std::nullopt);
    EXPECT_TRUE(r.empty());
}

TEST(KafkaClientMetadata, MetadataResponseRoundTripsAtV9ThroughV12)
{
    /// The original Catch2 test used GENERATE; run the whole body per version.
    for (Int16 version : {Int16{9}, Int16{10}, Int16{11}, Int16{12}})
    {
        MetadataResponse resp;
        resp.throttle_time_ms = 0;
        resp.brokers = {{1, "broker-1", 9092, std::optional<String>{"rack-a"}}};
        resp.cluster_id = "cluster-id";
        resp.controller_id = 1;

        MetadataPartition partition;
        partition.error_code = 0;
        partition.partition_index = 0;
        partition.leader_id = 1;
        partition.leader_epoch = 5;
        partition.replica_nodes = {1};
        partition.isr_nodes = {1};
        partition.offline_replicas = {};

        MetadataTopic topic;
        topic.error_code = 0;
        topic.name = "topic-a";
        topic.topic_id = uuidFromLowByte(1);
        topic.is_internal = false;
        topic.partitions = {partition};
        topic.topic_authorized_operations = INT32_MIN;
        resp.topics = {topic};

        const String frame = buildFrame([&](Writer & w)
        {
            ResponseHeader{11}.encode(w, MetadataRequest::responseHeaderFlexible(version));
            resp.encode(w, version);
        });

        /// Below v10 the topic UUID is not on the wire, so it decodes as zero.
        MetadataResponse expected = resp;
        if (version < 10)
            expected.topics[0].topic_id = Uuid{};

        Reader r(frame);
        const auto size = r.readInt32();
        EXPECT_EQ(static_cast<size_t>(size), r.remaining());
        const auto header = ResponseHeader::decode(r, true);
        EXPECT_EQ(header.correlation_id, 11);
        const auto got = MetadataResponse::decode(r, version);
        EXPECT_EQ(got, expected);
        EXPECT_TRUE(r.empty());
    }
}

TEST(KafkaClientMetadata, MetadataCodecRejectsUnsupportedVersions)
{
    MetadataRequest req;
    Writer w;
    EXPECT_THROW(req.encode(w, 8), Kafka::Exception);
    EXPECT_THROW(req.encode(w, 13), Kafka::Exception);
}
