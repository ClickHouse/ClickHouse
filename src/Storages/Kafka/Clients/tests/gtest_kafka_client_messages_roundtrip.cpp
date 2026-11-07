/// End-to-end round-trip tests for every phase-2 API: build a value, frame it
/// the way the connection layer will (size prefix + header + body), parse it
/// back, assert equality. Mirrors the Rust port's tests/round_trip.rs.

#include <Storages/Kafka/Clients/Protocol/RequestHeader.h>
#include <Storages/Kafka/Clients/Protocol/Messages/Fetch.h>
#include <Storages/Kafka/Clients/Protocol/Messages/FindCoordinator.h>
#include <Storages/Kafka/Clients/Protocol/Messages/ListOffsets.h>
#include <Storages/Kafka/Clients/Protocol/Messages/OffsetCommit.h>
#include <Storages/Kafka/Clients/Protocol/Messages/OffsetFetch.h>
#include <Storages/Kafka/Clients/Protocol/Messages/Produce.h>
#include <Storages/Kafka/Clients/Protocol/Messages/Sasl.h>
#include <Storages/Kafka/Clients/Protocol/Records.h>

#include <gtest/gtest.h>

#include <base/types.h>

using namespace Kafka::Protocol;
using Kafka::Bytes;
using Kafka::Uuid;

namespace
{

Uuid uuidFromLowByte(UInt8 low)
{
    Uuid id;
    id.data[15] = low;
    return id;
}

/// Frame request -> parse back; returns the decoded body.
/// Value-returning, so gtest fatal assertions cannot be used here; the
/// original Catch2 REQUIREs become non-fatal EXPECTs.
template <typename Req>
Req requestRoundTrip(const Req & req, Int16 version)
{
    RequestHeader header;
    header.api_key = toWire(Req::api_key);
    header.api_version = version;
    header.correlation_id = 3;
    header.client_id = "c";
    header.flexible = Req::flexible(version);

    const String frame = buildFrame([&](Writer & w)
    {
        header.encode(w);
        req.encode(w, version);
    });

    Reader r(frame);
    const Int32 size = r.readInt32();
    EXPECT_EQ(static_cast<size_t>(size), r.remaining());
    const auto got_header = RequestHeader::decode(r, header.flexible);
    EXPECT_EQ(got_header.api_key, toWire(Req::api_key));
    const Req got = Req::decode(r, version);
    EXPECT_TRUE(r.empty());
    return got;
}

/// Variants for messages with zero-copy (string_view) fields: the decoded
/// value references the frame, so verification runs while the frame is alive.
template <typename Req, typename Check>
void requestRoundTripWith(const Req & req, Int16 version, Check && check)
{
    RequestHeader header;
    header.api_key = toWire(Req::api_key);
    header.api_version = version;
    header.correlation_id = 3;
    header.client_id = "c";
    header.flexible = Req::flexible(version);

    const String frame = buildFrame([&](Writer & w)
    {
        header.encode(w);
        req.encode(w, version);
    });

    Reader r(frame);
    const Int32 size = r.readInt32();
    ASSERT_EQ(static_cast<size_t>(size), r.remaining());
    RequestHeader::decode(r, header.flexible);
    const Req got = Req::decode(r, version);
    ASSERT_TRUE(r.empty());
    check(got);
}

template <typename Req, typename Resp, typename Check>
void responseRoundTripWith(const Resp & resp, Int16 version, Check && check)
{
    const String frame = buildFrame([&](Writer & w)
    {
        ResponseHeader{9}.encode(w, Req::responseHeaderFlexible(version));
        resp.encode(w, version);
    });

    Reader r(frame);
    const Int32 size = r.readInt32();
    ASSERT_EQ(static_cast<size_t>(size), r.remaining());
    ResponseHeader::decode(r, Req::responseHeaderFlexible(version));
    const Resp got = Resp::decode(r, version);
    ASSERT_TRUE(r.empty());
    check(got);
}

/// Frame response -> parse back; returns the decoded body.
/// Value-returning, so the original Catch2 REQUIREs become non-fatal EXPECTs.
template <typename Req, typename Resp>
Resp responseRoundTrip(const Resp & resp, Int16 version)
{
    const String frame = buildFrame([&](Writer & w)
    {
        ResponseHeader{9}.encode(w, Req::responseHeaderFlexible(version));
        resp.encode(w, version);
    });

    Reader r(frame);
    const Int32 size = r.readInt32();
    EXPECT_EQ(static_cast<size_t>(size), r.remaining());
    const auto header = ResponseHeader::decode(r, Req::responseHeaderFlexible(version));
    EXPECT_EQ(header.correlation_id, 9);
    const Resp got = Resp::decode(r, version);
    EXPECT_TRUE(r.empty());
    return got;
}

}

/// Fetch round trips at v11 (non-flex, by name), v12 (flex, by name) and v13 (by id).
TEST(KafkaClientMessagesRoundtrip, FetchRoundTripsAtV11V12AndV13)
{
    for (Int16 version : {Int16{11}, Int16{12}, Int16{13}})
    {
        const bool by_id = FetchRequest::usesTopicIds(version);

        FetchRequest req;
        req.replica_id = -1;
        req.max_wait_ms = 500;
        req.min_bytes = 1;
        req.max_bytes = 1'048'576;
        req.isolation_level = 0;
        req.session_id = 0;
        req.session_epoch = -1;
        FetchPartition partition;
        partition.partition = 0;
        partition.current_leader_epoch = 0;
        partition.fetch_offset = 0;
        partition.last_fetched_epoch = -1;  // absent below v12; -1 is also the decode default
        partition.log_start_offset = -1;
        partition.partition_max_bytes = 1'048'576;
        FetchTopic topic;
        if (by_id)
            topic.topic_id = uuidFromLowByte(0xBE);
        else
            topic.name = "topic-a";
        topic.partitions = {partition};
        req.topics = {topic};
        ForgottenTopic forgotten;
        if (by_id)
            forgotten.topic_id = uuidFromLowByte(0xFA);
        else
            forgotten.name = "topic-b";
        forgotten.partitions = {1, 2, 3};
        req.forgotten_topics_data = {forgotten};
        req.rack_id = "rack-a";
        EXPECT_EQ(requestRoundTrip(req, version), req);

        /// The response carries a records blob — zero-copy view, so verify
        /// while the frame is alive.
        const String blob = "fake-records-blob-bytes";
        FetchResponse resp;
        resp.throttle_time_ms = 0;
        resp.error_code = 0;
        resp.session_id = 42;
        FetchPartitionResponse presp;
        presp.partition_index = 0;
        presp.error_code = 0;
        presp.high_watermark = 100;
        presp.last_stable_offset = 100;
        presp.log_start_offset = 0;
        presp.aborted_transactions = std::vector<AbortedTransaction>{{1, 50}};
        presp.preferred_read_replica = -1;
        presp.records = blob;
        FetchTopicResponse topic_resp;
        if (by_id)
            topic_resp.topic_id = uuidFromLowByte(0xBE);
        else
            topic_resp.name = "topic-a";
        topic_resp.partitions = {presp};
        resp.responses = {topic_resp};
        responseRoundTripWith<FetchRequest>(resp, version, [&](const FetchResponse & got) { EXPECT_EQ(got, resp); });
    }
}

/// List offsets round trips at v5 (non-flex), v6 and v7.
TEST(KafkaClientMessagesRoundtrip, ListOffsetsRoundTripsAtV5V6AndV7)
{
    for (Int16 version : {Int16{5}, Int16{6}, Int16{7}})
    {
        ListOffsetsRequest req;
        req.replica_id = -1;
        req.isolation_level = 0;
        req.topics = {{"topic-a", {{0, 0, list_offsets_latest}}}};
        EXPECT_EQ(requestRoundTrip(req, version), req);

        ListOffsetsResponse resp;
        resp.topics = {{"topic-a", {{0, 0, 1'700'000'000'000, 42, 1}}}};
        EXPECT_EQ((responseRoundTrip<ListOffsetsRequest, ListOffsetsResponse>(resp, version)), resp);
    }
}

/// Find coordinator round trips at v4 (batched) and v3 (single key).
TEST(KafkaClientMessagesRoundtrip, FindCoordinatorRoundTripsAtV4AndV3)
{
    FindCoordinatorRequest req;
    req.key_type = coordinator_type_group;
    req.coordinator_keys = {"group-1"};
    EXPECT_EQ(requestRoundTrip(req, 4), req);
    EXPECT_EQ(requestRoundTrip(req, 3), req);

    FindCoordinatorResponse resp;
    resp.coordinators = {{"group-1", 1, "broker-1", 9092, 0, std::nullopt}};
    EXPECT_EQ((responseRoundTrip<FindCoordinatorRequest, FindCoordinatorResponse>(resp, 4)), resp);

    /// v3 does not echo the key back.
    FindCoordinatorResponse v3 = resp;
    v3.coordinators[0].key.clear();
    EXPECT_EQ((responseRoundTrip<FindCoordinatorRequest, FindCoordinatorResponse>(v3, 3)), v3);
}

TEST(KafkaClientMessagesRoundtrip, OffsetCommitRoundTripsAtV8AndV9)
{
    for (Int16 version : {Int16{8}, Int16{9}})
    {
        OffsetCommitRequest req;
        req.group_id = "g";
        req.generation_id_or_member_epoch = -1;  // group-less commit
        req.member_id = "";
        req.group_instance_id = std::nullopt;
        req.topics = {{"t", {{0, 42, 1, std::optional<String>{"meta"}}}}};
        EXPECT_EQ(requestRoundTrip(req, version), req);

        OffsetCommitResponse resp;
        resp.topics = {{"t", {{0, 0}}}};
        EXPECT_EQ((responseRoundTrip<OffsetCommitRequest, OffsetCommitResponse>(resp, version)), resp);
    }
}

/// Offset fetch round trips at v6/v7 (single group) and v8 (batched).
TEST(KafkaClientMessagesRoundtrip, OffsetFetchRoundTripsAtV6V7AndV8)
{
    for (Int16 version : {Int16{6}, Int16{7}, Int16{8}})
    {
        OffsetFetchRequest req;
        OffsetFetchGroup group;
        group.group_id = "g";
        group.topics = std::vector<OffsetFetchTopic>{{"t", {0, 1}}};
        req.groups = {group};
        req.require_stable = version >= 7;  // field absent at v6: decode default false
        EXPECT_EQ(requestRoundTrip(req, version), req);

        OffsetFetchResponse resp;
        OffsetFetchGroupResponse group_resp;
        group_resp.group_id = version >= 8 ? "g" : "";  // v6/v7 do not echo the group id
        group_resp.topics = {{"t", {{0, 42, 1, std::nullopt, 0}}}};
        group_resp.error_code = 0;
        resp.groups = {group_resp};
        EXPECT_EQ((responseRoundTrip<OffsetFetchRequest, OffsetFetchResponse>(resp, version)), resp);
    }
}

TEST(KafkaClientMessagesRoundtrip, OffsetFetchRoundTripsAtV6V7AndV8NullTopicsMeansAllCommittedTopics)
{
    OffsetFetchRequest all;
    all.groups = {{"g", std::nullopt}};
    EXPECT_EQ(requestRoundTrip(all, 8), all);
    EXPECT_EQ(requestRoundTrip(all, 6), all);
}

/// Produce round trips at v7/v8 (non-flex) and v9 with a real record batch.
TEST(KafkaClientMessagesRoundtrip, ProduceRoundTripsAtV7V8AndV9WithARealRecordBatch)
{
    BatchRecord record;
    record.key = Bytes{"k"};
    record.value = Bytes{"v"};
    const String blob = encodeBatches({RecordBatch::make(0, {record})});

    for (Int16 version : {Int16{7}, Int16{8}, Int16{9}})
    {
        ProduceRequest req;
        req.transactional_id = std::nullopt;
        req.acks = -1;
        req.timeout_ms = 30'000;
        req.topic_data = {{"topic-a", {{0, std::string_view{blob}}}}};
        /// The request carries the blob as a zero-copy view — verify while
        /// the frame is alive.
        requestRoundTripWith(req, version, [&](const ProduceRequest & parsed)
        {
            EXPECT_EQ(parsed, req);
            /// The blob inside the parsed request still decodes to the
            /// original batch.
            const DecodedBatches recovered = decodeBatches(*parsed.topic_data[0].partition_data[0].records);
            ASSERT_EQ(recovered.batches.size(), 1);
            EXPECT_EQ(recovered.batches[0].records.size(), 1);
            EXPECT_EQ(recovered.batches[0].records[0], record);
        });

        ProduceResponse resp;
        ProducePartitionResponse presp;
        presp.index = 0;
        presp.error_code = 0;
        presp.base_offset = 100;
        presp.log_append_time_ms = -1;
        presp.log_start_offset = 0;
        presp.error_message = std::nullopt;
        if (version >= 8)
            presp.record_errors = {{3, std::optional<String>{"bad batch"}}};
        resp.responses = {{"topic-a", {presp}}};
        resp.throttle_time_ms = 0;
        EXPECT_EQ((responseRoundTrip<ProduceRequest, ProduceResponse>(resp, version)), resp);
    }
}

TEST(KafkaClientMessagesRoundtrip, SaslHandshakeV1UsesLegacyHeaders)
{
    EXPECT_FALSE(SaslHandshakeRequest::flexible(1));
    EXPECT_FALSE(SaslHandshakeRequest::responseHeaderFlexible(1));

    SaslHandshakeRequest req;
    req.mechanism = "PLAIN";
    EXPECT_EQ(requestRoundTrip(req, 1), req);

    SaslHandshakeResponse resp;
    resp.error_code = 0;
    resp.mechanisms = {"PLAIN", "SCRAM-SHA-256"};
    EXPECT_EQ((responseRoundTrip<SaslHandshakeRequest, SaslHandshakeResponse>(resp, 1)), resp);
}

/// Sasl authenticate round trips at v1 (non-flex) and v2.
TEST(KafkaClientMessagesRoundtrip, SaslAuthenticateRoundTripsAtV1AndV2)
{
    for (Int16 version : {Int16{1}, Int16{2}})
    {
        SaslAuthenticateRequest req;
        req.auth_bytes = Bytes("\0user\0secret", 12);
        EXPECT_EQ(requestRoundTrip(req, version), req);

        SaslAuthenticateResponse resp;
        resp.error_code = 0;
        resp.error_message = std::nullopt;
        resp.auth_bytes = {};
        resp.session_lifetime_ms = 0;
        EXPECT_EQ((responseRoundTrip<SaslAuthenticateRequest, SaslAuthenticateResponse>(resp, version)), resp);
    }
}
