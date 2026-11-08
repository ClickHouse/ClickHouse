#include <Storages/Kafka/Clients/Protocol/RequestHeader.h>
#include <Storages/Kafka/Clients/Protocol/Messages/ApiVersions.h>

#include <gtest/gtest.h>

#include <base/types.h>

using namespace Kafka::Protocol;

namespace
{

String bytes(std::initializer_list<UInt8> list)
{
    String out;
    for (UInt8 b : list)
        out.push_back(static_cast<char>(b));
    return out;
}

}

TEST(KafkaClientApiVersions, ApiVersionsRequestEncodesByteExactAtV3)
{
    ApiVersionsRequest req;
    req.client_software_name = "libafka";
    req.client_software_version = "0.1";

    Writer w;
    req.encode(w, 3);
    EXPECT_EQ(
        w.view(),
        bytes({
            0x08, 'l', 'i', 'b', 'a', 'f', 'k', 'a',  // compact string len 7 -> 8
            0x04, '0', '.', '1',                      // compact string len 3 -> 4
            0x00,                                     // tagged fields
        }));
}

TEST(KafkaClientApiVersions, ApiVersionsRequestRoundTripsThroughAFullFrame)
{
    ApiVersionsRequest req;
    req.client_software_name = "libafka";
    req.client_software_version = "0.0.1";

    RequestHeader header;
    header.api_key = toWire(ApiVersionsRequest::api_key);
    header.api_version = 3;
    header.correlation_id = 1;
    header.client_id = "test-client";
    header.flexible = ApiVersionsRequest::flexible(3);

    const String frame = buildFrame([&](Writer & w)
    {
        header.encode(w);
        req.encode(w, 3);
    });

    Reader r(frame);
    const auto size = r.readInt32();
    EXPECT_EQ(static_cast<size_t>(size), r.remaining());
    const auto got_header = RequestHeader::decode(r, true);
    EXPECT_EQ(got_header.api_key, 18);
    EXPECT_EQ(got_header.api_version, 3);
    EXPECT_EQ(got_header.client_id, std::optional<String>{"test-client"});
    const auto got = ApiVersionsRequest::decode(r, 3);
    EXPECT_EQ(got, req);
    EXPECT_TRUE(r.empty());
}

TEST(KafkaClientApiVersions, ApiVersionsResponseUsesResponseHeaderV0)
{
    EXPECT_FALSE(ApiVersionsRequest::responseHeaderFlexible(3));

    ApiVersionsResponse resp;
    resp.error_code = 0;
    resp.api_keys = {{18, 0, 3}};
    resp.throttle_time_ms = 5;

    const String frame = buildFrame([&](Writer & w)
    {
        ResponseHeader{7}.encode(w, ApiVersionsRequest::responseHeaderFlexible(3));
        resp.encode(w, 3);
    });

    Reader r(frame);
    const auto size = r.readInt32();
    EXPECT_EQ(static_cast<size_t>(size), r.remaining());
    const auto header = ResponseHeader::decode(r, false);
    EXPECT_EQ(header.correlation_id, 7);
    const auto got = ApiVersionsResponse::decode(r, 3);
    EXPECT_EQ(got, resp);
    EXPECT_TRUE(r.empty());
}

TEST(KafkaClientApiVersions, ApiVersionsResponseDecodeSurvivesUnknownTaggedFields)
{
    // Simulate a broker that appends a tagged field at the response top level.
    Writer w;
    w.writeInt16(0);
    w.writeUnsignedVarint(1);  // one api key entry... encoded below
    // Element: api_key 1, min 0, max 17, no tags — but count above is (len+1),
    // so 1 means empty array. Rebuild properly:
    Writer proper;
    proper.writeInt16(0);                        // error_code
    proper.writeUnsignedVarint(2);               // 1 element
    proper.writeInt16(1);                        // api_key
    proper.writeInt16(4);                        // min
    proper.writeInt16(17);                       // max
    proper.writeEmptyTaggedFields();             // element tags
    proper.writeInt32(0);                        // throttle
    proper.writeUnsignedVarint(1);               // ONE top-level tagged field
    proper.writeUnsignedVarint(0);               // tag id 0
    proper.writeUnsignedVarint(3);               // size 3
    proper.writeRaw(bytes({0x01, 0x02, 0x03}));  // opaque tag payload

    Reader r(proper.view());
    const auto got = ApiVersionsResponse::decode(r, 3);
    EXPECT_EQ(got.api_keys.size(), 1);
    EXPECT_EQ(got.api_keys[0].api_key, 1);
    EXPECT_EQ(got.api_keys[0].min_version, 4);
    EXPECT_EQ(got.api_keys[0].max_version, 17);
    EXPECT_TRUE(r.empty());
}
