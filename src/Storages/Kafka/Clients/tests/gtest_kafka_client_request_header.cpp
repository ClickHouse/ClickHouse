#include <Storages/Kafka/Clients/Protocol/RequestHeader.h>

#include <gtest/gtest.h>

#include <base/types.h>

using Kafka::Protocol::buildFrame;
using Kafka::Protocol::Reader;
using Kafka::Protocol::RequestHeader;
using Kafka::Protocol::ResponseHeader;
using Kafka::Protocol::Writer;

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

TEST(KafkaClientRequestHeader, RequestHeaderV2ByteExactClientIdStaysNonCompact)
{
    /// THE codec tripwire (KIP-482 exception): a uvarint length here makes
    /// brokers silently drop the connection.
    RequestHeader h;
    h.api_key = 18;
    h.api_version = 3;
    h.correlation_id = 0x12345678;
    h.client_id = "ab";
    h.flexible = true;

    Writer w;
    h.encode(w);
    EXPECT_EQ(
        w.view(),
        bytes({
            0x00, 0x12,              // api_key 18
            0x00, 0x03,              // api_version 3
            0x12, 0x34, 0x56, 0x78,  // correlation id
            0x00, 0x02, 'a', 'b',    // client_id as i16-length NULLABLE_STRING
            0x00,                    // empty tagged fields
        }));
}

TEST(KafkaClientRequestHeader, RequestHeaderRoundTripsInBothHeaderVersions)
{
    for (bool flexible : {true, false})
    {
        RequestHeader h;
        h.api_key = 17;
        h.api_version = 1;
        h.correlation_id = 7;
        h.client_id = std::nullopt;
        h.flexible = flexible;

        Writer w;
        h.encode(w);
        Reader r(w.view());
        const auto got = RequestHeader::decode(r, flexible);
        EXPECT_EQ(got, h);
        EXPECT_TRUE(r.empty());
    }
}

TEST(KafkaClientRequestHeader, ResponseHeaderRoundTripsInBothHeaderVersions)
{
    for (bool flexible : {true, false})
    {
        ResponseHeader h;
        h.correlation_id = 99;
        Writer w;
        h.encode(w, flexible);
        Reader r(w.view());
        const auto got = ResponseHeader::decode(r, flexible);
        EXPECT_EQ(got.correlation_id, 99);
        EXPECT_TRUE(r.empty());
    }
}

TEST(KafkaClientRequestHeader, FrameBuilderBackFillsTheI32SizePrefix)
{
    const String frame = buildFrame([](Writer & w) { w.writeRaw("hello"); });
    EXPECT_EQ(frame.size(), 9);
    EXPECT_EQ(frame.substr(0, 4), bytes({0x00, 0x00, 0x00, 0x05}));
    EXPECT_EQ(frame.substr(4), "hello");
}
