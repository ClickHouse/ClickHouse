#include <Storages/Kafka/Clients/Protocol/Wire.h>

#include <gtest/gtest.h>

#include <base/types.h>

#include <limits>

using Kafka::Exception;
using Kafka::ErrorKind;
using Kafka::Protocol::Reader;
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

TEST(KafkaClientWire, UnsignedVarintRoundTrip)
{
    for (UInt32 v : {0u, 1u, 127u, 128u, 255u, 16383u, 16384u, std::numeric_limits<UInt32>::max()})
    {
        Writer w;
        w.writeUnsignedVarint(v);
        Reader r(w.view());
        EXPECT_EQ(r.readUnsignedVarint(), v);
        EXPECT_TRUE(r.empty());
    }
}

TEST(KafkaClientWire, UnsignedVarintKnownEncodings)
{
    Writer w;
    w.writeUnsignedVarint(0);
    w.writeUnsignedVarint(1);
    w.writeUnsignedVarint(127);
    w.writeUnsignedVarint(128);
    w.writeUnsignedVarint(300);
    EXPECT_EQ(w.view(), bytes({0x00, 0x01, 0x7F, 0x80, 0x01, 0xAC, 0x02}));
}

TEST(KafkaClientWire, UnsignedVarintTooLongRejected)
{
    const String payload = bytes({0xFF, 0xFF, 0xFF, 0xFF, 0xFF});
    Reader r(payload);
    EXPECT_THROW(r.readUnsignedVarint(), Exception);
}

TEST(KafkaClientWire, ZigzagVarintRoundTripAndKnownEncodings)
{
    for (Int32 v : {0, 1, -1, 63, -64, std::numeric_limits<Int32>::max(), std::numeric_limits<Int32>::min()})
    {
        Writer w;
        w.writeVarint(v);
        Reader r(w.view());
        EXPECT_EQ(r.readVarint(), v);
        EXPECT_TRUE(r.empty());
    }
    // Zig-zag: 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3, 2 -> 4.
    Writer w;
    w.writeVarint(0);
    w.writeVarint(-1);
    w.writeVarint(1);
    w.writeVarint(-2);
    w.writeVarint(2);
    EXPECT_EQ(w.view(), bytes({0x00, 0x01, 0x02, 0x03, 0x04}));
}

TEST(KafkaClientWire, VarlongRoundTrip)
{
    for (Int64 v :
         {Int64{0}, Int64{1}, Int64{-1}, std::numeric_limits<Int64>::max(), std::numeric_limits<Int64>::min()})
    {
        Writer w;
        w.writeVarlong(v);
        Reader r(w.view());
        EXPECT_EQ(r.readVarlong(), v);
        EXPECT_TRUE(r.empty());
    }
}

TEST(KafkaClientWire, FixedIntegersAreBigEndian)
{
    Writer w;
    w.writeInt16(0x0102);
    w.writeInt32(0x01020304);
    w.writeInt64(0x0102030405060708);
    EXPECT_EQ(w.view(), bytes({0x01, 0x02, 0x01, 0x02, 0x03, 0x04, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}));

    Reader r(w.view());
    EXPECT_EQ(r.readInt16(), 0x0102);
    EXPECT_EQ(r.readInt32(), 0x01020304);
    EXPECT_EQ(r.readInt64(), 0x0102030405060708);

    Writer wn;
    wn.writeInt32(-1);
    EXPECT_EQ(wn.view(), bytes({0xFF, 0xFF, 0xFF, 0xFF}));
    Reader rn(wn.view());
    EXPECT_EQ(rn.readInt32(), -1);
}

TEST(KafkaClientWire, CompactStringRoundTrip)
{
    const std::vector<String> samples{String{}, String{"x"}, String{"hello"}, String(200, 'a')};
    for (const String & s : samples)
    {
        Writer w;
        w.writeCompactString(s);
        Reader r(w.view());
        EXPECT_EQ(r.readCompactString(), s);
        EXPECT_TRUE(r.empty());
    }
}

TEST(KafkaClientWire, CompactNullableStringNullPath)
{
    Writer w;
    w.writeCompactNullableString(std::nullopt);
    EXPECT_EQ(w.view(), bytes({0x00}));
    Reader r(w.view());
    EXPECT_EQ(r.readCompactNullableString(), std::nullopt);
}

TEST(KafkaClientWire, NullableStringRoundTrip)
{
    Writer w;
    w.writeNullableString(std::nullopt);
    w.writeNullableString(std::optional<String>{"hi"});
    EXPECT_EQ(w.view(), bytes({0xFF, 0xFF, 0x00, 0x02, 'h', 'i'}));
    Reader r(w.view());
    EXPECT_EQ(r.readNullableString(), std::nullopt);
    EXPECT_EQ(r.readNullableString(), std::optional<String>{"hi"});
    EXPECT_TRUE(r.empty());
}

TEST(KafkaClientWire, BytesVariantsRoundTrip)
{
    const String payload = bytes({0x00, 0x01, 0xFF});
    Writer w;
    w.writeBytes(payload);
    w.writeNullableBytes(std::nullopt);
    w.writeCompactBytes(payload);
    w.writeCompactNullableBytes(std::nullopt);
    Reader r(w.view());
    EXPECT_EQ(r.readBytes(), payload);
    EXPECT_EQ(r.readNullableBytes(), std::nullopt);
    EXPECT_EQ(r.readCompactBytes(), payload);
    EXPECT_EQ(r.readCompactNullableBytes(), std::nullopt);
    EXPECT_TRUE(r.empty());
}

TEST(KafkaClientWire, CompactArrayRoundTrip)
{
    const std::vector<Int32> items{10, 20, 30};
    Writer w;
    w.writeCompactArray(items, [](Writer & wr, Int32 v) { wr.writeInt32(v); });
    Reader r(w.view());
    const auto got = r.readCompactArray<Int32>([](Reader & rd) { return rd.readInt32(); });
    EXPECT_EQ(got, items);
    EXPECT_TRUE(r.empty());
}

TEST(KafkaClientWire, LegacyArrayRoundTrip)
{
    const std::vector<String> items{"a", "bc"};
    Writer w;
    w.writeArray(items, [](Writer & wr, const String & s) { wr.writeString(s); });
    Reader r(w.view());
    const auto got = r.readArray<String>([](Reader & rd) { return rd.readString(); });
    EXPECT_EQ(got, items);
    EXPECT_TRUE(r.empty());
}

TEST(KafkaClientWire, CorruptArrayCountDoesNotReserveHugeMemory)
{
    // COMPACT_ARRAY claiming ~500M elements with 2 bytes of payload.
    Writer w;
    w.writeUnsignedVarint(500'000'000);
    w.writeInt16(0);
    Reader r(w.view());
    EXPECT_THROW(r.readCompactArray<Int16>([](Reader & rd) { return rd.readInt16(); }), Exception);
}

TEST(KafkaClientWire, UnderflowIsAProtocolError)
{
    const String one = bytes({0x00});
    Reader r(one);
    try
    {
        r.readInt32();
        FAIL() << "expected throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::Protocol);
        EXPECT_FALSE(e.isRetriable());
    }
}

TEST(KafkaClientWire, UuidRoundTrip)
{
    Kafka::Uuid id;
    for (size_t i = 0; i < 16; ++i)
        id.data[i] = static_cast<UInt8>(i + 1);
    Writer w;
    w.writeUuid(id);
    Reader r(w.view());
    EXPECT_EQ(r.readUuid(), id);
    EXPECT_EQ(id.toString(), "01020304-0506-0708-090a-0b0c0d0e0f10");
}

TEST(KafkaClientWire, TaggedFieldsSkipUnknownTags)
{
    // 2 tags: tag=1 size=3 data=[1,2,3], tag=42 size=0, then a sentinel byte.
    Writer w;
    w.writeUnsignedVarint(2);
    w.writeUnsignedVarint(1);
    w.writeUnsignedVarint(3);
    w.writeRaw(bytes({1, 2, 3}));
    w.writeUnsignedVarint(42);
    w.writeUnsignedVarint(0);
    w.writeRaw(bytes({0xAA}));

    Reader r(w.view());
    r.skipTaggedFields();
    EXPECT_EQ(r.remaining(), 1);
    EXPECT_EQ(static_cast<UInt8>(r.readRaw(1)[0]), 0xAA);
}

TEST(KafkaClientWire, EmptyTaggedFieldsIsASingleZeroByte)
{
    Writer w;
    w.writeEmptyTaggedFields();
    EXPECT_EQ(w.view(), bytes({0x00}));
    Reader r(w.view());
    r.skipTaggedFields();
    EXPECT_TRUE(r.empty());
}

TEST(KafkaClientWire, VersionGuardThrowsNotSupported)
{
    try
    {
        Kafka::Protocol::checkVersionSupported(7, 12, 13, "Metadata");
        FAIL() << "expected throw";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.getKind(), ErrorKind::NotSupported);
        EXPECT_TRUE(String(e.message()).contains("Metadata v7"));
    }
}
