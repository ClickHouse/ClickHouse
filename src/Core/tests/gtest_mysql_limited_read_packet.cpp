#include <gtest/gtest.h>

#include <Core/MySQL/IMySQLReadPacket.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>

#include <string>

using namespace DB;
using namespace DB::MySQLProtocol;

namespace DB::ErrorCodes
{
extern const int LIMIT_EXCEEDED;
}

namespace
{

/// The limit `LimitedReadPacket` applies, from IMySQLReadPacket.cpp.
constexpr size_t PACKET_PAYLOAD_LIMIT = 10000;

/// Serves its data a few bytes at a time. The limiter only reaches its `expect_eof` check when it
/// has to refill, which a fully buffered source never makes it do.
class ChunkedSource : public ReadBuffer
{
public:
    ChunkedSource(String data_, size_t chunk_) : ReadBuffer(nullptr, 0), data(std::move(data_)), chunk(chunk_) {}

private:
    bool nextImpl() override
    {
        if (consumed >= data.size())
            return false;

        const size_t count = std::min(chunk, data.size() - consumed);
        BufferBase::set(data.data() + consumed, count, 0);
        consumed += count;
        return true;
    }

    String data;
    size_t chunk;
    size_t consumed = 0;
};

struct PayloadCollector : public LimitedReadPacket
{
    String payload;

    void readPayloadImpl(ReadBuffer & buf) override { readStringUntilEOF(payload, buf); }
};

String packet(const String & payload, uint8_t sequence_id)
{
    String result;
    const size_t length = payload.size();
    result += static_cast<char>(length & 0xFF);
    result += static_cast<char>((length >> 8) & 0xFF);
    result += static_cast<char>((length >> 16) & 0xFF);
    result += static_cast<char>(sequence_id);
    result += payload;
    return result;
}

}

/// The limit counts one packet's payload, not the connection. A payload exactly at the limit is
/// allowed even though the 4-byte header pushes the connection past it, and the next packet is not
/// mistaken for overflow.
TEST(MySQLLimitedReadPacket, PayloadAtTheLimitFollowedByAnotherPacket)
{
    const String first(PACKET_PAYLOAD_LIMIT, 'a');
    ChunkedSource in(packet(first, 0) + packet("second", 1), 64);

    uint8_t sequence_id = 0;
    PayloadCollector collector;
    collector.readPayload(in, sequence_id);

    EXPECT_EQ(collector.payload, first);
    EXPECT_EQ(sequence_id, 1);

    PayloadCollector next;
    next.readPayload(in, sequence_id);
    EXPECT_EQ(next.payload, "second");
}

TEST(MySQLLimitedReadPacket, PayloadOverTheLimitIsRejected)
{
    ChunkedSource in(packet(String(PACKET_PAYLOAD_LIMIT + 1, 'a'), 0), 64);

    uint8_t sequence_id = 0;
    PayloadCollector collector;
    try
    {
        collector.readPayload(in, sequence_id);
        FAIL() << "An oversized payload was accepted";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::LIMIT_EXCEEDED);
    }
}

/// The unpacked variant is handed a payload that already ends where the packet ends.
TEST(MySQLLimitedReadPacket, UnpackedPayloadOverTheLimitIsRejected)
{
    ChunkedSource in(String(PACKET_PAYLOAD_LIMIT + 1, 'a'), 64);

    PayloadCollector collector;
    try
    {
        collector.readPayloadWithUnpacked(in);
        FAIL() << "An oversized unpacked payload was accepted";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::LIMIT_EXCEEDED);
    }
}
