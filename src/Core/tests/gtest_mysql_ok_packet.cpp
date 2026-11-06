#include <gtest/gtest.h>

#include <Core/MySQL/PacketsGeneric.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;
using namespace DB::MySQLProtocol;
using namespace DB::MySQLProtocol::Generic;

namespace
{

size_t readPacketLength(const String & wire)
{
    return static_cast<UInt8>(wire[0]) | (static_cast<UInt8>(wire[1]) << 8) | (static_cast<UInt8>(wire[2]) << 16);
}

}

/// `getPayloadSize` must match what `writePayloadImpl` writes. When it did not, an `OK`
/// packet whose `last_insert_id` needs more than one length-encoded byte was written past
/// the declared payload size and serialization threw "Cannot write after end of buffer".
TEST(MySQLOKPacket, LargeLastInsertIdFitsTheDeclaredPayload)
{
    OKPacket packet(0x00, CLIENT_PROTOCOL_41, 0, 0, 0);
    packet.last_insert_id = 1ULL << 40;

    WriteBufferFromOwnString out;
    uint8_t sequence_id = 0;
    ASSERT_NO_THROW(packet.writePayload(out, sequence_id));

    const String wire = out.str();
    ASSERT_GE(wire.size(), 4u);
    const size_t expected_payload
        = 1 + getLengthEncodedNumberSize(0) + getLengthEncodedNumberSize(1ULL << 40) + 4 + getLengthEncodedStringSize("");
    EXPECT_EQ(readPacketLength(wire), expected_payload);
    EXPECT_EQ(wire.size(), expected_payload + 4);
}

/// Some servers omit the optional `info` field when there is nothing to report. Reading such
/// a packet must not try to parse a length-encoded string from the now-empty buffer.
TEST(MySQLOKPacket, ReadsPacketWithoutInfoField)
{
    /// header, affected_rows, last_insert_id, status_flags, warnings, no info.
    const String wire("\x00\x00\x00\x02\x00\x00\x00", 7);

    ReadBufferFromString in(wire);
    OKPacket packet(CLIENT_PROTOCOL_41);
    ASSERT_NO_THROW(packet.readPayloadWithUnpacked(in));
    EXPECT_EQ(packet.affected_rows, 0u);
    EXPECT_EQ(packet.last_insert_id, 0u);
    EXPECT_TRUE(packet.info.empty());
}
