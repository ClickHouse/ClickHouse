#pragma once

#include <IO/ReadBuffer.h>

namespace DB
{

namespace MySQLProtocol
{

extern const size_t MAX_PACKET_LENGTH;

/** Reading packets.
 *  Internally, it calls (if no more data) next() method of the underlying ReadBufferFromPocoSocket, and sets the working buffer to the rest part of the current packet payload.
 */
class PacketPayloadReadBuffer : public ReadBuffer
{
private:
    ReadBuffer & in;
    uint8_t & sequence_id;
    const size_t max_packet_size = MAX_PACKET_LENGTH;

    bool has_read_header = false;

    // Size of packet which is being read now.
    size_t payload_length = 0;

    // Offset in packet payload.
    size_t offset = 0;

protected:
    bool nextImpl() override;

public:
    PacketPayloadReadBuffer(ReadBuffer & in_, uint8_t & sequence_id_);
};

}

}

