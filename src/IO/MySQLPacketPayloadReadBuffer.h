#pragma once

#include <IO/ReadBuffer.h>

namespace DB
{

/** Reading packets.
 *  Internally, it calls (if no more data) next() method of the underlying ReadBufferFromPocoSocket, and sets the working buffer to the rest part of the current packet payload.
 */
class MySQLPacketPayloadReadBuffer : public ReadBuffer
{
private:
    ReadBuffer & in;
    uint8_t & sequence_id;

    bool has_read_header = false;

    // Size of packet which is being read now.
    size_t payload_length = 0;

    // Offset in packet payload.
    size_t offset = 0;

protected:
    bool nextImpl() override;

public:
    MySQLPacketPayloadReadBuffer(ReadBuffer & in_, uint8_t & sequence_id_);
};

}

