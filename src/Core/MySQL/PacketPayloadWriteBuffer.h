#pragma once

#include <IO/WriteBuffer.h>

namespace DB
{

namespace MySQLProtocol
{

/** Writing packets.
 *  https://dev.mysql.com/doc/internals/en/mysql-packet.html
 */
class PacketPayloadWriteBuffer : public WriteBuffer
{
public:
    PacketPayloadWriteBuffer(WriteBuffer & out_, size_t payload_length_, uint8_t & sequence_id_);

    bool remainingPayloadSize() { return total_left; }

protected:
    void nextImpl() override;

private:
    WriteBuffer & out;
    uint8_t & sequence_id;

    size_t total_left = 0;
    size_t payload_length = 0;
    size_t bytes_written = 0;
    bool eof = false;

    void startNewPacket();

    /// Sets working buffer to the rest of current packet payload.
    void setWorkingBuffer();
};

}

}
