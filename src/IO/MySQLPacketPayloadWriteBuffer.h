#pragma once

#include <IO/WriteBuffer.h>

namespace DB
{

/** Writing packets.
 *  https://dev.mysql.com/doc/internals/en/mysql-packet.html
 */
class MySQLPacketPayloadWriteBuffer : public WriteBuffer
{
public:
    MySQLPacketPayloadWriteBuffer(WriteBuffer & out_, size_t payload_length_, uint8_t & sequence_id_);

    bool remainingPayloadSize() const { return total_left; }

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
