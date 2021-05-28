#include <IO/MySQLPacketPayloadWriteBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_WRITE_AFTER_END_OF_BUFFER;
}

const size_t MAX_PACKET_LENGTH = (1 << 24) - 1; // 16 mb

MySQLPacketPayloadWriteBuffer::MySQLPacketPayloadWriteBuffer(WriteBuffer & out_, size_t payload_length_, uint8_t & sequence_id_)
    : WriteBuffer(out_.position(), 0), out(out_), sequence_id(sequence_id_), total_left(payload_length_)
{
    startNewPacket();
    setWorkingBuffer();
    pos = out.position();
}

void MySQLPacketPayloadWriteBuffer::startNewPacket()
{
    payload_length = std::min(total_left, MAX_PACKET_LENGTH);
    bytes_written = 0;
    total_left -= payload_length;

    out.write(reinterpret_cast<char *>(&payload_length), 3);
    out.write(sequence_id++);
    bytes += 4;
}

void MySQLPacketPayloadWriteBuffer::setWorkingBuffer()
{
    out.nextIfAtEnd();
    working_buffer = WriteBuffer::Buffer(out.position(), out.position() + std::min(payload_length - bytes_written, out.available()));

    if (payload_length - bytes_written == 0)
    {
        /// Finished writing packet. Due to an implementation of WriteBuffer, working_buffer cannot be empty. Further write attempts will throw Exception.
        eof = true;
        working_buffer.resize(1);
    }
}

void MySQLPacketPayloadWriteBuffer::nextImpl()
{
    const int written = pos - working_buffer.begin();
    if (eof)
        throw Exception("Cannot write after end of buffer.", ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER);

    out.position() += written;
    bytes_written += written;

    /// Packets of size greater than MAX_PACKET_LENGTH are split into few packets of size MAX_PACKET_LENGTH and las packet of size < MAX_PACKET_LENGTH.
    if (bytes_written == payload_length && (total_left > 0 || payload_length == MAX_PACKET_LENGTH))
        startNewPacket();

    setWorkingBuffer();
}

}
