#include <IO/MySQLBinlogEventReadBuffer.h>


namespace DB
{

MySQLBinlogEventReadBuffer::MySQLBinlogEventReadBuffer(ReadBuffer & in_)
    : ReadBuffer(nullptr, 0, 0), in(in_)
{
    nextIfAtEnd();
}

bool MySQLBinlogEventReadBuffer::nextImpl()
{
    if (hasPendingData())
        return true;

    if (in.eof())
        return false;

    if (checksum_buff_size == checksum_buff_limit)
    {
        if (likely(in.available() > CHECKSUM_CRC32_SIGNATURE_LENGTH))
        {
            working_buffer = ReadBuffer::Buffer(in.position(), in.buffer().end() - CHECKSUM_CRC32_SIGNATURE_LENGTH);
            in.ignore(working_buffer.size());
            return true;
        }

        in.readStrict(checksum_buf, CHECKSUM_CRC32_SIGNATURE_LENGTH);
        checksum_buff_size = checksum_buff_limit = CHECKSUM_CRC32_SIGNATURE_LENGTH;
    }
    else
    {
        for (size_t index = 0; index < checksum_buff_size - checksum_buff_limit; ++index)
            checksum_buf[index] = checksum_buf[checksum_buff_limit + index];

        checksum_buff_size -= checksum_buff_limit;
        size_t read_bytes = CHECKSUM_CRC32_SIGNATURE_LENGTH - checksum_buff_size;
        in.readStrict(checksum_buf + checksum_buff_size, read_bytes);   /// Minimum CHECKSUM_CRC32_SIGNATURE_LENGTH bytes
        checksum_buff_size = checksum_buff_limit = CHECKSUM_CRC32_SIGNATURE_LENGTH;
    }

    if (in.eof())
        return false;

    if (in.available() < CHECKSUM_CRC32_SIGNATURE_LENGTH)
    {
        size_t left_move_size = CHECKSUM_CRC32_SIGNATURE_LENGTH - in.available();
        checksum_buff_limit = checksum_buff_size - left_move_size;
    }

    working_buffer = ReadBuffer::Buffer(checksum_buf, checksum_buf + checksum_buff_limit);
    return true;
}

MySQLBinlogEventReadBuffer::~MySQLBinlogEventReadBuffer()
{
    try
    {
        /// ignore last 4 bytes
        nextIfAtEnd();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
