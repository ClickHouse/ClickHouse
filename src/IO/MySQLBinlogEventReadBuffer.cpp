#include <IO/MySQLBinlogEventReadBuffer.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MySQLBinlogEventReadBuffer::MySQLBinlogEventReadBuffer(ReadBuffer & in_, size_t checksum_signature_length_)
    : ReadBuffer(nullptr, 0, 0), in(in_), checksum_signature_length(checksum_signature_length_)
{
    if (checksum_signature_length > MAX_CHECKSUM_SIGNATURE_LENGTH)
        throw Exception("LOGICAL ERROR: checksum_signature_length must be less than MAX_CHECKSUM_SIGNATURE_LENGTH. It is a bug.",
            ErrorCodes::LOGICAL_ERROR);

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
        if (likely(in.available() > checksum_signature_length))
        {
            working_buffer = ReadBuffer::Buffer(in.position(), in.buffer().end() - checksum_signature_length);
            in.ignore(working_buffer.size());
            return true;
        }

        in.readStrict(checksum_buf, checksum_signature_length);
        checksum_buff_size = checksum_buff_limit = checksum_signature_length;
    }
    else
    {
        for (size_t index = 0; index < checksum_buff_size - checksum_buff_limit; ++index)
            checksum_buf[index] = checksum_buf[checksum_buff_limit + index];

        checksum_buff_size -= checksum_buff_limit;
        size_t read_bytes = checksum_signature_length - checksum_buff_size;
        in.readStrict(checksum_buf + checksum_buff_size, read_bytes);   /// Minimum checksum_signature_length bytes
        checksum_buff_size = checksum_buff_limit = checksum_signature_length;
    }

    if (in.eof())
        return false;

    if (in.available() < checksum_signature_length)
    {
        size_t left_move_size = checksum_signature_length - in.available();
        checksum_buff_limit = checksum_buff_size - left_move_size;
    }

    working_buffer = ReadBuffer::Buffer(checksum_buf, checksum_buf + checksum_buff_limit);
    return true;
}

MySQLBinlogEventReadBuffer::~MySQLBinlogEventReadBuffer()
{
    try
    {
        /// ignore last checksum_signature_length bytes
        nextIfAtEnd();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
