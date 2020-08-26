#pragma once

#include <IO/ReadBuffer.h>
#include <IO/HashingWriteBuffer.h>

namespace DB
{
/*
 * Calculates the hash from the read data. When reading, the data is read from the nested ReadBuffer.
 * Small pieces are copied into its own memory.
 */
class HashingReadBuffer : public IHashingBuffer<ReadBuffer>
{
public:
    HashingReadBuffer(ReadBuffer & in_, size_t block_size_ = DBMS_DEFAULT_HASHING_BLOCK_SIZE) :
        IHashingBuffer<ReadBuffer>(block_size_), in(in_)
    {
        working_buffer = in.buffer();
        pos = in.position();

        /// calculate hash from the data already read
        if (working_buffer.size())
        {
            calculateHash(pos, working_buffer.end() - pos);
        }
    }

private:
    bool nextImpl() override
    {
        in.position() = pos;
        bool res = in.next();
        working_buffer = in.buffer();
        pos = in.position();

        // `pos` may be different from working_buffer.begin() when using AIO.
        calculateHash(pos, working_buffer.end() - pos);

        return res;
    }

private:
    ReadBuffer & in;
};
}
