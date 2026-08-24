#pragma once

#include <IO/HashingWriteBuffer.h>
#include <IO/ReadBuffer.h>

namespace DB
{

/*
 * Calculates the hash from the read data. When reading, the data is read from the nested ReadBuffer.
 * Small pieces are copied into its own memory.
 */
class HashingReadBuffer : public IHashingBuffer<ReadBuffer>
{
public:
    explicit HashingReadBuffer(ReadBuffer & in_, size_t block_size_ = DBMS_DEFAULT_HASHING_BLOCK_SIZE)
        : IHashingBuffer<ReadBuffer>(block_size_), in(in_)
    {
        working_buffer = in.buffer();
        pos = in.position();
        hashing_begin = pos;
    }

    uint128 getHash()
    {
        if (pos > hashing_begin)
        {
            calculateHash(hashing_begin, pos - hashing_begin);
            hashing_begin = pos;
        }
        return IHashingBuffer<ReadBuffer>::getHash();
    }

private:
    bool nextImpl() override
    {
        if (pos > hashing_begin)
            calculateHash(hashing_begin, pos - hashing_begin);

        in.position() = pos;
        bool res = in.next();
        working_buffer = in.buffer();

        // `pos` may be different from working_buffer.begin() when using sophisticated ReadBuffers.
        pos = in.position();
        hashing_begin = pos;

        return res;
    }

    ReadBuffer & in;
    BufferBase::Position hashing_begin;
};

}
