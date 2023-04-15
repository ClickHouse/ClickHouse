#pragma once

#include <IO/CryptographicHashingWriteBuffer.h>
#include <IO/ReadBuffer.h>

namespace DB {

class CryptoHashingReadBuffer : public ICryptoHashingBuffer<ReadBuffer>
{
public:
    explicit CryptoHashingReadBuffer(ReadBuffer & in_, size_t block_size_ = DBMS_DEFAULT_HASHING_BLOCK_SIZE)
        : ICryptoHashingBuffer<ReadBuffer>(block_size_), in(in_)
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
        return ICryptoHashingBuffer<ReadBuffer>::getHash();
    }

private:
    bool nextImpl() override
    {
        if (pos > hashing_begin)
            calculateHash(hashing_begin, pos - hashing_begin);

        in.position() = pos;
        bool res = in.next();
        working_buffer = in.buffer();

        pos = in.position();
        hashing_begin = pos;

        return res;
    }

    ReadBuffer & in;
    BufferBase::Position hashing_begin;
};
}
