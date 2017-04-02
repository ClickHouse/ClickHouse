#pragma once

#include <IO/ReadBuffer.h>
#include <IO/HashingWriteBuffer.h>

namespace DB
{
/*
 * Считает хэш от прочитанных данных. При чтении данные читаются из вложенного ReadBuffer.
 * Мелкие кусочки копируются в собственную память.
 */
class HashingReadBuffer : public IHashingBuffer<ReadBuffer>
{
public:
    HashingReadBuffer(ReadBuffer & in_, size_t block_size = DBMS_DEFAULT_HASHING_BLOCK_SIZE) :
        in(in_)
    {
        working_buffer = in.buffer();
        pos = in.position();

        /// считаем хэш от уже прочитанных данных
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

        calculateHash(working_buffer.begin(), working_buffer.size());

        return res;
    }

private:
    ReadBuffer & in;
};
}
