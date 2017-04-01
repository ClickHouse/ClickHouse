#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadHelpers.h>

#define DBMS_DEFAULT_HASHING_BLOCK_SIZE 2048ULL


namespace DB
{

template <class Buffer>
class IHashingBuffer : public BufferWithOwnMemory<Buffer>
{
public:
    IHashingBuffer<Buffer>(size_t block_size_ = DBMS_DEFAULT_HASHING_BLOCK_SIZE)
        : BufferWithOwnMemory<Buffer>(block_size_), block_pos(0), block_size(block_size_), state(0, 0)
    {
    }

    uint128 getHash()
    {
        if (block_pos)
            return CityHash128WithSeed(&BufferWithOwnMemory<Buffer>::memory[0], block_pos, state);
        else
            return state;
    }

    void append(DB::BufferBase::Position data)
    {
        state = CityHash128WithSeed(data, block_size, state);
    }

    /// вычисление хэша зависит от разбиения по блокам
    /// поэтому нужно вычислить хэш от n полных кусочков и одного неполного
    void calculateHash(DB::BufferBase::Position data, size_t len);

protected:
    size_t block_pos;
    size_t block_size;
    uint128 state;
};

/** Вычисляет хеш от записываемых данных и передает их в указанный WriteBuffer.
  * В качестве основного буфера используется буфер вложенного WriteBuffer.
  */
class HashingWriteBuffer : public IHashingBuffer<WriteBuffer>
{
private:
    WriteBuffer & out;

    void nextImpl() override
    {
        size_t len = offset();

        Position data = working_buffer.begin();
        calculateHash(data, len);

        out.position() = pos;
        out.next();
        working_buffer = out.buffer();
    }

public:
    HashingWriteBuffer(
        WriteBuffer & out_,
        size_t block_size_ = DBMS_DEFAULT_HASHING_BLOCK_SIZE)
        : IHashingBuffer<DB::WriteBuffer>(block_size_), out(out_)
    {
        out.next(); /// Если до нас в out что-то уже писали, не дадим остаткам этих данных повлиять на хеш.
        working_buffer = out.buffer();
        pos = working_buffer.begin();
        state = uint128(0, 0);
    }

    uint128 getHash()
    {
        next();
        return IHashingBuffer<WriteBuffer>::getHash();
    }
};
}


std::string uint128ToString(uint128 data);

std::ostream & operator<<(std::ostream & os, const uint128 & data);
std::istream & operator>>(std::istream & is, uint128 & data);
