#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadHelpers.h>
#include <city.h>

#define DBMS_DEFAULT_HASHING_BLOCK_SIZE 2048ULL


namespace DB
{

template <typename Buffer>
class IHashingBuffer : public BufferWithOwnMemory<Buffer>
{
public:
    using uint128 = CityHash_v1_0_2::uint128;

    explicit IHashingBuffer(size_t block_size_ = DBMS_DEFAULT_HASHING_BLOCK_SIZE)
        : BufferWithOwnMemory<Buffer>(block_size_), block_pos(0), block_size(block_size_), state(0, 0)
    {
    }

    uint128 getHash()
    {
        if (block_pos)
            return CityHash_v1_0_2::CityHash128WithSeed(&BufferWithOwnMemory<Buffer>::memory[0], block_pos, state);
        else
            return state;
    }

    void append(DB::BufferBase::Position data)
    {
        state = CityHash_v1_0_2::CityHash128WithSeed(data, block_size, state);
    }

    /// computation of the hash depends on the partitioning of blocks
    /// so you need to compute a hash of n complete pieces and one incomplete
    void calculateHash(DB::BufferBase::Position data, size_t len);

protected:
    size_t block_pos;
    size_t block_size;
    uint128 state;
};

/** Computes the hash from the data to write and passes it to the specified WriteBuffer.
  * The buffer of the nested WriteBuffer is used as the main buffer.
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
    explicit HashingWriteBuffer(
        WriteBuffer & out_,
        size_t block_size_ = DBMS_DEFAULT_HASHING_BLOCK_SIZE)
        : IHashingBuffer<DB::WriteBuffer>(block_size_), out(out_)
    {
        out.next(); /// If something has already been written to `out` before us, we will not let the remains of this data affect the hash.
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
