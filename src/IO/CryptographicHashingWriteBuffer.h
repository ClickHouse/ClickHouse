#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadHelpers.h>
#include <Core/Types.h>

#define DBMS_DEFAULT_HASHING_BLOCK_SIZE 2048ULL

UInt128 sipHash128(const char*, const size_t);

namespace DB
{
template <typename Buffer>
class ICryptoHashingBuffer : public BufferWithOwnMemory<Buffer> {
public:
    using uint128 = std::pair<uint64_t, uint64_t>;

    explicit ICryptoHashingBuffer(size_t block_size_= DBMS_DEFAULT_HASHING_BLOCK_SIZE)
        : BufferWithOwnMemory<Buffer>(block_size_)
        , block_pos(0)
        , block_size(block_size_)
    {
    }

    uint128 getHash()
    {
        if (block_pos) {
            return applyHashFn(BufferWithOwnMemory<Buffer>::memory.data(), block_pos);
        } else {
           return state;
        }
    }

    void append(DB::BufferBase::Position data)
    {
        state = applyHashFn(data, block_size);
    }

    void calculateHash(DB::BufferBase::Position data, size_t len);
private:
    uint128 applyHashFn(const char* begin, const size_t size) {
        auto hashed = sipHash128(begin, size);
        return {hashed & (~(0llu)), hashed >> 64};
    }
protected:
    size_t block_pos;
    size_t block_size;
    uint128 state;
};

class CryptoHashingWriteBuffer : public ICryptoHashingBuffer<WriteBuffer> {
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
    explicit CryptoHashingWriteBuffer(
        WriteBuffer& out_,
        size_t block_size_ = DBMS_DEFAULT_HASHING_BLOCK_SIZE)
        : ICryptoHashingBuffer<DB::WriteBuffer>(block_size_), out(out_)
    {
        // clear buffer in case there is something
        out.next();
        working_buffer = out.buffer();
        pos = working_buffer.begin();
        state = uint128(0, 0);
    }

    void sync() override
    {
        out.sync();
    }

    uint128 getHash()
    {
        next();
        return ICryptoHashingBuffer<WriteBuffer>::getHash();
    }
};

}
