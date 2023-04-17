#pragma once

#include <IO/HashingWriteBuffer.h>
#include <IO/CryptographicHashingWriteBuffer.h>
#include <IO/WriteBuffer.h>
#include <Core/Types.h>

#include <optional>

namespace DB
{
class AbstractHashingWriteBuffer
{
public:
    using uint128 = std::pair<uint64_t, uint64_t>;

    AbstractHashingWriteBuffer(WriteBuffer & out_, bool cryptographic_mode_)
        : cryptographic_mode(cryptographic_mode_)
    {
        if (cryptographic_mode) {
            cryptoBuf.emplace(out_);
        } else {
            hashingBuf.emplace(out_);
        }
    }

    void sync()
    {
        if (cryptographic_mode)
        {
            cryptoBuf->sync();
        }
        else
        {
            hashingBuf->sync();
        }
    }

    uint128 getHash()
    {
        if (cryptographic_mode)
        {
            return cryptoBuf->getHash();
        }
        else
        {
            return hashingBuf->getHash();
        }
    }

    void append(DB::BufferBase::Position data)
    {
        if (cryptographic_mode)
        {
            cryptoBuf->append(data);
        }
        else
        {
            hashingBuf->append(data);
        }
    }

    void calculateHash(DB::BufferBase::Position data, size_t len)
    {
        if (cryptographic_mode)
        {
            cryptoBuf->calculateHash(data, len);
        }
        else
        {
            hashingBuf->calculateHash(data, len);
        }
    }

    WriteBuffer& getBuf() {
        if (cryptographic_mode)
        {
            return *cryptoBuf;
        }
        else
        {
            return *hashingBuf;
        }
    }

private:
    bool cryptographic_mode;
    std::optional<HashingWriteBuffer> hashingBuf;
    std::optional<CryptoHashingWriteBuffer> cryptoBuf;
};
}
