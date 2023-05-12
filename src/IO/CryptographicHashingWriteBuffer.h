#pragma once

#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadHelpers.h>
#include <Core/Types.h>
#include <Core/SettingsEnums.h>
#include <Common/SipHash.h>

#if USE_SSL
#    include <openssl/md4.h>
#    include <openssl/md5.h>
#    include <openssl/sha.h>
#endif

#define DBMS_DEFAULT_HASHING_BLOCK_SIZE 2048ULL
typedef std::pair<uint64_t, uint64_t> uint128;
typedef uint128 (*HashFnApplier) (const char*, const size_t);

namespace DB
{

inline uint128 SipHashApplier(const char* begin, const size_t size) {
    auto hashed = sipHash128(begin, size);
    return {hashed & (~(0llu)), hashed >> 64};
}

#if USE_SSL
inline uint128 SHA256Applier(const char* begin, const size_t size) {
    uint64_t buf[4];

    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
    SHA256_Final(reinterpret_cast<unsigned char*>(&buf), &ctx);

    return {buf[0], buf[1]};
}

inline uint128 MD4Applier(const char* begin, const size_t size) {
    uint64_t buf[4];

    MD4_CTX ctx;
    MD4_Init(&ctx);
    MD4_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
    MD4_Final(reinterpret_cast<unsigned char*>(&buf), &ctx);

    return {buf[0], buf[1]};
}

inline uint128 MD5Applier(const char* begin, const size_t size) {
    uint64_t buf[4];

    MD5_CTX ctx;
    MD5_Init(&ctx);
    MD5_Update(&ctx, reinterpret_cast<const unsigned char *>(begin), size);
    MD5_Final(reinterpret_cast<unsigned char*>(&buf), &ctx);

    return {buf[0], buf[1]};
}
#endif

inline HashFnApplier chooseHashFunction(HashFn hashFnType) {
        switch (hashFnType)
        {
            case HashFn::SipHash:
                return &SipHashApplier;
#if USE_SSL
            case HashFn::MD4:
                return &MD4Applier;
            case HashFn::MD5:
                return &MD5Applier;
            case HashFn::SHA256:
                return &SHA256Applier;
#endif
            default:
                return &SipHashApplier;
        }
}

template <typename Buffer>
class ICryptoHashingBuffer : public BufferWithOwnMemory<Buffer> {
public:
    explicit ICryptoHashingBuffer(HashFnApplier hasher_, size_t block_size_= DBMS_DEFAULT_HASHING_BLOCK_SIZE)
        : BufferWithOwnMemory<Buffer>(block_size_)
        , block_pos(0)
        , block_size(block_size_)
        , hasher(hasher_)
    {
    }

    uint128 getHash()
    {
        if (block_pos) {
            return hasher(BufferWithOwnMemory<Buffer>::memory.data(), block_pos);
        } else {
           return state;
        }
    }

    void append(DB::BufferBase::Position data)
    {
        state = hasher(data, block_size);
    }

    void calculateHash(DB::BufferBase::Position data, size_t len);

protected:
    size_t block_pos;
    size_t block_size;
    uint128 state;

    HashFnApplier hasher;
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
        HashFnApplier hasher_,
        size_t block_size_ = DBMS_DEFAULT_HASHING_BLOCK_SIZE)
        : ICryptoHashingBuffer<DB::WriteBuffer>(hasher_, block_size_), out(out_)
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
