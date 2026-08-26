#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobHashingWriteBuffer.h>

#include <IO/BufferWithOwnMemory.h>
#include <IO/HashingReadBuffer.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <Common/Exception.h>
#include <base/hex.h>

#include "config.h"

#if USE_SSL
#    include <Common/OpenSSLHelpers.h>
#    include <openssl/evp.h>
#endif

/// `XXH_INLINE_ALL` renames every public symbol under the `XXH_INLINE_` prefix (`XXH_NAMESPACE`) and
/// makes the whole library a header-only, static-inline implementation local to THIS translation
/// unit -- no link dependency on the separately-compiled `ch_contrib::xxHash` object. This file is
/// part of the `dbms` target (not `clickhouse_functions_obj`, which gets the flag via its own
/// `target_link_libraries(... ch_contrib::xxHash)`), so the macro is defined locally here, same
/// effect, same prefixed names as `Functions/FunctionsHashing.h` uses.
/// xxHash is included through this wrapper (which marks it a system header) to suppress the vendored-C
/// warnings from lz4's shadowing copy under `-Werror -Weverything`. See `CasXxh3Streamer.h`.
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasXxh3Streamer.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int OPENSSL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}
}

namespace DB::Cas
{

namespace
{

/// Thin adapter over the existing `HashingWriteBuffer` so `CityHash128` blob hashes stay
/// byte-identical to today. Bytes written to `*this` alias directly into `hashing`'s own buffer
/// (the same zero-copy trick `HashingWriteBuffer` itself uses against its nested sink), so this
/// adds no extra copy and no change to the chunked `CityHash128WithSeed` chaining.
class CityHash128BlobHashingWriteBuffer : public IBlobHashingWriteBuffer
{
public:
    explicit CityHash128BlobHashingWriteBuffer(WriteBuffer & sink)
        : IBlobHashingWriteBuffer()
        , hashing(sink)
    {
        working_buffer = hashing.buffer();
        pos = working_buffer.begin();
    }

    void sync() override
    {
        hashing.sync();
    }

    String getHashHex() override
    {
        next();
        return getHexUIntLowercase(hashing.getHash());
    }

private:
    void nextImpl() override
    {
        hashing.position() = pos;
        hashing.next();
        working_buffer = hashing.buffer();
    }

    void finalizeImpl() override
    {
        next();
        hashing.finalize();
    }

    void cancelImpl() noexcept override
    {
        hashing.cancel();
    }

    HashingWriteBuffer hashing;
};

/// A hash-and-passthrough buffer over the xxhash library's streaming `XXH3_128bits` state. Unlike
/// `CityHash128`, xxh3's streaming digest is defined to agree with its one-shot digest, so there is
/// no chunked-convention to preserve -- this just needs to feed every byte to the streaming state
/// (`update`) and forward the same bytes to `sink` unchanged.
class Xxh3128BlobHashingWriteBuffer : public BufferWithOwnMemory<IBlobHashingWriteBuffer>
{
public:
    explicit Xxh3128BlobHashingWriteBuffer(WriteBuffer & sink_, size_t buf_size = DBMS_DEFAULT_HASHING_BLOCK_SIZE)
        : BufferWithOwnMemory<IBlobHashingWriteBuffer>(buf_size)
        , sink(sink_)
    {
        if (!state.valid())
            throw Exception(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Xxh3128BlobHashingWriteBuffer: failed to allocate the xxh3 streaming state");
    }

    void sync() override
    {
        sink.sync();
    }

    String getHashHex() override
    {
        next();
        UInt64 low = 0;
        UInt64 high = 0;
        state.digest(low, high);
        return getHexUIntLowercase(UInt128{low, high});
    }

private:
    void nextImpl() override
    {
        const size_t len = offset();
        if (!len)
            return;

        state.update(working_buffer.begin(), len);
        sink.write(working_buffer.begin(), len);
    }

    WriteBuffer & sink;
    Xxh3Streamer state;
};

#if USE_SSL
/// A hash-and-passthrough buffer over OpenSSL's streaming EVP SHA-256 digest. Unlike the 128-bit
/// hashes above, `Sha256` produces a 32-byte digest (64 lowercase hex chars, see `blobHashLenFor`).
/// Every byte written is folded into the running EVP digest (`EVP_DigestUpdate`) AND forwarded
/// unchanged to `sink`, exactly like `Xxh3128BlobHashingWriteBuffer` above -- streaming SHA-256 is
/// defined to agree with the one-shot digest, so there is no chunked-convention to preserve either.
class Sha256BlobHashingWriteBuffer : public BufferWithOwnMemory<IBlobHashingWriteBuffer>
{
public:
    explicit Sha256BlobHashingWriteBuffer(WriteBuffer & sink_, size_t buf_size = DBMS_DEFAULT_HASHING_BLOCK_SIZE)
        : BufferWithOwnMemory<IBlobHashingWriteBuffer>(buf_size)
        , sink(sink_)
        , ctx(EVP_MD_CTX_new(), &EVP_MD_CTX_free)
    {
        if (!ctx)
            throw Exception(ErrorCodes::OPENSSL_ERROR,
                "Sha256BlobHashingWriteBuffer: EVP_MD_CTX_new failed: {}", getOpenSSLErrors());

        if (EVP_DigestInit_ex(ctx.get(), EVP_sha256(), nullptr) != 1)
            throw Exception(ErrorCodes::OPENSSL_ERROR,
                "Sha256BlobHashingWriteBuffer: EVP_DigestInit_ex failed: {}", getOpenSSLErrors());
    }

    void sync() override
    {
        sink.sync();
    }

    String getHashHex() override
    {
        next();

        unsigned char digest[EVP_MAX_MD_SIZE];
        unsigned int digest_len = 0;
        if (EVP_DigestFinal_ex(ctx.get(), digest, &digest_len) != 1)
            throw Exception(ErrorCodes::OPENSSL_ERROR,
                "Sha256BlobHashingWriteBuffer: EVP_DigestFinal_ex failed: {}", getOpenSSLErrors());

        chassert(digest_len == 32);
        return hexString(digest, digest_len);
    }

private:
    using EVP_MD_CTX_ptr = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>;

    void nextImpl() override
    {
        const size_t len = offset();
        if (!len)
            return;

        if (EVP_DigestUpdate(ctx.get(), working_buffer.begin(), len) != 1)
            throw Exception(ErrorCodes::OPENSSL_ERROR,
                "Sha256BlobHashingWriteBuffer: EVP_DigestUpdate failed: {}", getOpenSSLErrors());

        sink.write(working_buffer.begin(), len);
    }

    WriteBuffer & sink;
    EVP_MD_CTX_ptr ctx;
};
#endif

}

std::unique_ptr<IBlobHashingWriteBuffer> makeBlobHashingWriteBuffer(BlobHashAlgo algo, WriteBuffer & sink)
{
    switch (algo)
    {
        case BlobHashAlgo::CityHash128:
            return std::make_unique<CityHash128BlobHashingWriteBuffer>(sink);
        case BlobHashAlgo::XXH3_128:
            return std::make_unique<Xxh3128BlobHashingWriteBuffer>(sink);
        case BlobHashAlgo::Sha256:
#if USE_SSL
            return std::make_unique<Sha256BlobHashingWriteBuffer>(sink);
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "blob_hash = 'sha256' requires ClickHouse built with SSL support");
#endif
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "makeBlobHashingWriteBuffer: unknown BlobHashAlgo {}", static_cast<int>(algo));
}

String blobHashHexOneShot(BlobHashAlgo algo, std::string_view bytes)
{
    switch (algo)
    {
        case BlobHashAlgo::CityHash128:
        {
            /// Preserve the blob content-hash convention used by `poolContentHash`: hash through
            /// `HashingReadBuffer` in `DBMS_DEFAULT_HASHING_BLOCK_SIZE` chunks, chaining
            /// `CityHash128WithSeed`. A one-shot `CityHash128WithSeed` call would produce a different
            /// result for payloads larger than one hash block.
            ReadBufferFromMemory in(bytes.data(), bytes.size());
            HashingReadBuffer hashing(in);
            hashing.ignoreAll();
            return getHexUIntLowercase(hashing.getHash());
        }
        case BlobHashAlgo::XXH3_128:
        {
            UInt64 low = 0;
            UInt64 high = 0;
            xxh3_128_oneshot(bytes.data(), bytes.size(), low, high);
            return getHexUIntLowercase(UInt128{low, high});
        }
        case BlobHashAlgo::Sha256:
        {
#if USE_SSL
            /// One-shot SHA-256 is defined to agree with the streaming EVP digest above (there is no
            /// chunked convention to preserve, unlike `CityHash128`), so this can go straight through
            /// `encodeSHA256`'s one-shot path instead of round-tripping through a streaming buffer.
            unsigned char digest[32];
            encodeSHA256(bytes.data(), bytes.size(), digest);
            return hexString(digest, sizeof(digest));
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                "blob_hash = 'sha256' requires ClickHouse built with SSL support");
#endif
        }
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "blobHashHexOneShot: unknown BlobHashAlgo {}", static_cast<int>(algo));
}

}
