#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasBlobDigest.h>
#include <IO/WriteBuffer.h>
#include <base/types.h>
#include <memory>
#include <string_view>

namespace DB::Cas
{

/// A streaming hash-and-passthrough `WriteBuffer`: every byte written is forwarded unchanged to the
/// nested sink AND folded into the running digest, exposed as lowercase hex once flushed (32 hex
/// chars for the 128-bit hashes, 64 hex chars for `Sha256` -- see `blobHashLenFor`) (`getHashHex`
/// calls `next()` first, mirroring `HashingWriteBuffer::getHash`). It does not finalize or cancel the
/// underlying sink -- that stays the caller's responsibility, the same contract `CaContentWriteBuffer`
/// already relies on for `HashingWriteBuffer` and its nested sink.
class IBlobHashingWriteBuffer : public WriteBuffer
{
public:
    explicit IBlobHashingWriteBuffer(Position ptr = nullptr, size_t size = 0) : WriteBuffer(ptr, size) {}
    ~IBlobHashingWriteBuffer() override = default;

    /// Flushes any pending bytes (like `HashingWriteBuffer::getHash`) and returns the digest as
    /// lowercase hex (length depends on `algo`, see `blobHashLenFor`). May be called only once useful
    /// data has stopped flowing; does not itself finalize the buffer (mirrors
    /// `HashingWriteBuffer::getHash`, which also does not finalize).
    virtual String getHashHex() = 0;
};

/// Builds a streaming hash-and-passthrough buffer for `algo` over `sink`. Every byte written to the
/// returned buffer is forwarded unchanged to `sink` and included in its digest. `sink` must outlive
/// the returned buffer, as with `HashingWriteBuffer`.
///
/// `CityHash128` is a thin adapter over the existing `HashingWriteBuffer`: it retains the
/// `DBMS_DEFAULT_HASHING_BLOCK_SIZE` chunks and chained `CityHash128WithSeed` convention, so the
/// default algorithm produces byte-identical blob hashes. `XXH3_128` uses the xxhash library's
/// streaming `XXH3_128bits` state, and `Sha256` uses OpenSSL's streaming EVP SHA-256 digest. Both
/// of those algorithms define their streaming digest to agree with their one-shot digest.
std::unique_ptr<IBlobHashingWriteBuffer> makeBlobHashingWriteBuffer(BlobHashAlgo algo, WriteBuffer & sink);

/// Computes the lowercase-hex digest of `bytes` for `algo`. This is used by the re-hash and
/// copy-forward path and by tests. For `CityHash128`, the implementation deliberately uses
/// `HashingReadBuffer` so it follows the same chunked, chained convention as the streaming write
/// path; a one-shot `CityHash128WithSeed` call would diverge for payloads larger than one hash block.
/// The one-shot `XXH3_128bits` and OpenSSL `encodeSHA256` paths agree with their streaming
/// counterparts.
String blobHashHexOneShot(BlobHashAlgo algo, std::string_view bytes);

}
