#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <Common/SipHash.h>

namespace DB
{

/// Feeds everything written to it into a `SipHash` over a fixed-size window and discards the bytes,
/// so a stream that is only ever hash input never has to be held in memory to be hashed.
///
/// `SipHash::update` carries state, including any sub-word remainder, across calls, so hashing a
/// stream in consecutive chunks gives the same 64-bit value as hashing one contiguous buffer holding
/// all of it: the window size is not observable in the result.
class SipHashingWriteBuffer final : public BufferWithOwnMemory<WriteBuffer>
{
public:
    /// Any size is correct: `WriteBuffer` callers either loop over `available()` or check it before
    /// writing into the buffer directly.
    static constexpr size_t window_bytes = 32768;

    explicit SipHashingWriteBuffer(SipHash & hash_) : BufferWithOwnMemory<WriteBuffer>(window_bytes), hash(hash_) { }

    /// This buffer may be destroyed with a stream still half-written: nothing reads the discarded
    /// bytes, so abandoning one costs nothing and callers need not finalize. `~WriteBuffer` asserts
    /// against being left neither finalized nor canceled, so cancel here.
    ~SipHashingWriteBuffer() override { cancel(); }

private:
    void nextImpl() override { hash.update(working_buffer.begin(), offset()); }

    SipHash & hash;
};

}
