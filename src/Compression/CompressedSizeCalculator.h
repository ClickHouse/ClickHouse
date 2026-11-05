#pragma once

#include <Compression/ICompressionCodec.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <base/defines.h>
#include <Common/PODArray.h>


namespace DB
{

/// A write buffer that reports the on-disk size its input would compress to, without writing anything.
/// Uses `codec->tryGetCompressedSize` when codec can cheaply calculate size.
/// Otherwise compresses into a scratch buffer, discarding the output.
/// The buffer starts small and grows up to `block_size` as the input arrives, so a stream that sees little data costs little memory.
/// A block ends when it reaches `block_size`, at `finalize`, and at an explicit `next`, as on the stream of the MergeTree writer, a
/// `HashingWriteBuffer` that passes `next` to its `CompressedWriteBuffer`. An explicit `next` that finds the buffer exactly full is
/// taken for a write that did not fit.
class CompressedSizeCalculator : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit CompressedSizeCalculator(CompressionCodecPtr codec_ = nullptr, size_t block_size_ = DBMS_DEFAULT_BUFFER_SIZE);

    /// Total on-disk size of the streamed input (not a single block). Call `finalize` first.
    /// Per block: 16-byte checksum + block header + compressed payload.
    UInt64 getCompressedBytes() const
    {
        chassert(isFinalized());
        return compressed_total;
    }

    /// Size of the uncompressed input passed in via this buffer's `write`/streaming interface.
    UInt64 getUncompressedBytes() const { return count(); }

private:
    void nextImpl() override;
    void finalizeImpl() override;
    void compressBlock(size_t size);

    CompressionCodecPtr codec;
    const size_t block_size;
    /// Set while the last, partial block is compressed by `finalize`.
    bool finalizing = false;
    PODArray<char> scratch;
    UInt64 compressed_total = 0;
};

}
