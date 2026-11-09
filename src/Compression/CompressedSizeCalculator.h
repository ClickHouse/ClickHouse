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
class CompressedSizeCalculator : public BufferWithOwnMemory<WriteBuffer>
{
public:
    /// `buf_size` is the size of a block. With a smaller `initial_size`, the buffer starts that small and grows with the
    /// open block up to `buf_size`, so that a calculator of a few rows does not hold a whole block.
    explicit CompressedSizeCalculator(
        CompressionCodecPtr codec_ = nullptr, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE, size_t initial_size = 0);

    /// Compresses the open block whatever its size, as `next` ends a block of `CompressedWriteBuffer`. On a growing buffer
    /// `next` cannot tell a block that ends from a write that needs room.
    void endBlock();

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
    void finalizeImpl() override { endBlock(); }

    CompressionCodecPtr codec;
    const size_t block_size;
    bool ending_block = false;
    UInt64 compressed_total = 0;
};

}
