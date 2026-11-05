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
///
/// The buffer starts small and grows up to `block_size` as the input arrives, so a stream that sees little data costs little
/// memory. A block ends when it reaches `block_size`, at `endBlock`, at `finalize`, and at an explicit `next` from the
/// serialization, as on the stream of the MergeTree writer, a `HashingWriteBuffer` that passes `next` to its
/// `CompressedWriteBuffer`. An explicit `next` that finds the buffer exactly full is taken for a write that did not fit.
/// After a grow the working buffer begins behind the bytes kept, so `offset()` is not the size of the open block: `blockBytes` is.
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

    /// The bytes of the open block, not compressed yet.
    size_t blockBytes() const { return (working_buffer.begin() - memory.data()) + offset(); }

    /// Ends the open block, as the writer does at a granule boundary once the block holds `min_compress_block_size` bytes.
    void endBlock();

private:
    void nextImpl() override;
    void finalizeImpl() override;
    void compressBlock(size_t size);

    CompressionCodecPtr codec;
    const size_t block_size;
    /// Set while a block is ended on purpose, so that `nextImpl` compresses instead of growing.
    bool closing = false;
    PODArray<char> scratch;
    UInt64 compressed_total = 0;
};

}
