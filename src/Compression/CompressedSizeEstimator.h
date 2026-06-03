#pragma once

#include <Common/PODArray.h>

#include <Compression/ICompressionCodec.h>
#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>


namespace DB
{

/// A write buffer that reports the on-disk size its input would compress to, without writing anything.
/// Uses `codec->tryGetCompressedSize` when codec can cheaply calculate size.
/// Otherwise compresses into a scratch buffer, discarding the output.
class CompressedSizeEstimator : public BufferWithOwnMemory<WriteBuffer>
{
public:
    explicit CompressedSizeEstimator(CompressionCodecPtr codec_ = nullptr, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    /// Compressed size of a single block as `compress` would produce it: framework header + codec payload.
    static UInt32 getCompressedBlockSize(const ICompressionCodec & codec, const char * src, UInt32 src_size, PODArray<char> & scratch);

    /// Total on-disk size of everything streamed in so far (not a single block).
    /// Per block it adds the 16-byte checksum on top of `getCompressedBlockSize` (per block: checksum + header + payload).
    UInt64 getCompressedBytes()
    {
        nextIfAtEnd();
        return compressed_total;
    }

    /// Size of the uncompressed input passed in via this buffer's `write`/streaming interface.
    UInt64 getUncompressedBytes() { return count(); }

private:
    void nextImpl() override;

    CompressionCodecPtr codec;
    PODArray<char> scratch;
    UInt64 compressed_total = 0;
};

}
