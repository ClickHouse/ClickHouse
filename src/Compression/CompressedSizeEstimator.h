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

    /// Size that would be written to disk if this were a real `CompressedWriteBuffer`.
    /// Per block: checksum + framework header + codec payload.
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
