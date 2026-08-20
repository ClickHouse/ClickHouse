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
    explicit CompressedSizeCalculator(CompressionCodecPtr codec_ = nullptr, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

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

    CompressionCodecPtr codec;
    PODArray<char> scratch;
    UInt64 compressed_total = 0;
};

}
