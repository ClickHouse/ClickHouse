#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

class IDataType;


/// Decision logic for adaptive CODEC(Default) resolution
namespace AdaptiveCodec
{

/// Candidate codecs for `type`, in priority order. [0] is `NONE`: a block that no codec can shrink is stored uncompressed.
/// [1] is the default codec, thus we get "no worse than the default" compression. Extra candidates come from a per-type table.
/// Beyond [0] and [1], candidates must be ordered by descending decompression speed as draw in size should resolve to the fastest reads.
Codecs poolForType(const IDataType & type, const CompressionCodecPtr & deployment_default);

}

/// Adaptive codec picks the smallest-output codec per block from a default codec + type-appropriate pool and delegates to it.
/// On disk, each block carries the winner's method byte and reader decodes it with no knowledge of "adaptive".
/// The wrapper itself never appears on disk.
class CompressionCodecAdaptive final : public ICompressionCodec
{
public:
    CompressionCodecAdaptive(const IDataType & type, const CompressionCodecPtr & deployment_default);

    uint8_t getMethodByte() const override;
    void updateHash(SipHash & hash) const override;

    /// Compresses the block with whichever candidate produces the smallest output. Decompression cannot tell adaptive was involved.
    /// Ties go to the earliest pool entry, so `NONE` beats an equal-sized compressor.
    /// Candidates reporting their size via `tryGetCompressedSize` are compressed only if they win.
    /// Selection cost scales with the block size, so there is no small-block skip.
    UInt32 compress(const char * source, UInt32 source_size, char * dest) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }
    String getDescription() const override { return "Resolve CODEC(Default) to the best per-block codec from a type-appropriate pool."; }

protected:
    /// Max across all codecs in the pool. Exceeds `uncompressed_size` as this reserves the memory codecs need while compressing.
    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    /// Adaptive never appears on disk: it self-describes each block via the winner's method byte, so these must never be invoked directly.
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

private:
    /// pool[0] is NONE, pool[1] is the deployment default
    Codecs pool;
};

}
