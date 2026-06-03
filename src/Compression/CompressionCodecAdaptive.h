#pragma once

#include <Compression/ICompressionCodec.h>
#include <Core/TypeId.h>

#include <vector>

namespace DB
{

class IDataType;


/// Decision logic for adaptive CODEC(Default) resolution
namespace AdaptiveCodec
{

/// Candidate codecs for `type`, in priority order. The deployment default is always element 0, so it wins ties in `select` and bounds the
/// downside to "no worse than the default". Extra candidates come from a per-type table.
std::vector<CompressionCodecPtr> poolForType(const IDataType & type, const CompressionCodecPtr & deployment_default);

/// Pick the codec from `pool` whose compressed block is smallest.
/// TODO: return the winner's compressed bytes alongside the codec so compress() can skip re-compressing when a non-predicting codec wins.
CompressionCodecPtr select(const std::vector<CompressionCodecPtr> & pool, const char * source, UInt32 source_size);

/// The distinct types that can get a non-default codec.
std::vector<TypeIndex> candidateTypeIndexes();

}

/// Adaptive codec picks the smallest-output codec per block from a default codec + type-appropriate pool and delegates to it.
/// On disk, each block carries the winner's method byte and reader decodes it with no knowledge of "adaptive".
/// The wrapper itself never appears on disk.
class CompressionCodecAdaptive final : public ICompressionCodec
{
public:
    CompressionCodecAdaptive(const IDataType & type, const CompressionCodecPtr & deployment_default, UInt32 skip_threshold_);

    uint8_t getMethodByte() const override;
    void updateHash(SipHash & hash) const override;

    /// Selects the best codec for this block and delegates to it. The result carries the winner's method byte.
    UInt32 compress(const char * source, UInt32 source_size, char * dest) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }
    String getDescription() const override { return "Resolve CODEC(Default) to the best per-block codec from a type-appropriate pool."; }

protected:
    /// TODO: add `CODEC(NONE)` as pool[0]. If all codecs expand the block (output larger than input), the selector should store it raw.
    /// `NONE` must sit at position 0 (need to rewrite some logic as we rely on default being at [0] now).
    /// It is the fastest option, requiring no decompression.
    /// Once done, it's guaranteed the winner's output can never exceed the input, so this can simply `return uncompressed_size`.
    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    /// Adaptive never appears on disk: it self-describes each block via the winner's method byte, so these must never be invoked directly.
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

private:
    /// pool[0] is the deployment default
    std::vector<CompressionCodecPtr> pool;
    UInt32 skip_threshold;
};

}
