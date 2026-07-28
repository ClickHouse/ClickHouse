#pragma once

#include <Compression/ICompressionCodec.h>
#include <Core/TypeId.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

class IDataType;


/// Decision logic for adaptive CODEC(Default) resolution
namespace AdaptiveCodec
{

/// Candidate codecs for `type`, in priority order. [0] is `NONE`: a block that no codec can shrink is stored uncompressed.
/// [1] is the default codec, thus we get "no worse than the default" compression. Extra candidates come from a per-type table.
Codecs poolForType(const IDataType & type, const CompressionCodecPtr & deployment_default);

/// Pick the codec from `pool` whose compressed block is smallest.
/// TODO: return the winner's compressed bytes alongside the codec so compress() can skip re-compressing when a codec that can't report its size cheaply wins.
CompressionCodecPtr select(const Codecs & pool, const char * source, UInt32 source_size);

/// The distinct types that can get a non-default codec.
VectorWithMemoryTracking<TypeIndex> candidateTypeIndexes();

/// Whether `type` has a candidate beyond `NONE` and the default. Only such types are wrapped: for the rest, selection would compress
/// the default twice (once to measure, once to write) and could at best store a block raw, not worth the cost.
/// TODO: once we save the compression result and reuse it, wrapping is free, wrap every type.
bool isCandidateType(const IDataType & type);

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

    /// Selects the best codec for this block and delegates to it. The result carries the winner's method byte.
    /// Runs on every block regardless of size. Selection cost scales with the block, so there is no small-block skip.
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
