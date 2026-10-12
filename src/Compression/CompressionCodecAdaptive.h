#pragma once

#include <Compression/ICompressionCodec.h>

#include <Compression/ICodecSelector.h>
#include <DataTypes/IDataType_fwd.h>

namespace DB
{

class CompressionCodecMultiple;

namespace AdaptiveCodec
{

/// A pool entry. `chain` is `codec` followed by the deployment default, or null when that default is not a general-purpose compression.
struct Entry
{
    CompressionCodecPtr codec;
    std::shared_ptr<const CompressionCodecMultiple> chain = nullptr;
};

using Pool = VectorWithMemoryTracking<Entry>;

/// The pool for `type`, in priority order. [0] is the default codec.
/// Specialized candidates come from a per-type table. Beyond [0], they must be ordered by descending decompression speed
/// as a draw in size should resolve to the fastest reads. `NONE` is not a candidate as the raw block is what every candidate must beat.
Pool poolForType(const DataTypePtr & type, const CompressionCodecPtr & deployment_default);

}

/// Adaptive codec picks per block the smallest output among the pool candidates its selector runs.
/// On disk, each block carries the winner's method byte and reader decodes it with no knowledge of "adaptive".
/// The wrapper itself never appears on disk.
class CompressionCodecAdaptive final : public ICompressionCodec
{
public:
    CompressionCodecAdaptive(
        const DataTypePtr & type,
        const CompressionCodecPtr & deployment_default,
        std::unique_ptr<const AdaptiveCodec::ICodecSelector> selector_);

    uint8_t getMethodByte() const override;
    ASTPtr getCodecDescription() const override;
    void updateHash(SipHash & hash) const override;

    /// Compresses the block with the candidates the selector picks. After a run the selector sees the sizes and may pick more candidates.
    UInt32 compress(const char * source, UInt32 source_size, char * dest, CompressionState * state) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }
    String getDescription() const override { return "Resolve CODEC(Default) to the best per-block codec from a type-appropriate pool."; }
    CompressionStatePtr createCompressionState() const override;

protected:
    /// Max across `NONE` and every codec and chain in the pool. Exceeds `uncompressed_size` as this reserves the memory codecs need while compressing.
    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    /// Adaptive never appears on disk: it self-describes each block via the winner's method byte, so these must never be invoked directly.
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

private:
    AdaptiveCodec::Pool pool;
    std::unique_ptr<const AdaptiveCodec::ICodecSelector> selector;
};

}
