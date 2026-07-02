#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

class CompressionCodecMultiple final : public ICompressionCodec
{
public:
    CompressionCodecMultiple() = default;   /// Need for CompressionFactory to register codec by method byte.
    explicit CompressionCodecMultiple(Codecs codecs_);

    uint8_t getMethodByte() const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    static VectorWithMemoryTracking<uint8_t> getCodecsBytesFromData(const char * source);

    void updateHash(SipHash & hash) const override;

    /// Propagate these properties from the wrapped codecs so that callers inspecting only the
    /// outer `CompressionCodecMultiple` (e.g. the untyped compression settings validation in
    /// `MergeTreeSettings`) still see an experimental or column-type-requiring child in a chain
    /// such as `PCO, ZSTD(1)`.
    bool isExperimental() const override;
    bool requiresColumnTypeToCompress() const override;

protected:

    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 decompressed_size) const override;

    bool isCompression() const override;
    bool isGenericCompression() const override { return false; }

    String getDescription() const override { return "Apply multiple codecs consecutively defined by user."; }

private:
    Codecs codecs;
};

}
