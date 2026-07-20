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

    /// The pipeline stages, in application order (the generic-compression stage, if any, is among them).
    const Codecs & getCodecs() const { return codecs; }

    void updateHash(SipHash & hash) const override;

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
