#pragma once

#include <optional>
#include <span>
#include <Compression/ICompressionCodec.h>

namespace DB
{

class CompressionCodecMultiple final : public ICompressionCodec
{
public:
    CompressionCodecMultiple() = default; /// Need for CompressionFactory to register codec by method byte.
    explicit CompressionCodecMultiple(Codecs codecs_);

    uint8_t getMethodByte() const override;
    ASTPtr getCodecDescription() const override;
    ASTPtr getFullCodecDescription() const;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    /// The complete block `compress(source, source_size, dest)` writes, given the source with `completed_stages` stages already applied.
    UInt32 compressRemainingStages(size_t completed_stages, const char * input, UInt32 input_size, UInt32 source_size, char * dest) const;

    static VectorWithMemoryTracking<uint8_t> getCodecsBytesFromData(const char * source);

    /// The pipeline stages, in application order (the generic-compression stage, if any, is among them).
    std::span<const CompressionCodecPtr> getCodecs() const { return codecs ? *codecs : std::span<const CompressionCodecPtr>{}; }

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 decompressed_size) const override;

    bool isCompression() const override;
    bool isGenericCompression() const override { return false; }
    bool isEncryption() const override;
    bool isLossyCompression() const override;

    String getDescription() const override { return "Apply multiple codecs consecutively defined by user."; }

private:
    /// Writes what follows the 9-byte block header: the codec list (count + method bytes), then `input` compressed by all stages after `completed_stages`.
    UInt32 compressBody(size_t completed_stages, const char * input, UInt32 input_size, UInt32 dest_size, char * dest) const;

    std::optional<Codecs> codecs;
};

}
