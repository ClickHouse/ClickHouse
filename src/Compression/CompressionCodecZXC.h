#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

/// zxc (https://github.com/hellobertrand/zxc) is an asymmetric LZ codec: slow to
/// compress, but very fast to decompress at a ratio between LZ4 and ZSTD. It is a
/// good fit for write-once/read-many analytical columns.
class CompressionCodecZXC : public ICompressionCodec
{
public:
    static constexpr int ZXC_DEFAULT_LEVEL = 3;

    explicit CompressionCodecZXC(int level_);

    [[nodiscard]] uint8_t getMethodByte() const override;
    [[nodiscard]] ASTPtr getCodecDescription() const override;

    [[nodiscard]] UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    /// Room for zxc's wild copies past the decompressed data and its 32-byte over-reads past the compressed input,
    /// as for `LZ4`. MemorySanitizer false positives from zxc's SIMD kernels are handled by building zxc scalar under
    /// MSan (see contrib/zxc-cmake).
    [[nodiscard]] UInt32 getAdditionalSizeAtTheEndOfBuffer() const override;

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    [[nodiscard]] bool isCompression() const override { return true; }
    [[nodiscard]] bool isGenericCompression() const override { return true; }

    [[nodiscard]] String getDescription() const override
    {
        return "Asymmetric LZ codec (zxc): slow compression, very fast decompression, ratio between LZ4 and ZSTD. Levels 1..7.";
    }

private:
    const int level;
};

}
