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

    uint8_t getMethodByte() const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    /// zxc decodes literals with 32-byte SIMD "wild" loads that can read up to
    /// ZXC_PAD_SIZE (32) bytes past the end of the compressed input. Reserve slack
    /// at the end of the (de)compression buffers so these over-reads stay inside
    /// allocated memory. This is the same mechanism `LZ4` uses. The over-read
    /// bytes are only ever wild-copied and never influence results; MemorySanitizer
    /// false positives from zxc's SIMD kernels are handled separately by building
    /// zxc scalar under MSan (see contrib/zxc-cmake).
    UInt32 getAdditionalSizeAtTheEndOfBuffer() const override { return 64; }

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }

    String getDescription() const override
    {
        return "Asymmetric LZ codec (zxc): slow compression, very fast decompression, ratio between LZ4 and ZSTD. Levels 1..7.";
    }

private:
    const int level;
};

}
