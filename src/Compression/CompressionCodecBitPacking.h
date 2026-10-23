#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

/** Packs the integers of a column in chunks of 64 KiB of them, each as its least integer and the difference of every
  * integer from it in as many bits as the largest difference takes, see `BitPackingFormat`. A codec that prepares the
  * data for a general-purpose one, as `Delta` and `GCD` are, applicable to integers of 1, 2, 4 or 8 bytes: `Int*`,
  * `UInt*`, `Decimal32`, `Decimal64`, `Date`, `Date32`, `DateTime`, `DateTime64`, `Enum*` and `IPv4`. Both the host
  * and a GPU unpack it, so the GPU aggregation reads a column of `CODEC(BitPacking, GDeflate)` as the compressed blocks
  * of the second codec and unpacks what they expand to on the device.
  */
class CompressionCodecBitPacking : public ICompressionCodec
{
public:
    /// `element_size` 0, of a codec made without a column type, unpacks but does not pack.
    CompressionCodecBitPacking(UInt8 element_size_, bool is_signed_);

    uint8_t getMethodByte() const override;
    ASTPtr getCodecDescription() const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }

    String getDescription() const override
    {
        return "Packs integers in chunks of 64 KiB, each as its least value and the differences from it in as few bits as they take. "
               "A GPU unpacks it as well.";
    }

private:
    const UInt8 element_size;
    const bool is_signed;
};

}
