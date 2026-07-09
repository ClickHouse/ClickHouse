#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

/** Experimental codec implementing the Iguana compression pipeline
  * (https://github.com/SnellerInc/iguana): LZ-style structural compression followed by a 32-way
  * interleaved 8-bit rANS entropy stage applied to each of the six substreams. Blocks (and
  * individual substreams) that do not compress are stored verbatim, so the output never grows by
  * more than a small header.
  */
class CompressionCodecIguana : public ICompressionCodec
{
public:
    CompressionCodecIguana();

    uint8_t getMethodByte() const override;

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }
    bool isExperimental() const override { return true; }

    String getDescription() const override
    {
        return "Experimental LZ + interleaved rANS compression from the Iguana library.";
    }
};

}
