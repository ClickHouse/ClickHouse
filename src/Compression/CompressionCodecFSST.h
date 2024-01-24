#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>

namespace DB
{

class CompressionCodecFSST : public ICompressionCodec
{
public:
    explicit CompressionCodecFSST();

    uint8_t getMethodByte() const override;

    UInt32 getAdditionalSizeAtTheEndOfBuffer() const override;

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }

private:
    // symbol table
};

CompressionCodecFSST::CompressionCodecFSST() {
    setCodecDescription("FSST");
}


