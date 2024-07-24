#ifdef ENABLE_SZ3

#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/BufferWithOwnMemory.h>

#include <SZ3/api/sz.hpp>

namespace DB
{

class CompressionCodecSZ3 : public ICompressionCodec
{
public:
    explicit CompressionCodecSZ3();

    uint8_t getMethodByte() const override;

    UInt32 getAdditionalSizeAtTheEndOfBuffer() const override { return 0; }

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }
    bool isExperimental() const override { return true; }

private:
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;
};

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

CompressionCodecSZ3::CompressionCodecSZ3()
{
    setCodecDescription("SZ3");
}

uint8_t CompressionCodecSZ3::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::SZ3);
}

void CompressionCodecSZ3::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, true);
}

UInt32 CompressionCodecSZ3::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return uncompressed_size;
}

UInt32 CompressionCodecSZ3::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    SZ3::Config conf(1);
    size_t src_size = static_cast<size_t>(source_size);
    auto* res = SZ_compress(conf, source, src_size);
    size_t i = 0;
    for (; res[i] != 0; ++i)
    {
        dest[i] = res[i];
    }
    return static_cast<UInt32>(i);
}

void CompressionCodecSZ3::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32) const 
{
    SZ3::Config conf;
    char* copy_compressed = new char(source_size);
    memcpy(copy_compressed, source, source_size);
    SZ_decompress(conf, copy_compressed, source_size, dest);
    delete copy_compressed;
}

void registerCodecSZ3(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::SZ3);
    factory.registerCompressionCodec("SZ3", method_code, [&](const ASTPtr &) -> CompressionCodecPtr {
        return std::make_shared<CompressionCodecSZ3>();
    });
}

}
#endif // ENABLE_SZ3
