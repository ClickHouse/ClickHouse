#include <Compression/CompressionCodecNone.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Parsers/IAST.h>

namespace DB
{

CompressionCodecNone::CompressionCodecNone()
{
    setCodecDescription("NONE");
}

uint8_t CompressionCodecNone::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::NONE);
}

void CompressionCodecNone::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecNone::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    memcpy(dest, source, source_size);
    return source_size;
}

void CompressionCodecNone::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size != uncompressed_size)
        throw Exception(decompression_error_code, "Wrong data for compression codec NONE: source_size ({}) != uncompressed_size ({})",
            source_size, uncompressed_size);

    memcpy(dest, source, uncompressed_size);
}

void registerCodecNone(CompressionCodecFactory & factory)
{
    factory.registerSimpleCompressionCodec("NONE", static_cast<char>(CompressionMethodByte::NONE), [&] ()
    {
        return std::make_shared<CompressionCodecNone>();
    });
}

}
