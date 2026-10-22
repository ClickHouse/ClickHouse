#include <Compression/CompressionCodecGDeflate.h>

#if USE_GPU

#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/registerCompressionCodecs.h>
#include <GPU/GPUGDeflate.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

CompressionCodecGDeflate::CompressionCodecGDeflate(int algorithm_)
    : algorithm(algorithm_)
{
}

uint8_t CompressionCodecGDeflate::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::GDeflate);
}

ASTPtr CompressionCodecGDeflate::getCodecDescription() const
{
    return makeCodecDescription("GDeflate", {make_intrusive<ASTLiteral>(static_cast<UInt64>(algorithm))});
}

void CompressionCodecGDeflate::updateHash(SipHash & hash) const
{
    getCodecDescription()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecGDeflate::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return static_cast<UInt32>(GPU::gdeflateMaxCompressedBytes(uncompressed_size, algorithm));
}

UInt32 CompressionCodecGDeflate::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    return static_cast<UInt32>(GPU::gdeflateCompress(source, source_size, dest, algorithm));
}

UInt32 CompressionCodecGDeflate::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    GPU::gdeflateDecompress(source, source_size, dest, uncompressed_size);
    return uncompressed_size;
}

void registerCodecGDeflate(CompressionCodecFactory & factory)
{
    const UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::GDeflate);
    factory.registerCompressionCodec("GDeflate", method_code, [&](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        int algorithm = CompressionCodecGDeflate::DEFAULT_ALGORITHM;
        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() != 1)
                throw Exception(
                    ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "GDeflate codec must have 1 parameter, given {}", arguments->children.size());

            const auto * literal = arguments->children[0]->as<ASTLiteral>();
            if (!literal)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "GDeflate codec argument must be integer");

            const UInt64 value = literal->value.safeGet<UInt64>();
            if (value > CompressionCodecGDeflate::MAX_ALGORITHM)
                throw Exception(
                    ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "GDeflate codec algorithm must be between 0 and {}, given {}",
                    CompressionCodecGDeflate::MAX_ALGORITHM,
                    value);
            algorithm = static_cast<int>(value);
        }
        return std::make_shared<CompressionCodecGDeflate>(algorithm);
    });
}

}

#endif
