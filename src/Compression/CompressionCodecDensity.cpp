#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionCodecDensity.h>
#include <Parsers/ASTLiteral.h>
#include <Common/ErrorCodes.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

CompressionCodecDensity::CompressionCodecDensity(DENSITY_ALGORITHM algo_) : algo(algo_)
{
    setCodecDescription("Density", {std::make_shared<ASTLiteral>(static_cast<UInt64>(algo))});
}

uint8_t CompressionCodecDensity::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::Density);
}

void CompressionCodecDensity::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash);
}

UInt32 CompressionCodecDensity::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return density_compress_safe_size(uncompressed_size);
}

UInt32 CompressionCodecDensity::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    density_processing_result res = density_compress(reinterpret_cast<const uint8_t *>(source), source_size, reinterpret_cast<uint8_t *>(dest), density_compress_safe_size(source_size), algo);
    if (res.state != DENSITY_STATE_OK)
        throw Exception("Cannot compress block with Density; ", ErrorCodes::CANNOT_COMPRESS);
    return res.bytesWritten;
}

void CompressionCodecDensity::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    density_processing_result res = density_decompress(reinterpret_cast<const uint8_t *>(source), source_size, reinterpret_cast<uint8_t *>(dest), density_decompress_safe_size(uncompressed_size));
    if (res.state != DENSITY_STATE_OK)
        throw Exception("Cannot decompress block with Density; ", ErrorCodes::CANNOT_DECOMPRESS);
}

void registerCodecDensity(CompressionCodecFactory & factory)
{
    UInt8 method_code = UInt8(CompressionMethodByte::Density);
    factory.registerCompressionCodec(
        "Density",
        method_code,
        [&](const ASTPtr & arguments) -> CompressionCodecPtr
        {
            DENSITY_ALGORITHM algo = CompressionCodecDensity::DENSITY_DEFAULT_ALGO;
            //std::cerr << arguments << std::endl;
            if (arguments && !arguments->children.empty())
            {
                if (arguments->children.size() != 1)
                    throw Exception(
                        "Deisnty codec must have 1 parameter, given " + std::to_string(arguments->children.size()),
                        ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE);

                const auto children = arguments->children;

                const auto * algo_literal = children[0]->as<ASTLiteral>();
                if (!algo_literal)
                    throw Exception("Density codec argument must be string (algorithm)", ErrorCodes::ILLEGAL_CODEC_PARAMETER);


                if (algo_literal->value.getType() == Field::Types::Which::UInt64) {
                    const auto algorithm = algo_literal->value.safeGet<UInt64>();
                    if (algorithm == 3) {
                        algo = DENSITY_ALGORITHM_LION;
                    } else if (algorithm == 2) {
                        algo = DENSITY_ALGORITHM_CHEETAH;
                    } else if (algorithm == 1) {
                        algo = DENSITY_ALGORITHM_CHAMELEON;
                    } else {
                        throw Exception("Density codec argument may be LION, CHAMELEON, CHEETAH", ErrorCodes::ILLEGAL_CODEC_PARAMETER);
                    }
                } else {
                    const auto algorithm = algo_literal->value.safeGet<std::string>();
                    if (algorithm == "LION") {
                        algo = DENSITY_ALGORITHM_LION;
                    } else if (algorithm == "CHAMELEON") {
                        algo = DENSITY_ALGORITHM_CHAMELEON;
                    } else if (algorithm == "CHEETAH") {
                        algo = DENSITY_ALGORITHM_CHEETAH;
                    } else {
                        throw Exception("Density codec argument may be LION, CHAMELEON, CHEETAH", ErrorCodes::ILLEGAL_CODEC_PARAMETER);
                    }
                }
            }

            return std::make_shared<CompressionCodecDensity>(algo);
        });
}

}
