#if !defined(ARCADIA_BUILD)
#   include "config_core.h"
#endif

#if USE_DENSITY

#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Parsers/ASTLiteral.h>

#include <src/density_api.h>


namespace DB
{

class CompressionCodecDensity : public ICompressionCodec
{
public:
    explicit CompressionCodecDensity(DENSITY_ALGORITHM algo_);

    uint8_t getMethodByte() const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }
    bool isExperimental() const override { return true; }

private:
    const DENSITY_ALGORITHM algo;
};


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
        throw Exception("Cannot compress block with Density", ErrorCodes::CANNOT_COMPRESS);
    return res.bytesWritten;
}

void CompressionCodecDensity::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    density_processing_result res = density_decompress(reinterpret_cast<const uint8_t *>(source), source_size, reinterpret_cast<uint8_t *>(dest), density_decompress_safe_size(uncompressed_size));
    if (res.state != DENSITY_STATE_OK)
        throw Exception("Cannot decompress block with Density", ErrorCodes::CANNOT_DECOMPRESS);
}

void registerCodecDensity(CompressionCodecFactory & factory)
{
    UInt8 method_code = UInt8(CompressionMethodByte::Density);
    factory.registerCompressionCodec(
        "Density",
        method_code,
        [&](const ASTPtr & arguments) -> CompressionCodecPtr
        {
            DENSITY_ALGORITHM algorithm = DENSITY_ALGORITHM_CHAMELEON;

            if (arguments && !arguments->children.empty())
            {
                if (arguments->children.size() != 1)
                    throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                        "Density codec must have only one parameter, given {}", arguments->children.size());

                const auto children = arguments->children;

                const auto * algo_literal = children[0]->as<ASTLiteral>();
                if (!algo_literal || algo_literal->value.getType() != Field::Types::UInt64)
                    throw Exception("Density codec argument must be integer",
                        ErrorCodes::ILLEGAL_CODEC_PARAMETER);

                const UInt64 algorithm_num = algo_literal->value.safeGet<UInt64>();
                if (algorithm_num < 1 || algorithm_num > 3)
                    throw Exception("Density codec level can be 1, 2 or 3.", ErrorCodes::ILLEGAL_CODEC_PARAMETER);

                algorithm = static_cast<DENSITY_ALGORITHM>(algorithm_num);
            }

            return std::make_shared<CompressionCodecDensity>(algorithm);
        });
}

}

#endif
