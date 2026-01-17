#include <Compression/CompressionCodecOpenZL.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Common/SipHash.h>

#include <openzl/zl_compress.h>
#include <openzl/zl_decompress.h>
#include <openzl/zl_errors.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

CompressionCodecOpenZL::CompressionCodecOpenZL(const String & profile_)
    : profile(profile_)
{
    setCodecDescription("OpenZL", {std::make_shared<ASTLiteral>(profile)});
}

uint8_t CompressionCodecOpenZL::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::OpenZL);
}

void CompressionCodecOpenZL::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecOpenZL::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return static_cast<UInt32>(ZL_compressBound(uncompressed_size));
}

UInt32 CompressionCodecOpenZL::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    /// TODO: Use 'profile' parameter when OpenZL CGraph profile API is integrated
    ZL_CCtx * cctx = ZL_CCtx_create();
    if (!cctx)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Failed to create OpenZL compression context");

    ZL_Report report = ZL_CCtx_compress(
        cctx,
        dest, ZL_compressBound(source_size),
        source, source_size);

    ZL_CCtx_free(cctx);

    if (ZL_isError(report))
        throw Exception(ErrorCodes::CANNOT_COMPRESS,
            "OpenZL compression failed: {}", ZL_ErrorCode_toString(ZL_errorCode(report)));

    return static_cast<UInt32>(ZL_validResult(report));
}

UInt32 CompressionCodecOpenZL::doDecompressData(
    const char * source, UInt32 source_size,
    char * dest, UInt32 uncompressed_size) const
{
    ZL_Report report = ZL_decompress(
        dest, uncompressed_size,
        source, source_size);

    if (ZL_isError(report))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
            "OpenZL decompression failed: {}", ZL_ErrorCode_toString(ZL_errorCode(report)));

    return static_cast<UInt32>(ZL_validResult(report));
}

void registerCodecOpenZL(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::OpenZL);

    auto codec_builder = [](const ASTPtr & arguments, const IDataType * /* column_type */) -> CompressionCodecPtr
    {
        String profile = "generic";

        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() > 1)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "OpenZL codec accepts at most 1 parameter (profile), got {}",
                    arguments->children.size());

            const auto * literal = arguments->children[0]->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::Which::String)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "OpenZL codec parameter must be a string (profile name)");

            profile = literal->value.safeGet<String>();
        }

        return std::make_shared<CompressionCodecOpenZL>(profile);
    };

    factory.registerCompressionCodecWithType("OpenZL", method_code, codec_builder);
}

}
