#include <Compression/CompressionCodecZXC.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Compression/registerCompressionCodecs.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>

#include <zxc.h>

#include <cstring>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

uint8_t CompressionCodecZXC::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::ZXC);
}

void CompressionCodecZXC::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecZXC::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return static_cast<UInt32>(zxc_compress_bound(uncompressed_size));
}

UInt32 CompressionCodecZXC::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    zxc_compress_opts_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.level = level;
    /// ClickHouse checksums every compressed block itself, so zxc's own checksum
    /// would be redundant work on the hot decompression path.
    opts.checksum_enabled = 0;

    int64_t compressed_size = zxc_compress(source, source_size, dest, zxc_compress_bound(source_size), &opts);
    if (compressed_size < 0)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with ZXC codec: error code {}", compressed_size);

    return static_cast<UInt32>(compressed_size);
}

UInt32 CompressionCodecZXC::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    zxc_decompress_opts_t opts;
    memset(&opts, 0, sizeof(opts));
    opts.checksum_enabled = 0;

    int64_t decompressed_size = zxc_decompress(source, source_size, dest, uncompressed_size, &opts);
    if (decompressed_size < 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress ZXC-encoded data: error code {}", decompressed_size);
    if (static_cast<UInt32>(decompressed_size) != uncompressed_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ZXC-encoded data: decompressed size {} does not match the expected size {}",
            decompressed_size, uncompressed_size);

    return static_cast<UInt32>(decompressed_size);
}

CompressionCodecZXC::CompressionCodecZXC(int level_)
    : level(level_)
{
    ASTs arguments;
    arguments.push_back(make_intrusive<ASTLiteral>(static_cast<UInt64>(level)));
    setCodecDescription("ZXC", arguments);
}

void registerCodecZXC(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::ZXC);
    factory.registerCompressionCodec("ZXC", method_code, [&](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        int level = CompressionCodecZXC::ZXC_DEFAULT_LEVEL;
        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() != 1)
                throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "ZXC codec must have 1 parameter, given {}",
                    arguments->children.size());

            const auto * literal = arguments->children[0]->as<ASTLiteral>();
            if (!literal)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "ZXC codec argument must be integer");

            level = static_cast<int>(literal->value.safeGet<UInt64>());
            if (level < zxc_min_level() || level > zxc_max_level())
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "ZXC codec level must be between {} and {}, given {}",
                    zxc_min_level(), zxc_max_level(), level);
        }
        return std::make_shared<CompressionCodecZXC>(level);
    });
}

}
