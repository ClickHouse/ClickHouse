#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Parsers/ASTLiteral.h>
#include <lib/lizard_compress.h>
#include <lib/lizard_decompress.h>


namespace DB
{
class CompressionCodecLizard : public ICompressionCodec
{
public:
    static constexpr auto LIZARD_DEFAULT_LEVEL = 1;

    CompressionCodecLizard(int level_);

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
    const int level;
};


namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

CompressionCodecLizard::CompressionCodecLizard(int level_) : level(level_)
{
    setCodecDescription("Lizard", {std::make_shared<ASTLiteral>(static_cast<UInt64>(level))});
}

uint8_t CompressionCodecLizard::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::Lizard);
}

void CompressionCodecLizard::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash);
}

UInt32 CompressionCodecLizard::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return Lizard_compressBound(uncompressed_size);
}

UInt32 CompressionCodecLizard::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    int res = Lizard_compress(source, dest, source_size, Lizard_compressBound(source_size), level);

    if (res == 0)
        throw Exception("Cannot compress block with Lizard", ErrorCodes::CANNOT_COMPRESS);
    return res;
}

void CompressionCodecLizard::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    int res = Lizard_decompress_safe(source, dest, source_size, uncompressed_size);

    if (res < 0)
        throw Exception("Cannot compress block with Lizard", ErrorCodes::CANNOT_DECOMPRESS);
}

void registerCodecLizard(CompressionCodecFactory & factory)
{
    UInt8 method_code = UInt8(CompressionMethodByte::Lizard);
    factory.registerCompressionCodec(
        "Lizard",
        method_code,
        [&](const ASTPtr & arguments) -> CompressionCodecPtr
        {
            int level = CompressionCodecLizard::LIZARD_DEFAULT_LEVEL;
            if (arguments && !arguments->children.empty())
            {
                if (arguments->children.size() > 1)
                    throw Exception(
                        "Lizard codec must have 1 parameter, given " + std::to_string(arguments->children.size()),
                        ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE);

                const auto children = arguments->children;
                const auto * literal = children[0]->as<ASTLiteral>();
                if (!literal)
                    throw Exception("Lizard codec argument must be integer", ErrorCodes::ILLEGAL_CODEC_PARAMETER);

                level = literal->value.safeGet<UInt64>();
                // compression level will be truncated to LIZARD_MAX_CLEVEL if it is greater and to LIZARD_MIN_CLEVEL if it is less
                //if (level > 1)//ZSTD_maxCLevel())
                //    throw Exception("Lizard codec can't have level more that " + toString(1/*ZSTD_maxCLevel()*/) + ", given " + toString(level), ErrorCodes::ILLEGAL_CODEC_PARAMETER);
            }

            return std::make_shared<CompressionCodecLizard>(level);
        });
}

}
