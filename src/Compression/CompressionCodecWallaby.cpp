#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/Wallaby.h>
#include <Compression/registerCompressionCodecs.h>
#include <Common/SipHash.h>
#include <DataTypes/IDataType.h>
#include <Parsers/IAST.h>

#include <limits>
#include <memory>

namespace DB
{

/** Wallaby codec: lossless compression of `Float32` and `Float64` columns.
  *
  * The values of a block are turned into integers (bit patterns, or the numerators of a common denominator such as
  * a power of ten), predicted from the previous ones, and the residuals are entropy-coded with an alphabet fitted
  * to the block. The format and the algorithm are described in Wallaby.cpp.
  */
class CompressionCodecWallaby final : public ICompressionCodec
{
public:
    explicit CompressionCodecWallaby(UInt8 float_width_);

    [[nodiscard]] uint8_t getMethodByte() const override;
    [[nodiscard]] ASTPtr getCodecDescription() const override;
    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;
    [[nodiscard]] UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    [[nodiscard]] bool isCompression() const override { return true; }
    [[nodiscard]] bool isGenericCompression() const override { return false; }
    [[nodiscard]] bool isFloatingPointTimeSeriesCodec() const override { return true; }
    [[nodiscard]] String getDescription() const override;

private:
    UInt8 float_width;
};

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
}

CompressionCodecWallaby::CompressionCodecWallaby(UInt8 float_width_)
    : float_width(float_width_)
{
}

ASTPtr CompressionCodecWallaby::getCodecDescription() const
{
    return makeCodecDescription("Wallaby");
}

uint8_t CompressionCodecWallaby::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::Wallaby);
}

void CompressionCodecWallaby::updateHash(SipHash & hash) const
{
    getCodecDescription()->updateTreeHash(hash, /* ignore_aliases */ true);
    hash.update(float_width);
}

String CompressionCodecWallaby::getDescription() const
{
    return "Lossless compression of floating-point columns: integerization (decimals and other rational grids), prediction, and entropy coding with an alphabet fitted to the block.";
}

UInt32 CompressionCodecWallaby::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    /// A block that does not compress is stored verbatim after a one-byte header.
    if (uncompressed_size == std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Codec Wallaby cannot reserve a compression buffer for {} bytes", uncompressed_size);
    return uncompressed_size + 1;
}

UInt32 CompressionCodecWallaby::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    if (float_width != sizeof(Float32) && float_width != sizeof(Float64))
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with codec Wallaby: unsupported float width {}", static_cast<UInt32>(float_width));
    return Wallaby::compress(source, source_size, dest, float_width);
}

UInt32 CompressionCodecWallaby::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    /// The float width is stored in the block: an instance created from the method byte alone can decompress.
    Wallaby::decompress(source, source_size, dest, uncompressed_size);
    return uncompressed_size;
}

namespace
{

bool isApplicableToType(const IDataType & column_type)
{
    return WhichDataType(column_type).isNativeFloat();
}

}

void registerCodecWallaby(CompressionCodecFactory & factory)
{
    const auto method_code = static_cast<UInt8>(CompressionMethodByte::Wallaby);
    auto codec_builder = [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        if (arguments && !arguments->children.empty())
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "Wallaby codec must not have parameters, given {}", arguments->children.size());

        UInt8 float_width = sizeof(Float64);
        if (column_type)
        {
            if (!isApplicableToType(*column_type))
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Codec Wallaby is not applicable for {} because the data type is not Float*",
                    column_type->getName());

            float_width = static_cast<UInt8>(column_type->getSizeOfValueInMemory());
        }

        return std::make_shared<CompressionCodecWallaby>(float_width);
    };
    factory.registerCompressionCodecWithType("Wallaby", method_code, codec_builder, {.is_applicable_to_type = isApplicableToType});
}

CompressionCodecPtr getCompressionCodecWallaby(UInt8 float_width)
{
    return std::make_shared<CompressionCodecWallaby>(float_width);
}

}
