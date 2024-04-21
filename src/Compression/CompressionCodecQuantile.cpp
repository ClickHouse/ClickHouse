#include "base/types.h"
#include <Common/Exception.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTLiteral.h>

#include "config.h"

#ifdef USE_PCO
#include <pco.h>
#endif

namespace DB
{

class CompressionCodecQuantile : public ICompressionCodec
{
public:
    CompressionCodecQuantile(UInt8 float_width_, UInt8 level_);

    uint8_t getMethodByte() const override;

    void updateHash(SipHash & hash) const override;

    static constexpr UInt8 MAX_COMPRESSION_LEVEL = 12;
    static constexpr UInt8 DEFAULT_COMPRESSION_LEVEL = 8;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }
    bool isFloatingPointTimeSeriesCodec() const override { return true; }

private:
    static constexpr UInt32 HEADER_SIZE = 2;

    static constexpr UInt8 PCO_TYPE_F32 = 5;
    static constexpr UInt8 PCO_TYPE_F64 = 6;

    const UInt8 float_width;
    const UInt8 level;
};

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_CODEC_PARAMETER;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int BAD_ARGUMENTS;
}

CompressionCodecQuantile::CompressionCodecQuantile(UInt8 float_width_, UInt8 level_) : float_width{float_width_}, level{level_}
{
    setCodecDescription("Quantile", {std::make_shared<ASTLiteral>(static_cast<UInt64>(level))});
}

uint8_t CompressionCodecQuantile::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::Quantile);
}

void CompressionCodecQuantile::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecQuantile::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    dest[0] = static_cast<char>(float_width);
    dest[1] = static_cast<char>(level);
    PcoError compress_error;
    UInt32 compressed_data_written = 0;

    switch (float_width)
    {
        case sizeof(Float32):
            compress_error = pco_simple_compress(PCO_TYPE_F32, reinterpret_cast<const uint8_t *>(source), source_size, reinterpret_cast<uint8_t *>(dest + HEADER_SIZE), getMaxCompressedDataSize(source_size) - HEADER_SIZE, level, &compressed_data_written);
            break;
        case sizeof(Float64):
            compress_error = pco_simple_compress(PCO_TYPE_F64, reinterpret_cast<const uint8_t *>(source), source_size, reinterpret_cast<uint8_t *>(dest + HEADER_SIZE), getMaxCompressedDataSize(source_size) - HEADER_SIZE, level, &compressed_data_written);
            break;
        default:
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Float size for Quantile codec can be 4 or 8, given {}", static_cast<UInt32>(float_width));
    }

    if (compress_error != PcoError::PcoSuccess)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with Quantile codec");

    return HEADER_SIZE + compressed_data_written;
}

void CompressionCodecQuantile::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < HEADER_SIZE)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Quantile-encoded data. File has wrong header {}", source_size);

    UInt8 src_float_width = source[0], src_level = source[1];
    PcoError decompress_error;

    switch (src_float_width)
    {
        case sizeof(Float32):
            decompress_error = pco_simple_decompress(PCO_TYPE_F32, reinterpret_cast<const uint8_t *>(source + HEADER_SIZE), source_size - HEADER_SIZE, reinterpret_cast<uint8_t *>(dest), uncompressed_size);
            break;
        case sizeof(Float64):
            decompress_error = pco_simple_decompress(PCO_TYPE_F64, reinterpret_cast<const uint8_t *>(source + HEADER_SIZE), source_size - HEADER_SIZE, reinterpret_cast<uint8_t *>(dest), uncompressed_size);
            break;
        default:
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Float size for Quantile codec can be 4 or 8, given {}", static_cast<UInt32>(src_float_width));
    }

    if (decompress_error != PcoError::PcoSuccess)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress with Quantile codec {} {}", static_cast<UInt32>(src_float_width), static_cast<UInt32>(src_level));
}

UInt32 CompressionCodecQuantile::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    UInt32 float_count = (uncompressed_size + float_width - 1) / float_width;
    UInt32 file_size = 0;
    PcoError err;

    switch (float_width)
    {
        case sizeof(Float64):
            err = pco_file_size(PCO_TYPE_F64, float_count, &file_size);
            break;
        case sizeof(Float32):
            err = pco_file_size(PCO_TYPE_F32, float_count, &file_size);
            break;
        default:
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Float size for Quantile codec can be 4 or 8, given {}", static_cast<UInt32>(float_width));
    }

    if (err != PcoError::PcoSuccess)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot get max compressed data size");

    return HEADER_SIZE + file_size;
}

namespace
{

UInt8 getFloatBytesSize(const IDataType & column_type)
{
    if (!WhichDataType(column_type).isFloat())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Quantile codec is not applicable for {} because the data type is not float",
                        column_type.getName());
    }

    if (auto float_size = column_type.getSizeOfValueInMemory(); float_size == 4 || float_size == 8)
    {
        return static_cast<UInt8>(float_size);
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Quantile codec is applicable only for floats of size 4 or 8 bytes. Given type {}",
                    column_type.getName());
}

}

void registerCodecQuantile(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::Quantile);
    auto codec_builder = [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        UInt8 float_width = 4;
        if (column_type != nullptr)
            float_width = getFloatBytesSize(*column_type);

        UInt8 level = CompressionCodecQuantile::DEFAULT_COMPRESSION_LEVEL;
        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() > 2)
                throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                                "Quantile codec must have from 0 to 2 parameters, given {}", arguments->children.size());

            const auto * literal = arguments->children.front()->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::Which::UInt64)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Quantile codec argument must be unsigned integer");

            level = literal->value.safeGet<UInt8>();
            if (level < 1 || level > CompressionCodecQuantile::MAX_COMPRESSION_LEVEL)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Quantile codec level must be between {} and {}",
                                1, static_cast<int>(CompressionCodecQuantile::MAX_COMPRESSION_LEVEL));

            if (arguments->children.size() == 2)
            {
                literal = arguments->children[1]->as<ASTLiteral>();
                if (!literal || !isInt64OrUInt64FieldType(literal->value.getType()))
                    throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Quantile codec argument must be unsigned integer");

                size_t user_float_width = literal->value.safeGet<UInt64>();
                if (user_float_width != 4 && user_float_width != 8)
                    throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Float size for Quantile codec can be 4 or 8, given {}", user_float_width);
                float_width = static_cast<UInt8>(user_float_width);
            }
        }

        return std::make_shared<CompressionCodecQuantile>(float_width, level);
    };
    factory.registerCompressionCodecWithType("Quantile", method_code, codec_builder);
}

}
