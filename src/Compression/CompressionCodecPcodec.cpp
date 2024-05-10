#include "config.h"

#ifdef USE_PCO
#if USE_PCO

#include "base/types.h"
#include <Common/Exception.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTLiteral.h>

#include <pco.h>

namespace DB
{

class CompressionCodecPcodec : public ICompressionCodec
{
public:
    CompressionCodecPcodec(UInt8 column_type_width_, UInt8 level_, UInt8 pco_type_);

    uint8_t getMethodByte() const override;

    void updateHash(SipHash & hash) const override;

    static constexpr UInt8 MAX_COMPRESSION_LEVEL = 12;
    static constexpr UInt8 DEFAULT_COMPRESSION_LEVEL = 8;

    static constexpr UInt8 PCO_TYPE_U32 = 1;
    static constexpr UInt8 PCO_TYPE_U64 = 2;
    static constexpr UInt8 PCO_TYPE_I32 = 3;
    static constexpr UInt8 PCO_TYPE_I64 = 4;
    static constexpr UInt8 PCO_TYPE_F32 = 5;
    static constexpr UInt8 PCO_TYPE_F64 = 6;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }
    bool isExperimental() const override { return true; }

private:
    static constexpr UInt32 HEADER_SIZE = 1;

    const UInt8 column_type_width;
    const UInt8 level;
    const UInt8 pco_type;
};

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_CODEC_PARAMETER;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int BAD_ARGUMENTS;
}

CompressionCodecPcodec::CompressionCodecPcodec(UInt8 column_type_width_, UInt8 level_, UInt8 pco_type_)
    : column_type_width{column_type_width_}, level{level_}, pco_type{pco_type_}
{
    setCodecDescription("Pcodec", {std::make_shared<ASTLiteral>(static_cast<UInt64>(level))});
}

uint8_t CompressionCodecPcodec::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::Pcodec);
}

void CompressionCodecPcodec::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecPcodec::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    dest[0] = static_cast<char>(pco_type);
    UInt32 compressed_data_written = 0;

    PcoError compress_error = pco_simple_compress(
        pco_type,
        reinterpret_cast<const uint8_t *>(source),
        source_size,
        reinterpret_cast<uint8_t *>(dest + HEADER_SIZE),
        getMaxCompressedDataSize(source_size) - HEADER_SIZE,
        level,
        &compressed_data_written);

    if (compress_error != PcoError::PcoSuccess)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with Pcodec codec");

    return HEADER_SIZE + compressed_data_written;
}

void CompressionCodecPcodec::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < HEADER_SIZE)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Pcodec-encoded data. File has wrong header {}", source_size);

    UInt8 src_pco_type = source[0];
    PcoError decompress_error = pco_simple_decompress(
        src_pco_type,
        reinterpret_cast<const uint8_t *>(source + HEADER_SIZE),
        source_size - HEADER_SIZE,
        reinterpret_cast<uint8_t *>(dest),
        uncompressed_size);

    if (decompress_error != PcoError::PcoSuccess)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress with Pcodec codec");
}

UInt32 CompressionCodecPcodec::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    UInt32 nums_count = (uncompressed_size + column_type_width - 1) / column_type_width;
    UInt32 data_size = 0;

    PcoError err = pco_file_size(pco_type, nums_count, &data_size);

    if (err != PcoError::PcoSuccess)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot get max compressed data size");

    return HEADER_SIZE + data_size;
}

namespace
{

UInt8 getPcoType(const IDataType & column_type)
{
    switch (WhichDataType(column_type).idx)
    {
        case TypeIndex::UInt32:
            return CompressionCodecPcodec::PCO_TYPE_U32;
        case TypeIndex::UInt64:
            return CompressionCodecPcodec::PCO_TYPE_U64;
        case TypeIndex::Int32:
            return CompressionCodecPcodec::PCO_TYPE_I32;
        case TypeIndex::Int64:
            return CompressionCodecPcodec::PCO_TYPE_I64;
        case TypeIndex::Float32:
            return CompressionCodecPcodec::PCO_TYPE_F32;
        case TypeIndex::Float64:
            return CompressionCodecPcodec::PCO_TYPE_F64;
        default:
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Pcodec codec is not applicable for {} because the data type must be one of these: Float32, Float64, UInt32, UInt64, "
                "Int32, Int64",
                column_type.getName());
    }
}

UInt8 getColumnTypeWidth(const IDataType & column_type)
{
    if (!WhichDataType(column_type).isFloat() && !WhichDataType(column_type).isInt32() && !WhichDataType(column_type).isInt64()
        && !WhichDataType(column_type).isUInt32() && !WhichDataType(column_type).isUInt64())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Pcodec codec is not applicable for {} because the data type must be one of these: Float32, Float64, UInt32, UInt64, Int32, "
            "Int64",
            column_type.getName());

    auto value_size = column_type.getSizeOfValueInMemory();
    if (value_size != 4 && value_size != 8)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Pcodec codec is applicable only for floats or integers of size 4 or 8 bytes. Given type {}",
            column_type.getName());

    return static_cast<UInt8>(value_size);
}

}

void registerCodecPcodec(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::Pcodec);
    auto codec_builder = [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        UInt8 pco_type = CompressionCodecPcodec::PCO_TYPE_U32;
        UInt8 column_type_width = 4;

        if (column_type != nullptr)
        {
            pco_type = getPcoType(*column_type);
            column_type_width = getColumnTypeWidth(*column_type);
        }

        UInt8 level = CompressionCodecPcodec::DEFAULT_COMPRESSION_LEVEL;
        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() > 2)
                throw Exception(
                    ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                    "Pcodec codec must have from 0 to 2 parameters, given {}",
                    arguments->children.size());

            const auto * literal = arguments->children.front()->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::Which::UInt64)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Pcodec codec argument must be unsigned integer");

            level = literal->value.safeGet<UInt8>();
            if (level < 1 || level > CompressionCodecPcodec::MAX_COMPRESSION_LEVEL)
                throw Exception(
                    ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "Pcodec codec level must be between {} and {}",
                    1,
                    static_cast<int>(CompressionCodecPcodec::MAX_COMPRESSION_LEVEL));

            if (arguments->children.size() == 2)
            {
                literal = arguments->children[1]->as<ASTLiteral>();
                if (!literal || !isInt64OrUInt64FieldType(literal->value.getType()))
                    throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Pcodec codec argument must be unsigned integer");

                size_t user_column_type_width = literal->value.safeGet<UInt64>();
                if (user_column_type_width != 4 && user_column_type_width != 8)
                    throw Exception(
                        ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                        "Column type width for Pcodec codec can be 4 or 8, given {}",
                        user_column_type_width);

                column_type_width = static_cast<UInt8>(user_column_type_width);
            }
        }

        return std::make_shared<CompressionCodecPcodec>(column_type_width, level, pco_type);
    };
    factory.registerCompressionCodecWithType("Pcodec", method_code, codec_builder);
}

}

#endif
#endif
