#include "config.h"

#if USE_SZ3
#    include <Compression/CompressionFactory.h>
#    include <Compression/CompressionInfo.h>
#    include <Compression/ICompressionCodec.h>
#    include <Core/TypeId.h>
#    include <DataTypes/IDataType.h>
#    include <IO/BufferWithOwnMemory.h>
#    include <IO/WriteBuffer.h>
#    include <IO/WriteHelpers.h>
#    include <Interpreters/Context.h>
#    include <Parsers/ASTLiteral.h>
#    include <Parsers/IAST.h>
#    include "Common/Exception.h"
#    include <Common/SipHash.h>
#    include "base/types.h"

#    include <SZ3/api/sz.hpp>
#    include <SZ3/utils/Config.hpp>

namespace DB
{

class CompressionCodecSZ3 : public ICompressionCodec
{
public:
    CompressionCodecSZ3(UInt8 float_size_, SZ3::ALGO algorithm_, SZ3::EB error_bound_mode_, double error_value_);

    uint8_t getMethodByte() const override;

    UInt32 getAdditionalSizeAtTheEndOfBuffer() const override { return 0; }

    void updateHash(SipHash & hash) const override;

    void setAndCheckVectorDimension(size_t dimension) override;

protected:
    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }
    /// SZ3 is still under development, it writes its current version into the serialized compressed data.
    /// Therefore, update SZ3 with care to avoid breaking existing persistencies.
    /// We mark it as experimental for now.
    bool isLossyCompression() const override { return true; }
    bool isExperimental() const override { return true; }
    bool needsVectorDimensionUpfront() const override { return true; }
    String getDescription() const override { return "SZ3 is a lossy compressor for floating-point data with error bounds."; }

private:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    std::optional<size_t> dimension;
    const UInt8 float_width;
    const SZ3::ALGO algorithm;
    const SZ3::EB error_bound_mode;
    const Float64 error_value;
};

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int CORRUPTED_DATA;
extern const int ILLEGAL_CODEC_PARAMETER;
extern const int LOGICAL_ERROR;
}

String getSZ3AlgorithmString(SZ3::ALGO algorithm)
{
    for (const auto & [algorithm_string, algorithm_id] : SZ3::ALGO_MAP)
    {
        if (algorithm_id == algorithm)
            return algorithm_string;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid algorithm");
}

String getSZ3ErrorBoundModeString(SZ3::EB error_bound_mode)
{
    for (const auto & [error_bound_string, error_bound_mode_id] : SZ3::EB_MAP)
    {
        if (error_bound_mode_id == error_bound_mode)
            return error_bound_string;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid error bound mode");
}

CompressionCodecSZ3::CompressionCodecSZ3(UInt8 float_size_, SZ3::ALGO algorithm_, SZ3::EB error_bound_mode_, double error_value_)
    : float_width(float_size_)
    , algorithm(algorithm_)
    , error_bound_mode(error_bound_mode_)
    , error_value(error_value_)
{
    setCodecDescription(
        "SZ3",
        {std::make_shared<ASTLiteral>(getSZ3AlgorithmString(algorithm)),
         std::make_shared<ASTLiteral>(getSZ3ErrorBoundModeString(error_bound_mode)),
         std::make_shared<ASTLiteral>(error_value)});
}

uint8_t CompressionCodecSZ3::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::SZ3);
}

void CompressionCodecSZ3::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, true);
    hash.update(float_width);
}

UInt32 CompressionCodecSZ3::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return sizeof(UInt8) + sizeof(SZ3::Config) + uncompressed_size;
}

UInt32 CompressionCodecSZ3::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    SZ3::Config config;

    std::vector<size_t> result_dimensions;
    size_t num_floats = (source_size / float_width) / dimension.value_or(1);

    result_dimensions.push_back(num_floats);
    result_dimensions.push_back(dimension.value_or(1));

    config.setDims(result_dimensions.begin(), result_dimensions.end());

    config.cmprAlgo = algorithm;
    config.errorBoundMode = error_bound_mode;

    switch (error_bound_mode)
    {
        case SZ3::EB_REL:
            config.relErrorBound = error_value;
            break;
        case SZ3::EB_ABS:
            config.absErrorBound = error_value;
            break;
        case SZ3::EB_PSNR:
            config.psnrErrorBound = error_value;
            break;
        case SZ3::EB_L2NORM:
            config.l2normErrorBound = error_value;
            break;
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid error bound mode");
    }

    std::unique_ptr<char[]> compressed;
    size_t compressed_size;
    switch (float_width)
    {
        case 4:
        {
            try
            {
                compressed.reset(SZ_compress(config, reinterpret_cast<const float *>(source), compressed_size));
            }
            catch (...)
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Unexpected data to compress");
            }
            break;
        }
        case 8:
        {
            try
            {
                compressed.reset(SZ_compress(config, reinterpret_cast<const double *>(source), compressed_size));
            }
            catch (...)
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Unexpected data to compress");
            }
            break;
        }
        default:
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Unexpected float width in SZ3 compressed data");
    }

    size_t offset = 0;
    memcpy(dest + offset, &float_width, sizeof(UInt8));
    offset += sizeof(UInt8);

    memcpy(dest + offset, compressed.get(), compressed_size);
    return static_cast<UInt32>(offset + compressed_size);
}

void CompressionCodecSZ3::setAndCheckVectorDimension(size_t dimension_)
{
    if (dimension.has_value() && *dimension != dimension_)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Vector dimensions are not equals: {} and {}", dimension_, *dimension);
    dimension = dimension_;
}

void CompressionCodecSZ3::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 /*uncompressed_size*/) const
{
    /// Hardcoded, because it is not declared (we just calculated the minimal size of decompressed config) and it is less than sizeof(SZ3::Config).
    static constexpr const size_t config_size = 39;
    if (source_size < 1 + config_size)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Can not decompress data {} that is less than minimal size of compressed", source_size);

    UInt8 width = static_cast<UInt8>(*source);
    --source_size;
    ++source;

    SZ3::Config config;
    switch (width)
    {
        case 4:
        {
            try
            {
                float * dest_typed = reinterpret_cast<float *>(dest);
                SZ_decompress<float>(config, const_cast<char *>(source), source_size, dest_typed);
            }
            catch (...)
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Unexpected data to compress");
            }
            break;
        }
        case 8:
        {
            try
            {
                double * dest_typed = reinterpret_cast<double *>(dest);
                SZ_decompress<double>(config, const_cast<char *>(source), source_size, dest_typed);
            }
            catch (...)
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Unexpected data to compress");
            }
            break;
        }
        default:
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Unexpected float width in SZ3 compressed data");
    }
}

UInt8 getFloatByteWidth(const IDataType & column_type)
{
    if (!WhichDataType(column_type).isNativeFloat())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Codec 'SZ3' is not applicable for {} because the data type is not Float*", column_type.getName());

    return column_type.getSizeOfValueInMemory();
}

SZ3::ALGO getSZ3Algorithm(const String & algorithm)
{
    return SZ3::ALGO_MAP.at(algorithm);
}

SZ3::EB getSZ3ErrorBoundMode(const String & error_bound_mode)
{
    return SZ3::EB_MAP.at(error_bound_mode);
}

void registerCodecSZ3(CompressionCodecFactory & factory)
{
    auto method_code = static_cast<UInt8>(CompressionMethodByte::SZ3);
    auto codec_builder = [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        UInt8 float_width = 4;
        if (column_type)
            float_width = getFloatByteWidth(*column_type);

        if (!arguments || arguments->children.empty())
        {
            static constexpr auto default_algorithm = SZ3::ALGO_INTERP_LORENZO;
            static constexpr auto default_error_bound_mode = SZ3::EB_REL;
            static constexpr auto default_error_bound = 1e-2;

            return std::make_shared<CompressionCodecSZ3>(float_width, default_algorithm, default_error_bound_mode, default_error_bound);
        }
        else if (arguments->children.size() == 3)
        {
            const auto & children = arguments->children;
            const auto * literal = children[0]->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::Which::String)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "1st argument of codec 'SZ3' must be a String");
            auto algorithm_string = static_cast<String>(literal->value.safeGet<String>());
            auto algorithm = getSZ3Algorithm(algorithm_string);

            literal = children[1]->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::Which::String)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "2nd argument of codec 'SZ3' be a String");
            auto error_bound_mode_string = static_cast<String>(literal->value.safeGet<String>());
            auto error_bound_mode = getSZ3ErrorBoundMode(error_bound_mode_string);

            literal = children[2]->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::Which::Float64)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "3rd argument of codec 'SZ3' be a Float64");
            auto error_value = static_cast<double>(literal->value.safeGet<Float64>());

            return std::make_shared<CompressionCodecSZ3>(float_width, algorithm, error_bound_mode, error_value);
        }
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Codec SZ3 must have 0 or 3 arguments but {} arguments are given", arguments->children.size());
        }
    };
    factory.registerCompressionCodecWithType("SZ3", method_code, codec_builder);
}

CompressionCodecPtr getCompressionCodecSZ3(UInt8 float_bytes_size)
{
    return std::make_shared<CompressionCodecSZ3>(float_bytes_size, SZ3::ALGO_INTERP_LORENZO, SZ3::EB_REL, 0.001);
}

}
#endif
