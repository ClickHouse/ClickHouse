#include <memory>
#include "Common/Exception.h"
#include "base/types.h"

#include "config.h"

#if USE_SZ3
#    include <Compression/CompressionFactory.h>
#    include <Compression/CompressionInfo.h>
#    include <Compression/ICompressionCodec.h>
#    include <Core/Settings.h>
#    include <DataTypes/IDataType.h>
#    include <IO/BufferWithOwnMemory.h>
#    include <IO/WriteBuffer.h>
#    include <IO/WriteHelpers.h>
#    include <Interpreters/Context.h>
#    include <Parsers/ASTFunction.h>
#    include <Parsers/ASTIdentifier.h>
#    include <Parsers/ASTLiteral.h>
#    include <Parsers/IAST.h>
#    include <Common/CurrentThread.h>
#    include <Common/SipHash.h>

#    include <SZ3/api/sz.hpp>
#    include <SZ3/utils/Config.hpp>

namespace DB
{

class CompressionCodecSZ3 : public ICompressionCodec
{
public:
    explicit CompressionCodecSZ3(UInt8 float_size_, const SZ3::ALGO & algorithm_, const SZ3::EB & error_bound_mode_, double error_value_);

    uint8_t getMethodByte() const override;

    UInt32 getAdditionalSizeAtTheEndOfBuffer() const override { return 0; }

    void updateHash(SipHash & hash) const override;

    void setDimensions(const std::vector<size_t> & dimensions) override;

    bool isVectorCodec() const override { return true; }

    String getDescription() const override { return "SZ3 is a lossy compressor for floating-point data with error bounds."; }

protected:
    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }
    /// SZ3 is still under development, it writes its current version into the serialized compressed data.
    /// Therefore, update SZ3 with care to avoid breaking existing persistencies.
    /// We mark it as experimental for now.
    bool isExperimental() const override { return true; }

private:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    std::vector<size_t> dimensions;
    const UInt8 float_size;
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

CompressionCodecSZ3::CompressionCodecSZ3(
    UInt8 float_size_, const SZ3::ALGO & algorithm_, const SZ3::EB & error_bound_mode_, double error_value_)
    : float_size(float_size_)
    , algorithm(algorithm_)
    , error_bound_mode(error_bound_mode_)
    , error_value(error_value_)
{
    setCodecDescription("SZ3");
}

uint8_t CompressionCodecSZ3::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::SZ3);
}

void CompressionCodecSZ3::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, true);
    hash.update(float_size);
}

UInt32 CompressionCodecSZ3::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return uncompressed_size + sizeof(UInt8);
}

UInt32 CompressionCodecSZ3::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    SZ3::Config config;

    std::vector<size_t> result_dimensions;
    size_t num_rows = source_size / float_size;
    for (auto dim : dimensions)
        num_rows /= dim;

    result_dimensions.push_back(num_rows);
    for (auto dim : dimensions)
        result_dimensions.push_back(dim);

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

    char * compressed;
    size_t compressed_size;
    switch (float_size)
    {
        case 4: {
            try
            {
                compressed = SZ_compress(config, reinterpret_cast<const float *>(source), compressed_size);
            }
            catch (...)
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Incorrect data to compress");
            }
            break;
        }
        case 8: {
            try
            {
                compressed = SZ_compress(config, reinterpret_cast<const double *>(source), compressed_size);
            }
            catch (...)
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Incorrect data to compress");
            }
            break;
        }
        default:
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Unexpected float width in SZ3 compressed data");
    }

    size_t offset = 0;
    memcpy(dest + offset, &float_size, sizeof(UInt8));
    offset += sizeof(UInt8);

    memcpy(dest + offset, compressed, compressed_size);
    delete[] compressed;
    return static_cast<UInt32>(offset + compressed_size);
}

void CompressionCodecSZ3::setDimensions(const std::vector<size_t> & dimensions_)
{
    dimensions = dimensions_;
}

void CompressionCodecSZ3::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 /*uncompressed_size*/) const
{
    UInt8 width = static_cast<UInt8>(*source);
    --source_size;
    ++source;

    SZ3::Config config;
    switch (width)
    {
        case 4: {
            try
            {
                float * dest_typed = reinterpret_cast<float *>(dest);
                SZ_decompress<float>(config, const_cast<char *>(source), source_size, dest_typed);
            }
            catch (...)
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Incorrect data to compress");
            }
            break;
        }
        case 8: {
            try
            {
                double * dest_typed = reinterpret_cast<double *>(dest);
                SZ_decompress<double>(config, const_cast<char *>(source), source_size, dest_typed);
            }
            catch (...)
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Incorrect data to compress");
            }
            break;
        }
        default:
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Unexpected float width in SZ3 compressed data");
    }
}

UInt8 getFloatByteSize(const IDataType & column_type)
{
    if (WhichDataType(column_type).isFloat32())
        return 4;
    else if (WhichDataType(column_type).isFloat64())
        return 8;
    else
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Codec 'SZ3' can only be used for data types Float32 or Float64, given data type: {}",
            column_type.getName());
}

SZ3::ALGO getAlgorithm(const String & algorithm)
{
    for (size_t i = 0; i < std::size(SZ3::ALGO_STR); ++i)
    {
        if (SZ3::ALGO_STR[i] == algorithm)
            return SZ3::ALGO_OPTIONS[i];
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect algorithm for codec 'sz3' {}", algorithm);
}

SZ3::EB getErrorBoundMode(const String & error_bound_mode)
{
    for (size_t i = 0; i < std::size(SZ3::EB_STR); ++i)
    {
        if (SZ3::EB_STR[i] == error_bound_mode)
            return SZ3::EB_OPTIONS[i];
    }
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect error bound mode for codec 'sz3' {}", error_bound_mode);
}

void registerCodecSZ3(CompressionCodecFactory & factory)
{
    auto method_code = static_cast<UInt8>(CompressionMethodByte::SZ3);
    auto codec_builder = [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        UInt8 float_width = 4;
        if (column_type)
            float_width = getFloatByteSize(*column_type);

        bool use_zero_error_bound = !column_type || WhichDataType(column_type).isUInt64();

        if (!arguments || arguments->children.empty())
        {
            static constexpr SZ3::ALGO default_algorithm = SZ3::ALGO_INTERP_LORENZO;
            static constexpr SZ3::EB default_error_bound_mode = SZ3::EB_REL;
            static constexpr double default_error_bound = 1e-2;
            return std::make_shared<CompressionCodecSZ3>(
                float_width, default_algorithm, default_error_bound_mode, use_zero_error_bound ? 0 : default_error_bound);
        }
        else if (arguments->children.size() == 3)
        {
            const auto & children = arguments->children;
            const auto * literal = children[0]->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::Which::String)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Codec 'SZ3' argument 0 must be a String");
            auto algorithm_string = static_cast<String>(literal->value.safeGet<String>());
            auto algorithm = getAlgorithm(algorithm_string);

            literal = children[1]->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::Which::String)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Codec 'SZ3' argument 1 must be a String");
            auto error_bound_mode_string = static_cast<String>(literal->value.safeGet<String>());
            auto error_bound_mode = getErrorBoundMode(error_bound_mode_string);

            literal = children[2]->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::Which::Float64)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Codec 'SZ3' argument 2 must be a Float64");
            auto error_value = static_cast<double>(literal->value.safeGet<Float64>());

            return std::make_shared<CompressionCodecSZ3>(float_width, algorithm, error_bound_mode, use_zero_error_bound ? 0 : error_value);
        }
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Codec SZ3 must have 0 or 3 arguments but {} arguments are given", arguments->children.size());
        }
    };
    factory.registerCompressionCodecWithType("SZ3", method_code, codec_builder);
}

}
#endif
