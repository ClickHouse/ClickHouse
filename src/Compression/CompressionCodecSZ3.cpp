#include "config.h"

#if USE_SZ3
#    include <array>
#    include <cstring>
#    include <memory>
#    include <Compression/CompressionFactory.h>
#    include <Compression/CompressionInfo.h>
#    include <Compression/ICompressionCodec.h>
#    include <Compression/registerCompressionCodecs.h>
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

#    include <zstd.h>

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
    /// SZ3 only applies to floating-point data, so it is not a generic codec. This also prevents it
    /// from being selected for structural substreams (e.g. array sizes) where only generic codecs are allowed.
    bool isGenericCompression() const override { return false; }
    /// SZ3 is still under development, it writes its current version into the serialized compressed data.
    /// Therefore, update SZ3 with care to avoid breaking existing persistencies.
    /// We mark it as experimental for now.
    bool isLossyCompression() const override { return true; }
    bool isExperimental() const override { return true; }
    bool needsVectorDimensionUpfront() const override { return true; }
    String getDescription() const override { return "SZ3 is a lossy compressor for floating-point data with error bounds."; }

private:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

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

static String getSZ3AlgorithmString(SZ3::ALGO algorithm)
{
    for (const auto & [algorithm_string, algorithm_id] : SZ3::ALGO_MAP)
    {
        if (algorithm_id == algorithm)
            return algorithm_string;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid algorithm");
}

static String getSZ3ErrorBoundModeString(SZ3::EB error_bound_mode)
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
        {make_intrusive<ASTLiteral>(getSZ3AlgorithmString(algorithm)),
         make_intrusive<ASTLiteral>(getSZ3ErrorBoundModeString(error_bound_mode)),
         make_intrusive<ASTLiteral>(error_value)});
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
    /// `SZ_compress` can fall back to a lossless path and its worst-case output is
    /// `4096 + config.size_est() + ZSTD_compressBound(num * sizeof(T))` (see `SZ_compress_size_bound`),
    /// which can exceed `uncompressed_size`. `size_est()` is bounded by `sizeof(SZ3::Config)` and
    /// `num * sizeof(T)` equals `uncompressed_size`, so reserve a conservative upper bound (plus our
    /// own leading byte for the float width).
    return sizeof(UInt8) + 4096 + sizeof(SZ3::Config) + static_cast<UInt32>(ZSTD_compressBound(uncompressed_size));
}

UInt32 CompressionCodecSZ3::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    SZ3::Config config;

    size_t num_floats = (source_size / float_width) / dimension.value_or(1);
    std::array<size_t, 2> result_dimensions{num_floats, dimension.value_or(1)};

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
        case SZ3::EB_ABS_AND_REL:
        case SZ3::EB_ABS_OR_REL:
            /// Combined modes need both bounds; the codec takes a single value, so apply it to both.
            config.absErrorBound = error_value;
            config.relErrorBound = error_value;
            break;
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

template <typename T>
static void decompressSZ3(const char * source, UInt32 source_size, char * dest, size_t expected_num, UInt32 uncompressed_size)
{
    SZ3::Config config;
    T * decompressed = nullptr;
    try
    {
        /// `decompressed == nullptr` makes SZ3 allocate the output buffer itself, sized to the number
        /// of elements stored in the compressed data, so a corrupted element count can not overflow `dest`.
        SZ_decompress<T>(config, source, source_size, decompressed);
    }
    catch (const std::exception & e)
    {
        delete[] decompressed;
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Cannot decompress SZ3 data: {}", e.what());
    }

    std::unique_ptr<T[]> holder(decompressed);

    /// The number of elements comes from untrusted data; it must match the trusted uncompressed size exactly.
    if (config.num != expected_num)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "SZ3 decompressed element count {} does not match the expected {}", config.num, expected_num);

    memcpy(dest, decompressed, uncompressed_size);
}

UInt32 CompressionCodecSZ3::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size == 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Can not decompress empty SZ3 data");

    const UInt8 width = static_cast<UInt8>(*source);
    ++source;
    --source_size;

    if (width != 4 && width != 8)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Unexpected float width {} in SZ3 compressed data", static_cast<UInt16>(width));

    if (uncompressed_size % width != 0)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Uncompressed size {} is not a multiple of the float width {} for SZ3 codec",
            uncompressed_size, static_cast<UInt16>(width));
    const size_t expected_num = uncompressed_size / width;

    /// SZ3 validates the magic number and data version and (with ClickHouse's bounds-checking patches in
    /// the contrib fork) parses the rest of the compressed data without reading out of bounds. We let SZ3
    /// allocate the output buffer itself so a corrupted element count can not overflow `dest`, then validate
    /// the element count against the trusted `uncompressed_size` before copying.
    if (width == 4)
        decompressSZ3<float>(source, source_size, dest, expected_num, uncompressed_size);
    else
        decompressSZ3<double>(source, source_size, dest, expected_num, uncompressed_size);

    return uncompressed_size;
}

static UInt8 getFloatByteWidth(const IDataType & column_type)
{
    if (!WhichDataType(column_type).isNativeFloat())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Codec 'SZ3' is not applicable for {} because the data type is not Float*", column_type.getName());

    return static_cast<UInt8>(column_type.getSizeOfValueInMemory());
}

static SZ3::ALGO getSZ3Algorithm(const String & algorithm)
{
    return SZ3::ALGO_MAP.at(algorithm);
}

static SZ3::EB getSZ3ErrorBoundMode(const String & error_bound_mode)
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
