#include "config.h"

#if USE_SZ3
#    include <array>
#    include <cmath>
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
    /// SZ3 must be applied to raw floating-point data, so it can not follow another (e.g. delta) codec;
    /// this flag makes the codec-stack validation reject such combinations (like ALP/Gorilla/FPC).
    bool isFloatingPointTimeSeriesCodec() const override { return true; }
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

    /// SZ3 compresses whole fixed-width floating-point values. `CompressedWriteBuffer` chunks the column
    /// stream into compressed blocks by the `max_compress_block_size` byte count, so a single value can only
    /// be split across two blocks if that setting is not a multiple of the value width. Compressing such a
    /// block would silently drop the trailing partial value (the truncating division below), while the block
    /// header still records the full, untruncated size; the part would then be accepted on insert but fail to
    /// read, because `doDecompressData` rejects a trusted size that is not a multiple of the float width.
    /// Reject the misconfiguration up front instead of writing an unreadable part.
    if (source_size % float_width != 0)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The SZ3 codec compresses whole {}-byte floating-point values, but it received a {}-byte block. "
            "Set 'max_compress_block_size' to a multiple of {} for columns compressed with the SZ3 codec.",
            static_cast<UInt16>(float_width), source_size, static_cast<UInt16>(float_width));

    const size_t total_floats = source_size / float_width;
    size_t inner_dimension = dimension.value_or(1);
    /// Fall back to flat 1D compression when this block does not contain a whole number of fixed-width
    /// vectors. This happens when arrays of different lengths reach the same codec instance (e.g. a merge
    /// that combines parts whose arrays have different cardinalities) or when a compression-block boundary
    /// splits an array. Otherwise the truncating division below would drop the trailing elements, and
    /// decompression would then reject the block because the element count does not match.
    if (inner_dimension == 0 || total_floats % inner_dimension != 0)
        inner_dimension = 1;
    const size_t num_vectors = total_floats / inner_dimension;
    std::array<size_t, 2> result_dimensions{num_vectors, inner_dimension};

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
    size_t compressed_size = 0;
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
    /// SZ3 uses the array length as the inner dimension to exploit the correlation between neighbouring
    /// elements of fixed-width vectors. When arrays of different lengths reach the same codec instance -
    /// for example a merge that combines parts whose arrays have different cardinalities, or a single
    /// block containing variable-length arrays - a single fixed inner dimension can no longer describe the
    /// data. Failing here would get background merges permanently stuck on data that individual inserts
    /// already accepted, so instead we fall back to flat 1D compression. Each compressed block stores its
    /// own dimensions, so blocks compressed with different dimensions still decompress correctly.
    if (dimension.has_value() && *dimension != dimension_)
        dimension = 1;
    else
        dimension = dimension_;
}

template <typename T>
static void decompressSZ3(const char * source, UInt32 source_size, char * dest, size_t expected_num, UInt32 uncompressed_size)
{
    SZ3::Config config;

    /// Parse and validate the configuration BEFORE decompressing. SZ3 reads the compression algorithm,
    /// dimensions and the element count from the (untrusted) compressed data and dispatches on them. A crafted
    /// block could otherwise select an algorithm/encoder that this codec never produces and whose decoder is
    /// not hardened against corrupted input (e.g. ALGO_BIOMD/ALGO_BIOMDXTC, the OpenMP path), or claim a huge
    /// element count to force a large allocation. We only ever write the interpolation/Lorenzo algorithms in a
    /// single-stream layout, so reject anything else here.
    try
    {
        SZ_load_config(config, source, source_size);
    }
    catch (const std::exception & e)
    {
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Cannot parse SZ3 configuration: {}", e.what());
    }

    /// We only ever compress with the interpolation/Lorenzo algorithms, but SZ3 transparently falls back to a
    /// plain lossless (zstd) block for data that does not compress well (e.g. small or incompressible columns),
    /// so ALGO_LOSSLESS is also produced by this codec and must be accepted. Everything else (ALGO_NOPRED,
    /// ALGO_BIOMD, ALGO_BIOMDXTC) is never written here and routes into decoders that are not hardened against
    /// corrupted input, so reject it before dispatch.
    if (config.cmprAlgo != SZ3::ALGO_LORENZO_REG && config.cmprAlgo != SZ3::ALGO_INTERP_LORENZO
        && config.cmprAlgo != SZ3::ALGO_INTERP && config.cmprAlgo != SZ3::ALGO_LOSSLESS)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA, "SZ3 compressed data requests an unsupported algorithm {}", static_cast<int>(config.cmprAlgo));

    if (config.openmp)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "SZ3 compressed data requests the OpenMP path, which is not supported");

    /// The element count comes from untrusted data; require it to match the trusted uncompressed size before
    /// decompression so a corrupted count can not drive a large allocation or a buffer mismatch.
    if (config.num != expected_num)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA, "SZ3 element count {} does not match the expected {}", config.num, expected_num);

    /// The lossless (zstd) fallback the codec also produces (ALGO_LOSSLESS) reads a second, untrusted output
    /// size from its payload. The contrib fork bounds that size against the buffer capacity (`conf.num`
    /// elements) before zstd runs, so a crafted block can not write past the output buffer here.

    T * decompressed = nullptr;
    try
    {
        /// `decompressed == nullptr` makes SZ3 allocate the output buffer itself, sized to the (now validated)
        /// number of elements, so it can not overflow `dest`.
        SZ_decompress<T>(config, source, source_size, decompressed);
    }
    catch (const std::exception & e)
    {
        delete[] decompressed;
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Cannot decompress SZ3 data: {}", e.what());
    }

    std::unique_ptr<T[]> holder(decompressed);

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
    /// Only the algorithms that go through the default (interpolation/Lorenzo) decompression path are
    /// allowed. The other SZ3 algorithms (e.g. ALGO_BIOMD, ALGO_BIOMDXTC, ALGO_NOPRED, ALGO_LOSSLESS)
    /// use different decompositions/encoders that are neither tested nor hardened here.
    if (algorithm == "ALGO_LORENZO_REG")
        return SZ3::ALGO_LORENZO_REG;
    if (algorithm == "ALGO_INTERP_LORENZO")
        return SZ3::ALGO_INTERP_LORENZO;
    if (algorithm == "ALGO_INTERP")
        return SZ3::ALGO_INTERP;
    throw Exception(
        ErrorCodes::ILLEGAL_CODEC_PARAMETER,
        "Unsupported algorithm '{}' for codec 'SZ3'. Supported algorithms are "
        "'ALGO_LORENZO_REG', 'ALGO_INTERP_LORENZO' and 'ALGO_INTERP'",
        algorithm);
}

static SZ3::EB getSZ3ErrorBoundMode(const String & error_bound_mode)
{
    /// Restrict to the documented error bound modes and reject anything else with a user-facing error
    /// (a raw `EB_MAP.at` would throw `std::out_of_range`, surfacing as a logical error).
    if (error_bound_mode == "ABS")
        return SZ3::EB_ABS;
    if (error_bound_mode == "REL")
        return SZ3::EB_REL;
    if (error_bound_mode == "PSNR")
        return SZ3::EB_PSNR;
    if (error_bound_mode == "ABS_AND_REL")
        return SZ3::EB_ABS_AND_REL;
    /// Legacy aliases: the original experimental SZ3 codec parsed the mode string directly through
    /// `SZ3::EB_MAP`, so it also accepted `NORM` (L2 norm) and `ABS_OR_REL`. Column codecs are reparsed
    /// on metadata load (including `ATTACH`, where sanity checks are relaxed), so a table created on an
    /// earlier build with one of these modes must stay loadable after an upgrade. Both are still
    /// implemented by `doCompressData`, so they keep working; they are just not advertised above.
    if (error_bound_mode == "NORM")
        return SZ3::EB_L2NORM;
    if (error_bound_mode == "ABS_OR_REL")
        return SZ3::EB_ABS_OR_REL;
    throw Exception(
        ErrorCodes::ILLEGAL_CODEC_PARAMETER,
        "Unsupported error bound mode '{}' for codec 'SZ3'. Supported modes are "
        "'ABS', 'REL', 'PSNR' and 'ABS_AND_REL'",
        error_bound_mode);
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
            /// The error bound feeds SZ3's quantizer as a divisor; a non-finite or non-positive value
            /// produces NaN/Inf quantization indices that are then cast to integers, which is undefined
            /// behavior (and a non-positive bound is meaningless for a lossy error-bounded codec anyway).
            if (!std::isfinite(error_value) || error_value <= 0)
                throw Exception(
                    ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "The error bound (3rd argument) of codec 'SZ3' must be a finite positive number, got {}",
                    error_value);

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
