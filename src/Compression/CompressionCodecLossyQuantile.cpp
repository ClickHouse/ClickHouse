#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/registerCompressionCodecs.h>
#include <Common/PODArray.h>
#include <DataTypes/IDataType.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>
#include <base/unaligned.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_CODEC_PARAMETER;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
}

/// Lossy compression codec that encodes each floating-point value as a K-bit index
/// into a per-group codebook of empirical quantiles. Achieves 4-32x compression
/// on float columns with bounded, distribution-adaptive reconstruction error.
/// Motivated by brute-force vector search on S3 where I/O bandwidth dominates.
class CompressionCodecLossyQuantile : public ICompressionCodec
{
public:
    CompressionCodecLossyQuantile(UInt8 bits_, UInt32 group_size_, UInt32 stripe_size_, UInt8 float_width_);

    uint8_t getMethodByte() const override;
    void updateHash(SipHash & hash) const override;

    static constexpr UInt8 MIN_BITS = 1;
    static constexpr UInt8 MAX_BITS = 8;
    static constexpr UInt32 DEFAULT_GROUP_SIZE = 1048576;
    static constexpr UInt32 DEFAULT_STRIPE_SIZE = 1;
    static constexpr UInt32 MAX_GROUP_SIZE = 1u << 24;
    static constexpr UInt32 MAX_STRIPE_SIZE = 1u << 16;
    static constexpr UInt8 FORMAT_VERSION = 1;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;
    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }
    bool isFloatingPointTimeSeriesCodec() const override { return true; }
    bool isLossyCompression() const override { return true; }
    bool isExperimental() const override { return true; }
    String getDescription() const override
    {
        return "Lossy codec that encodes floating-point values as K-bit indices "
               "into per-group empirical quantile codebooks. Designed for vector search on S3.";
    }

private:
    /// Header layout (11 bytes):
    /// [version(1) | float_width(1) | bits(1) | group_size(4) | stripe_size(4)]
    static constexpr UInt32 HEADER_SIZE = 11;

    const UInt8 bits;
    const UInt32 group_size;
    const UInt32 stripe_size;
    const UInt8 float_width;

    template <typename T>
    UInt32 compressImpl(const char * source, UInt32 source_size, char * dest) const;

    template <typename T>
    static UInt32 decompressImpl(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size);

    template <typename T>
    static void computeQuantiles(const T * sorted_values, size_t count, T * centroids, size_t num_centroids);

    static void packBits(const UInt8 * indices, size_t count, UInt8 bits_per_value, char * dest);
    static void unpackBits(const char * source, size_t count, UInt8 bits_per_value, UInt8 * indices);
};


CompressionCodecLossyQuantile::CompressionCodecLossyQuantile(UInt8 bits_, UInt32 group_size_, UInt32 stripe_size_, UInt8 float_width_)
    : bits(bits_)
    , group_size(group_size_)
    , stripe_size(stripe_size_)
    , float_width(float_width_)
{
    setCodecDescription("LossyQuantile", {
        make_intrusive<ASTLiteral>(static_cast<UInt64>(bits)),
        make_intrusive<ASTLiteral>(static_cast<UInt64>(group_size)),
        make_intrusive<ASTLiteral>(static_cast<UInt64>(stripe_size))
    });
}

uint8_t CompressionCodecLossyQuantile::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::LossyQuantile);
}

void CompressionCodecLossyQuantile::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, true);
}

UInt32 CompressionCodecLossyQuantile::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    UInt64 num_values = uncompressed_size / float_width;
    UInt64 num_centroids = 1ULL << bits;
    UInt64 num_groups = (num_values + group_size - 1) / group_size;
    UInt64 centroids_total = num_groups * stripe_size * num_centroids * float_width;
    UInt64 indices_total = (num_values * bits + 7) / 8;
    UInt64 result = HEADER_SIZE + centroids_total + indices_total;

    if (result > std::numeric_limits<UInt32>::max())
        return std::numeric_limits<UInt32>::max();

    return static_cast<UInt32>(result);
}

template <typename T>
void CompressionCodecLossyQuantile::computeQuantiles(const T * sorted_values, size_t count, T * centroids, size_t num_centroids)
{
    if (count == 0)
        return;

    if (count == 1)
    {
        for (size_t i = 0; i < num_centroids; ++i)
            centroids[i] = sorted_values[0];
        return;
    }

    for (size_t q = 0; q < num_centroids; ++q)
    {
        double level = static_cast<double>(q + 1) / static_cast<double>(num_centroids + 1);
        double pos = level * static_cast<double>(count - 1);
        size_t lo = static_cast<size_t>(pos);
        double frac = pos - static_cast<double>(lo);

        if (lo + 1 >= count)
            centroids[q] = sorted_values[count - 1];
        else
            centroids[q] = static_cast<T>(
                static_cast<double>(sorted_values[lo]) * (1.0 - frac)
                + static_cast<double>(sorted_values[lo + 1]) * frac);
    }
}

void CompressionCodecLossyQuantile::packBits(const UInt8 * indices, size_t count, UInt8 bits_per_value, char * dest)
{
    UInt64 buffer = 0;
    UInt8 bits_in_buffer = 0;
    size_t dest_pos = 0;

    for (size_t i = 0; i < count; ++i)
    {
        buffer |= static_cast<UInt64>(indices[i]) << bits_in_buffer;
        bits_in_buffer += bits_per_value;

        while (bits_in_buffer >= 8)
        {
            dest[dest_pos++] = static_cast<char>(buffer & 0xFF);
            buffer >>= 8;
            bits_in_buffer -= 8;
        }
    }

    if (bits_in_buffer > 0)
        dest[dest_pos] = static_cast<char>(buffer & 0xFF);
}

void CompressionCodecLossyQuantile::unpackBits(const char * source, size_t count, UInt8 bits_per_value, UInt8 * indices)
{
    UInt64 buffer = 0;
    UInt8 bits_in_buffer = 0;
    size_t src_pos = 0;
    UInt8 mask = static_cast<UInt8>((1u << bits_per_value) - 1);

    for (size_t i = 0; i < count; ++i)
    {
        while (bits_in_buffer < bits_per_value)
        {
            buffer |= static_cast<UInt64>(static_cast<UInt8>(source[src_pos++])) << bits_in_buffer;
            bits_in_buffer += 8;
        }

        indices[i] = static_cast<UInt8>(buffer & mask);
        buffer >>= bits_per_value;
        bits_in_buffer -= bits_per_value;
    }
}

template <typename T>
UInt32 CompressionCodecLossyQuantile::compressImpl(const char * source, UInt32 source_size, char * dest) const
{
    const size_t num_values = source_size / sizeof(T);
    const size_t num_centroids = 1u << bits;
    const auto * src = reinterpret_cast<const T *>(source);

    char * dest_ptr = dest;

    dest_ptr[0] = static_cast<char>(FORMAT_VERSION);
    dest_ptr[1] = static_cast<char>(float_width);
    dest_ptr[2] = static_cast<char>(bits);
    unalignedStoreLittleEndian<UInt32>(dest_ptr + 3, group_size);
    unalignedStoreLittleEndian<UInt32>(dest_ptr + 7, stripe_size);
    dest_ptr += HEADER_SIZE;

    PODArray<T> sorted_buf;
    PODArray<T> centroids(num_centroids);
    PODArray<T> thresholds(num_centroids > 1 ? num_centroids - 1 : 0);
    PODArray<UInt8> indices;

    for (size_t group_start = 0; group_start < num_values; group_start += group_size)
    {
        size_t group_end = std::min(group_start + static_cast<size_t>(group_size), num_values);

        for (UInt32 s = 0; s < stripe_size; ++s)
        {
            sorted_buf.clear();
            for (size_t i = group_start + s; i < group_end; i += stripe_size)
            {
                T val = src[i];
                if (std::isfinite(val))
                    sorted_buf.push_back(val);
            }

            if (sorted_buf.empty())
            {
                for (size_t i = 0; i < num_centroids; ++i)
                    centroids[i] = T(0);
            }
            else
            {
                std::sort(sorted_buf.begin(), sorted_buf.end());
                computeQuantiles(sorted_buf.data(), sorted_buf.size(), centroids.data(), num_centroids);
            }

            std::memcpy(dest_ptr, centroids.data(), num_centroids * sizeof(T));
            dest_ptr += num_centroids * sizeof(T);

            for (size_t i = 0; i + 1 < num_centroids; ++i)
                thresholds[i] = static_cast<T>(
                    (static_cast<double>(centroids[i]) + static_cast<double>(centroids[i + 1])) * 0.5);

            indices.clear();
            for (size_t i = group_start + s; i < group_end; i += stripe_size)
            {
                T val = src[i];

                if (std::isnan(val))
                    val = sorted_buf.empty() ? T(0) : sorted_buf.front();
                else if (val == std::numeric_limits<T>::infinity())
                    val = sorted_buf.empty() ? T(0) : sorted_buf.back();
                else if (val == -std::numeric_limits<T>::infinity())
                    val = sorted_buf.empty() ? T(0) : sorted_buf.front();

                UInt8 idx = 0;
                if (num_centroids > 1)
                {
                    auto it = std::upper_bound(thresholds.begin(), thresholds.end(), val);
                    idx = static_cast<UInt8>(it - thresholds.begin());
                }
                indices.push_back(idx);
            }

            size_t packed_size = (indices.size() * bits + 7) / 8;
            packBits(indices.data(), indices.size(), bits, dest_ptr);
            dest_ptr += packed_size;
        }
    }

    return static_cast<UInt32>(dest_ptr - dest);
}

template <typename T>
UInt32 CompressionCodecLossyQuantile::decompressImpl(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size)
{
    if (source_size < HEADER_SIZE)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress LossyQuantile-encoded data: insufficient header");

    UInt8 hdr_version = static_cast<UInt8>(source[0]);
    if (hdr_version != FORMAT_VERSION)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress LossyQuantile-encoded data: unsupported format version {}",
            static_cast<unsigned>(hdr_version));

    UInt8 hdr_bits = static_cast<UInt8>(source[2]);
    UInt32 hdr_group_size = unalignedLoadLittleEndian<UInt32>(source + 3);
    UInt32 hdr_stripe_size = unalignedLoadLittleEndian<UInt32>(source + 7);

    if (hdr_bits < MIN_BITS || hdr_bits > MAX_BITS)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress LossyQuantile-encoded data: invalid bits value {}",
            static_cast<unsigned>(hdr_bits));

    if (hdr_group_size == 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress LossyQuantile-encoded data: group_size is zero");

    if (hdr_stripe_size == 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress LossyQuantile-encoded data: stripe_size is zero");

    const size_t num_values = uncompressed_size / sizeof(T);
    const size_t num_centroids = 1u << hdr_bits;
    auto * dst = reinterpret_cast<T *>(dest);

    const char * src_ptr = source + HEADER_SIZE;
    const char * src_end = source + source_size;

    PODArray<T> centroids(num_centroids);
    PODArray<UInt8> indices;

    for (size_t group_start = 0; group_start < num_values; group_start += hdr_group_size)
    {
        size_t group_end = std::min(group_start + static_cast<size_t>(hdr_group_size), num_values);

        for (UInt32 s = 0; s < hdr_stripe_size; ++s)
        {
            size_t centroids_bytes = num_centroids * sizeof(T);
            if (src_ptr + centroids_bytes > src_end)
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
                    "Cannot decompress LossyQuantile-encoded data: truncated centroid payload");

            std::memcpy(centroids.data(), src_ptr, centroids_bytes);
            src_ptr += centroids_bytes;

            size_t count_in_stripe = 0;
            for (size_t i = group_start + s; i < group_end; i += hdr_stripe_size)
                ++count_in_stripe;

            size_t packed_size = (count_in_stripe * hdr_bits + 7) / 8;
            if (src_ptr + packed_size > src_end)
                throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
                    "Cannot decompress LossyQuantile-encoded data: truncated index payload");

            indices.resize(count_in_stripe);
            unpackBits(src_ptr, count_in_stripe, hdr_bits, indices.data());
            src_ptr += packed_size;

            size_t idx = 0;
            for (size_t i = group_start + s; i < group_end; i += hdr_stripe_size)
            {
                UInt8 centroid_idx = indices[idx++];
                if (centroid_idx >= num_centroids)
                    centroid_idx = static_cast<UInt8>(num_centroids - 1);
                dst[i] = centroids[centroid_idx];
            }
        }
    }

    return uncompressed_size;
}

UInt32 CompressionCodecLossyQuantile::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    switch (float_width)
    {
        case 4:
            return compressImpl<Float32>(source, source_size, dest);
        case 8:
            return compressImpl<Float64>(source, source_size, dest);
        default:
            throw Exception(ErrorCodes::CANNOT_COMPRESS,
                "Cannot compress with codec LossyQuantile: unsupported float width {}",
                static_cast<unsigned>(float_width));
    }
}

UInt32 CompressionCodecLossyQuantile::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < HEADER_SIZE)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress LossyQuantile-encoded data: header is too short");

    UInt8 compressed_float_width = static_cast<UInt8>(source[1]);
    switch (compressed_float_width)
    {
        case 4:
            return decompressImpl<Float32>(source, source_size, dest, uncompressed_size);
        case 8:
            return decompressImpl<Float64>(source, source_size, dest, uncompressed_size);
        default:
            throw Exception(ErrorCodes::CANNOT_DECOMPRESS,
                "Cannot decompress LossyQuantile-encoded data: unsupported float width {}",
                static_cast<unsigned>(compressed_float_width));
    }
}

namespace
{

UInt8 getFloatBytesSize(const IDataType * column_type)
{
    if (!column_type)
        return 4;

    if (!WhichDataType(column_type).isNativeFloat())
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Codec LossyQuantile is not applicable for {} because the data type is not Float32/Float64",
            column_type->getName());

    return static_cast<UInt8>(column_type->getSizeOfValueInMemory());
}

}

void registerCodecLossyQuantile(CompressionCodecFactory & factory)
{
    auto method_code = static_cast<UInt8>(CompressionMethodByte::LossyQuantile);
    auto codec_builder = [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        UInt8 float_width = getFloatBytesSize(column_type);
        UInt8 codec_bits = 4;
        UInt32 codec_group_size = CompressionCodecLossyQuantile::DEFAULT_GROUP_SIZE;
        UInt32 codec_stripe_size = CompressionCodecLossyQuantile::DEFAULT_STRIPE_SIZE;

        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() > 3)
                throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                    "Codec LossyQuantile accepts 1 to 3 parameters, given {}", arguments->children.size());

            const auto * literal = arguments->children[0]->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::Which::UInt64)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "1st argument of codec LossyQuantile (bits) must be an unsigned integer");

            UInt64 bits_value = literal->value.safeGet<UInt64>();
            if (bits_value < CompressionCodecLossyQuantile::MIN_BITS || bits_value > CompressionCodecLossyQuantile::MAX_BITS)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "Codec LossyQuantile: bits must be between {} and {}, got {}",
                    static_cast<unsigned>(CompressionCodecLossyQuantile::MIN_BITS),
                    static_cast<unsigned>(CompressionCodecLossyQuantile::MAX_BITS),
                    bits_value);
            codec_bits = static_cast<UInt8>(bits_value);

            if (arguments->children.size() >= 2)
            {
                literal = arguments->children[1]->as<ASTLiteral>();
                if (!literal || literal->value.getType() != Field::Types::Which::UInt64)
                    throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                        "2nd argument of codec LossyQuantile (group_size) must be an unsigned integer");
                UInt64 group_size_value = literal->value.safeGet<UInt64>();
                if (group_size_value == 0 || group_size_value > CompressionCodecLossyQuantile::MAX_GROUP_SIZE)
                    throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                        "Codec LossyQuantile: group_size must be between 1 and {}, got {}",
                        CompressionCodecLossyQuantile::MAX_GROUP_SIZE, group_size_value);
                codec_group_size = static_cast<UInt32>(group_size_value);
            }

            if (arguments->children.size() == 3)
            {
                literal = arguments->children[2]->as<ASTLiteral>();
                if (!literal || literal->value.getType() != Field::Types::Which::UInt64)
                    throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                        "3rd argument of codec LossyQuantile (stripe_size) must be an unsigned integer");
                UInt64 stripe_size_value = literal->value.safeGet<UInt64>();
                if (stripe_size_value == 0 || stripe_size_value > CompressionCodecLossyQuantile::MAX_STRIPE_SIZE)
                    throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                        "Codec LossyQuantile: stripe_size must be between 1 and {}, got {}",
                        CompressionCodecLossyQuantile::MAX_STRIPE_SIZE, stripe_size_value);
                codec_stripe_size = static_cast<UInt32>(stripe_size_value);
            }
        }
        else if (arguments)
        {
            /// User explicitly wrote CODEC(LossyQuantile()) with empty parens
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                "Codec LossyQuantile requires at least 1 parameter (bits)");
        }
        /// else: arguments == nullptr → method-byte readback path,
        /// decompression reads actual parameters from the on-disk header.

        return std::make_shared<CompressionCodecLossyQuantile>(codec_bits, codec_group_size, codec_stripe_size, float_width);
    };

    factory.registerCompressionCodecWithType("LossyQuantile", method_code, codec_builder);
}

}
