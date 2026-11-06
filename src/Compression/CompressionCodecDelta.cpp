#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Compression/registerCompressionCodecs.h>
#include <DataTypes/IDataType.h>
#include <base/unaligned.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>

#include <cstring>
#include <utility>


namespace DB
{

class CompressionCodecDelta : public ICompressionCodec
{
public:
    explicit CompressionCodecDelta(UInt8 delta_bytes_size_);

    uint8_t getMethodByte() const override;

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override { return uncompressed_size + 2; }

    bool isCompression() const override { return false; }
    bool isGenericCompression() const override { return false; }
    bool isDeltaCompression() const override { return true; }

    String getDescription() const override
    {
        return "Preprocessor (should be followed by some compression codec). Stores difference between neighboring values; good for monotonically increasing or decreasing data.";
    }


private:
    const UInt8 delta_bytes_size;
};


namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

CompressionCodecDelta::CompressionCodecDelta(UInt8 delta_bytes_size_)
    : delta_bytes_size(delta_bytes_size_)
{
    setCodecDescription("Delta", {make_intrusive<ASTLiteral>(static_cast<UInt64>(delta_bytes_size))});
}

uint8_t CompressionCodecDelta::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::Delta);
}

void CompressionCodecDelta::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

namespace
{

template <typename T>
void compressDataForType(const char * source, UInt32 source_size, char * dest)
{
    if (source_size % sizeof(T) != 0)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with Delta codec, data size {} is not aligned to {}", source_size, sizeof(T));

    T prev_src = 0;
    const char * const source_end = source + source_size;
    while (source < source_end)
    {
        T curr_src = unalignedLoadLittleEndian<T>(source);
        unalignedStoreLittleEndian<T>(dest, curr_src - prev_src);
        prev_src = curr_src;

        source += sizeof(T);
        dest += sizeof(T);
    }
}

/** Delta decoding is an inclusive prefix sum of the deltas, and the scalar loop is limited by the
  * loop-carried accumulator to at most one element per cycle. Instead, process the data in 16-byte
  * registers: log2(lanes) shift-and-add steps compute the prefix sum within a register, then the
  * running total of all previous registers is added to every lane, and the last lane becomes the
  * running total for the next register.
  *
  * The kernel is written with generic clang vectors; the shuffles compile to single instructions
  * on the SSE2 (x86_64) and NEON (AArch64) baselines, so no arch-specific code or runtime dispatch
  * is needed. Lanes are loaded in native byte order, so the fast path also requires a little-endian
  * build to match the little-endian on-disk format; others fall back to the scalar loop below.
  */
#if (((defined(__x86_64__) || defined(__i386__)) && defined(__SSE2__)) || (defined(__aarch64__) && defined(__ARM_NEON))) \
    && __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
#define DELTA_CODEC_SIMD_DECOMPRESS

template <typename T>
using DeltaVec [[gnu::vector_size(16)]] = T;

/// Shift the lanes up by `shift` positions, filling the low lanes with zeros.
template <size_t shift, typename T, size_t... lane>
DeltaVec<T> shiftLanesIn(DeltaVec<T> x, std::index_sequence<lane...>)
{
    return __builtin_shufflevector(DeltaVec<T>{}, x, (lane < shift ? 0 : sizeof...(lane) + lane - shift)...);
}

template <typename T, size_t... lane>
DeltaVec<T> broadcastLastLane(DeltaVec<T> x, std::index_sequence<lane...>)
{
    return __builtin_shufflevector(x, x, (lane * 0 + sizeof...(lane) - 1)...);
}

template <typename T>
T decompressBlocks(const char * source, char * dest, size_t blocks)
{
    constexpr size_t lanes = 16 / sizeof(T);
    constexpr auto index = std::make_index_sequence<lanes>{};

    DeltaVec<T> sum{};
    for (size_t i = 0; i < blocks; ++i)
    {
        DeltaVec<T> x;
        memcpy(&x, source + i * 16, 16);
        if constexpr (lanes >= 2)
            x += shiftLanesIn<1>(x, index);
        if constexpr (lanes >= 4)
            x += shiftLanesIn<2>(x, index);
        if constexpr (lanes >= 8)
            x += shiftLanesIn<4>(x, index);
        if constexpr (lanes >= 16)
            x += shiftLanesIn<8>(x, index);
        /// Update the running total from the local prefix sum, so that the broadcast stays off
        /// the loop-carried dependency chain (which is then a single addition).
        const DeltaVec<T> carry = broadcastLastLane(x, index);
        x += sum;
        sum += carry;
        memcpy(dest + i * 16, &x, 16);
    }
    return sum[0];
}

#endif

template <typename T>
UInt32 decompressDataForType(const char * source, UInt32 source_size, char * dest, UInt32 output_size)
{
    if (source_size % sizeof(T) != 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress Delta-encoded data, data size {} is not aligned to {}", source_size, sizeof(T));

    if (source_size > output_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress delta-encoded data: {} bytes of data do not fit into the output of {} bytes", source_size, output_size);

    T accumulator{};
    const char * const source_end = source + source_size;

#ifdef DELTA_CODEC_SIMD_DECOMPRESS
    const size_t blocks = source_size / 16;
    accumulator = decompressBlocks<T>(source, dest, blocks);
    source += blocks * 16;
    dest += blocks * 16;
#endif

    while (source < source_end)
    {
        accumulator += unalignedLoadLittleEndian<T>(source);
        unalignedStoreLittleEndian<T>(dest, accumulator);
        source += sizeof(T);
        dest += sizeof(T);
    }

    return source_size;
}

}

UInt32 CompressionCodecDelta::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    UInt8 bytes_to_skip = source_size % delta_bytes_size;
    dest[0] = delta_bytes_size;
    dest[1] = bytes_to_skip; /// unused (backward compatibility)
    memcpy(&dest[2], source, bytes_to_skip);
    size_t start_pos = 2 + bytes_to_skip;
    switch (delta_bytes_size)
    {
    case 1:
        compressDataForType<UInt8>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 2:
        compressDataForType<UInt16>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 4:
        compressDataForType<UInt32>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    case 8:
        compressDataForType<UInt64>(&source[bytes_to_skip], source_size - bytes_to_skip, &dest[start_pos]);
        break;
    default:
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot compress to delta-encoded data. Invalid byte size {}", UInt32{delta_bytes_size});
    }
    return 1 + 1 + source_size;
}

UInt32 CompressionCodecDelta::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < 2)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress delta-encoded data. File has wrong header");

    if (uncompressed_size == 0)
        return 0;

    UInt8 bytes_size = source[0];

    if (!(bytes_size == 1 || bytes_size == 2 || bytes_size == 4 || bytes_size == 8))
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress delta-encoded data. File has wrong header");

    UInt8 bytes_to_skip = uncompressed_size % bytes_size;
    UInt32 output_size = uncompressed_size - bytes_to_skip;

    if (static_cast<UInt32>(2 + bytes_to_skip) > source_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress delta-encoded data. File has wrong header");

    memcpy(dest, &source[2], bytes_to_skip);
    UInt32 source_size_no_header = source_size - bytes_to_skip - 2;
    switch (bytes_size)
    {
    case 1:
        return bytes_to_skip + decompressDataForType<UInt8>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], output_size);
    case 2:
        return bytes_to_skip + decompressDataForType<UInt16>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], output_size);
    case 4:
        return bytes_to_skip + decompressDataForType<UInt32>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], output_size);
    case 8:
        return bytes_to_skip + decompressDataForType<UInt64>(&source[2 + bytes_to_skip], source_size_no_header, &dest[bytes_to_skip], output_size);
    default:
        /// This should be unreachable due to the check above
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot decompress delta-encoded data. File has unknown byte size {}", UInt32{bytes_size});
    }
}

namespace
{

UInt8 getDeltaBytesSize(const IDataType * column_type)
{
    if (!column_type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Codec Delta is not applicable for {} because the data type is not of fixed size",
            column_type->getName());

    size_t max_size = column_type->getSizeOfValueInMemory();
    if (max_size == 1 || max_size == 2 || max_size == 4 || max_size == 8)
        return static_cast<UInt8>(max_size);
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Codec Delta is only applicable for data types of size 1, 2, 4, 8 bytes. Given type {}",
        column_type->getName());
}

}

void registerCodecDelta(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::Delta);
    auto codec_builder = [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        /// Default bytes size is 1.
        UInt8 delta_bytes_size = 1;

        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() > 1)
                throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "Delta codec must have 1 parameter, given {}", arguments->children.size());

            const auto children = arguments->children;
            const auto * literal = children[0]->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::Which::UInt64)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Delta codec argument must be unsigned integer");

            size_t user_bytes_size = literal->value.safeGet<UInt64>();
            if (user_bytes_size != 1 && user_bytes_size != 2 && user_bytes_size != 4 && user_bytes_size != 8)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Delta value for delta codec can be 1, 2, 4 or 8, given {}", user_bytes_size);
            delta_bytes_size = static_cast<UInt8>(user_bytes_size);
        }
        else if (column_type)
        {
            delta_bytes_size = getDeltaBytesSize(column_type);
        }

        return std::make_shared<CompressionCodecDelta>(delta_bytes_size);
    };
    factory.registerCompressionCodecWithType("Delta", method_code, codec_builder);
}

CompressionCodecPtr getCompressionCodecDelta(UInt8 delta_bytes_size)
{
    return std::make_shared<CompressionCodecDelta>(delta_bytes_size);
}

}
