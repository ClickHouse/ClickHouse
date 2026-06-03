/// Enable AVX2/AVX-512 specializations + runtime dispatch in the pcodec hot loops.
#define PCODEC_MULTITARGET 1

#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Compression/registerCompressionCodecs.h>
#include <Compression/Pcodec/Constants.h>
#include <Compression/Pcodec/PcodecError.h>
#include <Compression/Pcodec/StandaloneDecoder.h>
#include <Compression/Pcodec/StandaloneEncoder.h>
#include <DataTypes/IDataType.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>

#include <cstring>
#include <vector>


namespace DB
{

/// A native C++ port of the pcodec ("pco") codec (https://github.com/pcodec/pcodec), specialized
/// for compressing sequences of fixed-width numbers. The on-disk format is bit-for-bit compatible
/// with the reference Rust implementation (format version 4.1, standalone version 3), so the
/// compressed payload is a valid `.pco` stream.
///
/// The block layout written by `doCompressData` mirrors the Gorilla/FPC codecs:
///   [1 byte] element width  [1 byte] bytes_to_skip  [skipped leading bytes]  [standalone .pco stream]
class CompressionCodecPco : public ICompressionCodec
{
public:
    CompressionCodecPco(UInt8 data_bytes_size_, UInt8 pco_type_byte_, UInt8 compression_level_);

    uint8_t getMethodByte() const override;

    void updateHash(SipHash & hash) const override;

    static constexpr UInt8 DEFAULT_COMPRESSION_LEVEL = 8;
    static constexpr UInt8 MAX_COMPRESSION_LEVEL = 12;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;
    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }
    bool isExperimental() const override { return true; }
    String getDescription() const override
    {
        return "Lossless compression of numeric sequences, ported from pcodec; excellent on numeric columns.";
    }

private:
    const UInt8 data_bytes_size;
    const UInt8 pco_type_byte;
    const UInt8 compression_level;
};


namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// Maps a ClickHouse data type to a pcodec number-type byte. The latent mapping only depends on
/// (float vs signed-int vs unsigned-int) and width; any consistent choice round-trips losslessly,
/// so types whose representation matches a supported width are accepted (Date, DateTime, Decimal,
/// IPv4, Enum, etc. via their underlying integer).
Pcodec::NumberTypeByte getPcoTypeByte(const IDataType & column_type)
{
    using namespace Pcodec;
    WhichDataType which(column_type);

    if (which.isFloat32())
        return NumberTypeByte::F32;
    if (which.isFloat64())
        return NumberTypeByte::F64;

    if (!column_type.isValueRepresentedByNumber())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Codec 'PCO' is not applicable for {} because it is not a number", column_type.getName());

    size_t size = column_type.getSizeOfValueInMemory();
    if (size != 1 && size != 2 && size != 4 && size != 8)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Codec 'PCO' is only applicable for numeric types of size 1, 2, 4 or 8 bytes, but {} has size {}",
            column_type.getName(),
            size);

    bool is_signed = which.isInt() || which.isDate32() || which.isEnum() || which.isDecimal() || which.isDateTime64()
        || which.isInt128() || which.isInt256() || which.isInterval();

    switch (size)
    {
        case 1:
            return is_signed ? NumberTypeByte::I8 : NumberTypeByte::U8;
        case 2:
            return is_signed ? NumberTypeByte::I16 : NumberTypeByte::U16;
        case 4:
            return is_signed ? NumberTypeByte::I32 : NumberTypeByte::U32;
        default:
            return is_signed ? NumberTypeByte::I64 : NumberTypeByte::U64;
    }
}

UInt8 widthOfPcoType(Pcodec::NumberTypeByte tb)
{
    using namespace Pcodec;
    switch (tb)
    {
        case NumberTypeByte::U8:
        case NumberTypeByte::I8:
            return 1;
        case NumberTypeByte::U16:
        case NumberTypeByte::I16:
        case NumberTypeByte::F16:
            return 2;
        case NumberTypeByte::U32:
        case NumberTypeByte::I32:
        case NumberTypeByte::F32:
            return 4;
        default:
            return 8;
    }
}

/// Encodes directly into `out` (which must hold at least getMaxCompressedDataSize bytes) and
/// returns the number of bytes written. No intermediate allocation, zeroing, or copy.
size_t pcoEncodeInto(UInt8 type_byte, const uint8_t * src, size_t n, size_t level, uint8_t * out)
{
    using namespace Pcodec;
    switch (static_cast<NumberTypeByte>(type_byte))
    {
        case NumberTypeByte::U32: return encodeStandaloneInto<uint32_t>(src, n, out, level);
        case NumberTypeByte::U64: return encodeStandaloneInto<uint64_t>(src, n, out, level);
        case NumberTypeByte::I32: return encodeStandaloneInto<int32_t>(src, n, out, level);
        case NumberTypeByte::I64: return encodeStandaloneInto<int64_t>(src, n, out, level);
        case NumberTypeByte::F32: return encodeStandaloneInto<float>(src, n, out, level);
        case NumberTypeByte::F64: return encodeStandaloneInto<double>(src, n, out, level);
        case NumberTypeByte::U16: return encodeStandaloneInto<uint16_t>(src, n, out, level);
        case NumberTypeByte::I16: return encodeStandaloneInto<int16_t>(src, n, out, level);
        case NumberTypeByte::U8: return encodeStandaloneInto<uint8_t>(src, n, out, level);
        case NumberTypeByte::I8: return encodeStandaloneInto<int8_t>(src, n, out, level);
        default:
            throw Exception(ErrorCodes::CANNOT_COMPRESS, "Codec 'PCO' got an unexpected number type byte");
    }
}

}

CompressionCodecPco::CompressionCodecPco(UInt8 data_bytes_size_, UInt8 pco_type_byte_, UInt8 compression_level_)
    : data_bytes_size(data_bytes_size_), pco_type_byte(pco_type_byte_), compression_level(compression_level_)
{
    setCodecDescription("PCO", {make_intrusive<ASTLiteral>(static_cast<UInt64>(compression_level))});
}

uint8_t CompressionCodecPco::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::PCO);
}

void CompressionCodecPco::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/true);
}

UInt32 CompressionCodecPco::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    /// Safe upper bound on the standalone stream. A mode may split each value into two latents
    /// (primary + secondary), each using at most MAX_ANS_BITS + width*8 bits, plus up to
    /// 2^MAX_ANS_BITS bins of metadata per latent variable. This must cover the value the
    /// standalone encoder writes directly into `dest` (encodeStandaloneMaxSize), plus our 2-byte
    /// header and the raw partial-value tail. The encoder splits blocks larger than MAX_ENTRIES
    /// values into several chunks, each adding a small fixed framing cost.
    UInt8 width = data_bytes_size == 0 ? 1 : data_bytes_size;
    UInt32 n = uncompressed_size / width + 1;
    UInt32 num_chunks = static_cast<UInt32>((n + Pcodec::MAX_ENTRIES - 1) / Pcodec::MAX_ENTRIES);
    return 2 + width + 256 + 2 * n * (static_cast<UInt32>(width) + static_cast<UInt32>(Pcodec::MAX_ANS_BYTES) + 2) + (1u << 17) + 64
        + num_chunks * 256;
}

UInt32 CompressionCodecPco::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    if (data_bytes_size == 0 || pco_type_byte == 0)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Codec 'PCO' was created without a numeric column type and cannot compress");

    UInt8 bytes_to_skip = source_size % data_bytes_size;
    dest[0] = static_cast<char>(data_bytes_size);
    dest[1] = static_cast<char>(bytes_to_skip);
    memcpy(&dest[2], source, bytes_to_skip);

    size_t n = (source_size - bytes_to_skip) / data_bytes_size;
    size_t encoded_size = 0;
    try
    {
        encoded_size = pcoEncodeInto(
            pco_type_byte,
            reinterpret_cast<const uint8_t *>(source) + bytes_to_skip,
            n,
            compression_level,
            reinterpret_cast<uint8_t *>(dest) + 2 + bytes_to_skip);
    }
    catch (const Pcodec::PcodecError & e)
    {
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Codec 'PCO' failed to compress: {}", e.what());
    }

    return static_cast<UInt32>(2 + bytes_to_skip + encoded_size);
}

UInt32 CompressionCodecPco::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < 2)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress PCO-encoded data: header too small");

    UInt8 bytes_size = static_cast<UInt8>(source[0]);
    if (bytes_size == 0)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress PCO-encoded data: invalid element width");

    UInt8 bytes_to_skip = uncompressed_size % bytes_size;
    if (static_cast<UInt32>(2 + bytes_to_skip) > source_size || bytes_to_skip > uncompressed_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress PCO-encoded data: wrong header");

    memcpy(dest, &source[2], bytes_to_skip);
    if (bytes_to_skip == uncompressed_size)
        return uncompressed_size;

    /// Copy the standalone stream into a padded buffer so the bit reader's per-batch positional
    /// reads can safely overshoot past the logical end (see DECODE_BATCH_OVERSHOOT).
    size_t comp_len = source_size - 2 - bytes_to_skip;
    Pcodec::PcoArray<uint8_t> padded(comp_len + Pcodec::DECODE_BATCH_OVERSHOOT, 0);
    memcpy(padded.data(), &source[2 + bytes_to_skip], comp_len);

    size_t expected = uncompressed_size - bytes_to_skip;
    size_t written = 0;
    try
    {
        /// The decoder writes through a typed pointer, so the output must be aligned to the element
        /// width. `ICompressionCodec::decompress` does not guarantee this — e.g.
        /// `CompressedReadBuffer::readBig` can pass an arbitrary caller-provided `char *`, and a
        /// leading partial value (bytes_to_skip != 0) also misaligns the offset. Decode straight
        /// into `dest` only when the resulting pointer is properly aligned; otherwise decode into an
        /// aligned scratch buffer and copy.
        auto * out = reinterpret_cast<uint8_t *>(dest) + bytes_to_skip;
        bool aligned = (reinterpret_cast<uintptr_t>(out) % bytes_size) == 0;
        if (aligned)
        {
            written = Pcodec::decodeStandalone(padded.data(), comp_len, out, expected);
        }
        else
        {
            Pcodec::PcoArray<uint64_t> aligned_scratch(expected / sizeof(uint64_t) + 2);
            auto * scratch = reinterpret_cast<uint8_t *>(aligned_scratch.data());
            written = Pcodec::decodeStandalone(padded.data(), comp_len, scratch, expected);
            memcpy(out, scratch, std::min(written, expected));
        }
    }
    catch (const Pcodec::PcodecError & e)
    {
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress PCO-encoded data: {}", e.what());
    }

    if (written != expected)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress PCO-encoded data: produced {} bytes but expected {}",
            written,
            expected);

    return uncompressed_size;
}

void registerCodecPco(CompressionCodecFactory & factory)
{
    auto method_code = static_cast<UInt8>(CompressionMethodByte::PCO);
    auto codec_builder = [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        UInt8 data_bytes_size = 0;
        UInt8 pco_type_byte = 0;
        if (column_type)
        {
            Pcodec::NumberTypeByte tb = getPcoTypeByte(*column_type);
            pco_type_byte = static_cast<UInt8>(tb);
            data_bytes_size = widthOfPcoType(tb);
        }

        UInt8 level = CompressionCodecPco::DEFAULT_COMPRESSION_LEVEL;
        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() != 1)
                throw Exception(
                    ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "Codec 'PCO' must have 0 or 1 parameters, given {}",
                    arguments->children.size());

            const auto * literal = arguments->children[0]->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::Which::UInt64)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Codec 'PCO' parameter (compression level) must be an unsigned integer");

            UInt64 user_level = literal->value.safeGet<UInt64>();
            if (user_level > CompressionCodecPco::MAX_COMPRESSION_LEVEL)
                throw Exception(
                    ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Codec 'PCO' compression level must be between 0 and {}, given {}",
                    static_cast<int>(CompressionCodecPco::MAX_COMPRESSION_LEVEL), user_level);
            level = static_cast<UInt8>(user_level);
        }

        return std::make_shared<CompressionCodecPco>(data_bytes_size, pco_type_byte, level);
    };
    factory.registerCompressionCodecWithType("PCO", method_code, codec_builder);
}

}
