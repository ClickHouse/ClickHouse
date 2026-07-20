#include "config.h"

#if USE_PCO

#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Compression/registerCompressionCodecs.h>
#include <Common/SipHash.h>
#include <DataTypes/IDataType.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>

#include <pco.h>

#include <cstring>
#include <limits>

namespace DB
{

/// Compression codec that links the `pco` (pcodec) Rust crate — the reference
/// implementation, patched in the ClickHouse fork to add runtime CPU-feature
/// dispatch of its hot loops (see contrib/pcodec). pcodec is specialized for
/// sequences of fixed-width numbers.
///
/// The block layout written by `doCompressData` mirrors the Gorilla/FPC codecs:
///   [1 byte] width-and-flags  [1 byte] bytes_to_skip  [skipped leading bytes]  [payload]
///
/// The low bits of the first byte hold the element width (1/2/4/8). The high bit
/// (0x80) is the "stored" flag: when set, the payload is the raw uncompressed
/// bytes (used when compression would expand the data), guaranteeing that the
/// output never exceeds the input by more than the 2-byte header. When clear,
/// the payload is a standalone `.pco` stream, wire-compatible with the reference
/// pcodec implementation.
class CompressionCodecPco : public ICompressionCodec
{
public:
    CompressionCodecPco(UInt8 data_bytes_size_, UInt8 pco_type_byte_, UInt8 compression_level_);

    uint8_t getMethodByte() const override;

    void updateHash(SipHash & hash) const override;

    static constexpr UInt8 DEFAULT_COMPRESSION_LEVEL = 8;
    static constexpr UInt8 MAX_COMPRESSION_LEVEL = 12;

    /// The high bit of the first block byte marks a stored (uncompressed) payload.
    static constexpr UInt8 STORED_FLAG = 0x80;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;
    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }
    bool isExperimental() const override { return true; }
    /// An instance created without a column type (data_bytes_size == 0) can only decompress.
    bool requiresColumnTypeToCompress() const override { return data_bytes_size == 0; }
    String getDescription() const override
    {
        return "Lossless compression of numeric sequences (pcodec); excellent on numeric columns.";
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

/// Maps a ClickHouse data type to a pcodec number-type byte. The mapping only depends on
/// (float vs signed-int vs unsigned-int) and width; any consistent choice round-trips losslessly,
/// so types whose representation matches a supported width are accepted (Date, DateTime, Decimal,
/// IPv4, Enum, etc. via their underlying integer).
UInt8 getPcoTypeByte(const IDataType & column_type)
{
    WhichDataType which(column_type);

    if (which.isFloat32())
        return PCO_F32;
    if (which.isFloat64())
        return PCO_F64;

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
            return is_signed ? PCO_I8 : PCO_U8;
        case 2:
            return is_signed ? PCO_I16 : PCO_U16;
        case 4:
            return is_signed ? PCO_I32 : PCO_U32;
        default:
            return is_signed ? PCO_I64 : PCO_U64;
    }
}

UInt8 widthOfPcoType(UInt8 type_byte)
{
    switch (type_byte)
    {
        case PCO_U8:
        case PCO_I8:
            return 1;
        case PCO_U16:
        case PCO_I16:
        case PCO_F16:
            return 2;
        case PCO_U32:
        case PCO_I32:
        case PCO_F32:
            return 4;
        default:
            return 8;
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
    /// `PCO` is type-dependent: the element width and the pcodec number-type byte (which also
    /// distinguishes signed/unsigned/float at the same width) determine the produced stream. Compact
    /// parts group substreams by `getHash`, so without these a `UInt32`, `Int64` and `Float64` column
    /// all sharing `CODEC(PCO)` could reuse a single codec object and encode with the wrong type/width.
    hash.update(data_bytes_size);
    hash.update(pco_type_byte);
}

UInt32 CompressionCodecPco::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    /// Tight bound: the encoder falls back to storing the raw bytes whenever the compressed stream
    /// would not fit in the raw payload size, so the output is at most the raw data plus the 2-byte
    /// header. `CompressedWriteBuffer` reserves this per block.
    UInt64 bound = UInt64{2} + uncompressed_size;

    /// The result (and `CompressedWriteBuffer`'s reserve) is a `UInt32`. Fail closed rather than
    /// silently truncating to a too-small reservation.
    if (bound > std::numeric_limits<UInt32>::max())
        throw Exception(
            ErrorCodes::CANNOT_COMPRESS,
            "Codec 'PCO' cannot reserve a compression buffer for {} bytes: the required upper bound {} exceeds 4 GiB",
            uncompressed_size,
            bound);

    return static_cast<UInt32>(bound);
}

UInt32 CompressionCodecPco::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    if (data_bytes_size == 0 || pco_type_byte == 0)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Codec 'PCO' was created without a numeric column type and cannot compress");

    UInt8 bytes_to_skip = source_size % data_bytes_size;
    dest[1] = static_cast<char>(bytes_to_skip);
    memcpy(&dest[2], source, bytes_to_skip);

    UInt64 n = (source_size - bytes_to_skip) / data_bytes_size;
    UInt64 raw_payload_size = n * data_bytes_size;
    auto * payload = reinterpret_cast<uint8_t *>(dest) + 2 + bytes_to_skip;

    /// Give the encoder exactly the raw payload size as its budget. If the `.pco` stream would be
    /// larger, it reports `PCO_WONT_FIT` and we store the raw bytes instead, keeping the output
    /// within `getMaxCompressedDataSize`.
    UInt64 encoded_size = 0;
    int rc = pco_compress(
        pco_type_byte,
        reinterpret_cast<const uint8_t *>(source) + bytes_to_skip,
        n,
        compression_level,
        payload,
        raw_payload_size,
        &encoded_size);

    if (rc == PCO_OK)
    {
        dest[0] = static_cast<char>(data_bytes_size);
        return static_cast<UInt32>(2 + bytes_to_skip + encoded_size);
    }
    if (rc == PCO_WONT_FIT)
    {
        dest[0] = static_cast<char>(data_bytes_size | CompressionCodecPco::STORED_FLAG);
        memcpy(payload, source + bytes_to_skip, raw_payload_size);
        return static_cast<UInt32>(2 + bytes_to_skip + raw_payload_size);
    }
    throw Exception(ErrorCodes::CANNOT_COMPRESS, "Codec 'PCO' failed to compress the data");
}

UInt32 CompressionCodecPco::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < 2)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress PCO-encoded data: header too small");

    /// Block layout written by `doCompressData` (documented in `docs/en/interfaces/specs/NativeFormat.md`):
    /// `[1 byte: W|flags][1 byte: B][B raw leading bytes][payload]`, where `W` is the element width,
    /// the `0x80` flag marks a stored payload, and `B = uncompressed_size mod W`. The `PCO` method byte
    /// `0xa0` is dispatched by the shared `CompressedReadBuffer` and can therefore reach this decoder
    /// from unchecked external framed input (notably the HTTP `decompress=1` path), so validate the
    /// stored header fields strictly and fail closed.
    UInt8 first_byte = static_cast<UInt8>(source[0]);
    bool stored = (first_byte & CompressionCodecPco::STORED_FLAG) != 0;
    UInt8 width = first_byte & ~CompressionCodecPco::STORED_FLAG;
    if (width != 1 && width != 2 && width != 4 && width != 8)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress PCO-encoded data: invalid element width {}", static_cast<UInt16>(width));

    /// A typed codec instance (created from a concrete column type) knows the exact element width its
    /// blocks were written with. A block that declares a different width — even one that would decode
    /// successfully, e.g. a `U64` payload read back as pairs of `UInt32` values — is corrupt or
    /// mismatched: fail closed instead of reinterpreting the data. The untyped method-byte instance
    /// (`data_bytes_size == 0`) has no expectation and validates the block against itself only.
    if (data_bytes_size != 0 && width != data_bytes_size)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress PCO-encoded data: the block declares element width {} but the codec was created for width {}",
            static_cast<UInt16>(width),
            static_cast<UInt16>(data_bytes_size));

    UInt8 bytes_to_skip = uncompressed_size % width;
    UInt8 stored_bytes_to_skip = static_cast<UInt8>(source[1]);
    if (stored_bytes_to_skip != bytes_to_skip)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress PCO-encoded data: the stored leading-byte count {} does not match the {} implied by the output size",
            static_cast<UInt16>(stored_bytes_to_skip),
            static_cast<UInt16>(bytes_to_skip));

    if (static_cast<UInt32>(2 + bytes_to_skip) > source_size || bytes_to_skip > uncompressed_size)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress PCO-encoded data: wrong header");

    memcpy(dest, &source[2], bytes_to_skip);

    UInt64 payload_len = source_size - 2 - bytes_to_skip;
    UInt64 expected = static_cast<UInt64>(uncompressed_size) - bytes_to_skip;
    auto * out = reinterpret_cast<uint8_t *>(dest) + bytes_to_skip;

    if (stored)
    {
        if (payload_len != expected)
            throw Exception(
                ErrorCodes::CANNOT_DECOMPRESS,
                "Cannot decompress PCO-encoded data: stored payload is {} bytes but expected {}",
                payload_len,
                expected);
        memcpy(out, &source[2 + bytes_to_skip], payload_len);
        return uncompressed_size;
    }

    UInt64 n = expected / width;
    /// A typed instance also pins the exact pcodec number type its blocks embed (`pco_type_byte`), so a
    /// same-width stream of a different type fails closed too; the untyped instance passes 0 (no
    /// expectation) and the stream is validated against the declared width only.
    int rc = pco_decompress(
        width,
        pco_type_byte,
        reinterpret_cast<const uint8_t *>(source) + 2 + bytes_to_skip,
        payload_len,
        out,
        n,
        expected);
    if (rc != PCO_OK)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress PCO-encoded data: the embedded pco stream is corrupt or its type does not match");

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
            pco_type_byte = getPcoTypeByte(*column_type);
            data_bytes_size = widthOfPcoType(pco_type_byte);
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

#else

#include <Compression/CompressionFactory.h>
#include <Compression/registerCompressionCodecs.h>

namespace DB
{

/// pco (pcodec) is not available in this build (Rust disabled); the `PCO` codec is not registered.
void registerCodecPco(CompressionCodecFactory &)
{
}

}

#endif
