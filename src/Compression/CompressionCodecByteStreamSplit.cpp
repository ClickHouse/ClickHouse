#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Common/TargetSpecific.h>
#include <DataTypes/IDataType.h>
#include <base/unaligned.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <IO/WriteHelpers.h>

namespace DB
{


class CompressionCodecByteStreamSplit : public ICompressionCodec
{
public:
    static constexpr UInt32 HEADER_SIZE = 2;

    explicit CompressionCodecByteStreamSplit(UInt8 element_bytes_size_);

    uint8_t getMethodByte() const override;
    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override
    {
        return uncompressed_size + HEADER_SIZE;
    }

    bool isCompression() const override { return false; }
    bool isGenericCompression() const override { return false; }

    String getDescription() const override
    {
        return "Preprocessor (should be followed by a compression codec). "
               "Transposes bytes of fixed-width elements so that bytes at the "
               "same position within each element are grouped into contiguous "
               "streams. Significantly improves compression of floating-point "
               "and other fixed-width columnar data (Float32, Float64, "
               "Decimal128, UUID, IPv6, FixedString).";
    }

private:
    const UInt8 element_bytes_size;
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

MULTITARGET_FUNCTION_AVX512BW_AVX512F_AVX2_SSE42(
MULTITARGET_FUNCTION_HEADER(
void), encodeImpl, MULTITARGET_FUNCTION_BODY((
    const char * __restrict__ src,
    char       * __restrict__ dst,
    UInt32 num_elements,
    UInt8  element_bytes) /// NOLINT
{
    for (UInt8 byte_idx = 0; byte_idx < element_bytes; ++byte_idx)
    {
        const char * src_byte = src + byte_idx;
        char       * dst_stream = dst + static_cast<UInt32>(byte_idx) * num_elements;

        for (UInt32 i = 0; i < num_elements; ++i)
            dst_stream[i] = src_byte[i * element_bytes];
    }
})
)

ALWAYS_INLINE void encode(
    const char * src,
    char       * dst,
    UInt32 num_elements,
    UInt8  element_bytes)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::AVX512BW))
    {
        encodeImplAVX512BW(src, dst, num_elements, element_bytes);
        return;
    }
    if (isArchSupported(TargetArch::AVX512F))
    {
        encodeImplAVX512F(src, dst, num_elements, element_bytes);
        return;
    }
    if (isArchSupported(TargetArch::AVX2))
    {
        encodeImplAVX2(src, dst, num_elements, element_bytes);
        return;
    }
    if (isArchSupported(TargetArch::SSE42))
    {
        encodeImplSSE42(src, dst, num_elements, element_bytes);
        return;
    }
#endif
    encodeImpl(src, dst, num_elements, element_bytes);
}

MULTITARGET_FUNCTION_AVX512BW_AVX512F_AVX2_SSE42(
MULTITARGET_FUNCTION_HEADER(
void), decodeImpl, MULTITARGET_FUNCTION_BODY((
    const char * __restrict__ src,
    char       * __restrict__ dst,
    UInt32 num_elements,
    UInt8  element_bytes) /// NOLINT
{
    for (UInt8 byte_idx = 0; byte_idx < element_bytes; ++byte_idx)
    {
        const char * src_stream = src + static_cast<UInt32>(byte_idx) * num_elements;
        char       * dst_byte   = dst + byte_idx;

        for (UInt32 i = 0; i < num_elements; ++i)
            dst_byte[i * element_bytes] = src_stream[i];
    }
})
)

ALWAYS_INLINE void decode(
    const char * src,
    char       * dst,
    UInt32 num_elements,
    UInt8  element_bytes)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::AVX512BW))
    {
        decodeImplAVX512BW(src, dst, num_elements, element_bytes);
        return;
    }
    if (isArchSupported(TargetArch::AVX512F))
    {
        decodeImplAVX512F(src, dst, num_elements, element_bytes);
        return;
    }
    if (isArchSupported(TargetArch::AVX2))
    {
        decodeImplAVX2(src, dst, num_elements, element_bytes);
        return;
    }
    if (isArchSupported(TargetArch::SSE42))
    {
        decodeImplSSE42(src, dst, num_elements, element_bytes);
        return;
    }
#endif
    decodeImpl(src, dst, num_elements, element_bytes);
}

UInt8 getElementBytesSize(const IDataType * column_type)
{
    if (!column_type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Codec ByteStreamSplit is not applicable for {} because the data "
            "type is not of fixed size",
            column_type->getName());

    size_t size = column_type->getSizeOfValueInMemory();

    if (size < 2)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Codec ByteStreamSplit is not applicable for {} — element size "
            "must be at least 2 bytes (splitting 1-byte values produces only "
            "one stream and has no effect)",
            column_type->getName());

    if (size > 255)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Codec ByteStreamSplit is not applicable for {} — element size "
            "{} exceeds the maximum supported width of 255 bytes",
            column_type->getName(), size);

    return static_cast<UInt8>(size);
}

} // anonymous namespace


CompressionCodecByteStreamSplit::CompressionCodecByteStreamSplit(UInt8 element_bytes_size_)
    : element_bytes_size(element_bytes_size_)
{
    setCodecDescription(
        "ByteStreamSplit",
        {make_intrusive<ASTLiteral>(static_cast<UInt64>(element_bytes_size))});
}

uint8_t CompressionCodecByteStreamSplit::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::ByteStreamSplit);
}

void CompressionCodecByteStreamSplit::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecByteStreamSplit::doCompressData(
    const char * source, UInt32 source_size, char * dest) const
{
    UInt8 bytes_to_skip = source_size % element_bytes_size;

    dest[0] = element_bytes_size;
    dest[1] = bytes_to_skip;

    if (bytes_to_skip)
        memcpy(dest + HEADER_SIZE, source, bytes_to_skip);

    const char * aligned_source = source + bytes_to_skip;
    char       * body_dest      = dest   + HEADER_SIZE + bytes_to_skip;
    UInt32       aligned_size   = source_size - bytes_to_skip;
    UInt32       num_elements   = aligned_size / element_bytes_size;

    if (num_elements > 0)
        encode(aligned_source, body_dest, num_elements, element_bytes_size);

    /// Total output: header + verbatim tail + transposed body
    /// Body is exactly aligned_size bytes (same count, reordered).
    return HEADER_SIZE + source_size;
}

UInt32 CompressionCodecByteStreamSplit::doDecompressData(
    const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size < HEADER_SIZE)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ByteStreamSplit-encoded data: source too small ({})",
            source_size);

    if (uncompressed_size == 0)
        return 0;

    UInt8 saved_element_bytes = static_cast<UInt8>(source[0]);
    UInt8 bytes_to_skip       = static_cast<UInt8>(source[1]);

    if (saved_element_bytes == 0)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ByteStreamSplit-encoded data: invalid element size 0 in header");

    if (bytes_to_skip >= saved_element_bytes)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ByteStreamSplit-encoded data: bytes_to_skip ({}) >= element_bytes ({})",
            static_cast<UInt32>(bytes_to_skip), static_cast<UInt32>(saved_element_bytes));

    if (source_size < static_cast<UInt32>(HEADER_SIZE + bytes_to_skip))
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ByteStreamSplit-encoded data: source too small for header + tail");

    if (bytes_to_skip)
        memcpy(dest, source + HEADER_SIZE, bytes_to_skip);

    UInt32 aligned_uncompressed = uncompressed_size - bytes_to_skip;

    if (aligned_uncompressed == 0)
        return bytes_to_skip;

    if (aligned_uncompressed % saved_element_bytes != 0)
        throw Exception(
            ErrorCodes::CANNOT_DECOMPRESS,
            "Cannot decompress ByteStreamSplit-encoded data: aligned uncompressed size ({}) "
            "is not a multiple of element size ({})",
            aligned_uncompressed, static_cast<UInt32>(saved_element_bytes));

    UInt32       num_elements = aligned_uncompressed / saved_element_bytes;
    const char * body_src     = source + HEADER_SIZE + bytes_to_skip;
    char       * aligned_dest = dest   + bytes_to_skip;

    decode(body_src, aligned_dest, num_elements, saved_element_bytes);

    return uncompressed_size;
}



void registerCodecByteStreamSplit(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<uint8_t>(CompressionMethodByte::ByteStreamSplit);

    auto codec_builder = [&](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
    {
        ///
        /// 1. CODEC(ByteStreamSplit) — no argument, column type must be
        ///    available. Element width is inferred from the type, exactly as
        ///    Parquet infers K from the physical type (4 for FLOAT/INT32,
        ///    8 for DOUBLE/INT64, N for FIXED_LEN_BYTE_ARRAY(N)).
        ///
        /// 2. CODEC(ByteStreamSplit(N)) — explicit width override. Useful
        ///    when no column type is available (e.g. clickhouse-compressor
        ///    tool), mirroring Delta's explicit-parameter escape hatch.
        ///    N must be >= 2 for the same reason as the type-inferred path.

        if (arguments && !arguments->children.empty())
        {
            /// Explicit width provided by user.
            if (arguments->children.size() > 1)
                throw Exception(
                    ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                    "ByteStreamSplit codec accepts at most 1 parameter (element "
                    "byte width), given {}",
                    arguments->children.size());

            const auto * literal = arguments->children[0]->as<ASTLiteral>();
            if (!literal || literal->value.getType() != Field::Types::Which::UInt64)
                throw Exception(
                    ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "ByteStreamSplit codec argument must be a positive integer "
                    "specifying the element byte width (e.g. 4 for Float32/Int32, "
                    "8 for Float64/Int64, 16 for UUID/IPv6/Int128)");

            UInt64 user_size = literal->value.safeGet<UInt64>();

            /// Mirror Parquet's minimum: splitting 1-byte values is a no-op.
            if (user_size < 2)
                throw Exception(
                    ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "ByteStreamSplit element byte width must be at least 2 "
                    "(splitting 1-byte values produces only one stream and has "
                    "no effect), given {}",
                    user_size);

            if (user_size > 255)
                throw Exception(
                    ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "ByteStreamSplit element byte width must be at most 255, "
                    "given {}",
                    user_size);

            return std::make_shared<CompressionCodecByteStreamSplit>(
                static_cast<UInt8>(user_size));
        }

        if (column_type)
            return std::make_shared<CompressionCodecByteStreamSplit>(
                getElementBytesSize(column_type));

        /// No arguments and no type: called by the factory for decompression-by-byte-code.
        /// The actual element size is stored in the compressed data header and is read
        /// back in doDecompressData, so the instance value is not used during decompression.
        return std::make_shared<CompressionCodecByteStreamSplit>(4);
    };

    factory.registerCompressionCodecWithType("ByteStreamSplit", method_code, codec_builder);
}

} // namespace DB
