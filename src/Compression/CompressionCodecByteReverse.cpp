#include <Compression/ICompressionCodec.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Compression/CompressionFactory.h>
#include <Compression/registerCompressionCodecs.h>
#include <Compression/CompressionInfo.h>
#include <Parsers/IAST.h>
#include <Common/TargetSpecific.h>
#include <base/unaligned.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
}

/// ByteReverse writes the bytes of every block in the reverse order. It does not compress anything by itself:
/// it is a preprocessor that must be followed by a general purpose codec, for example `CODEC(ByteReverse, ZSTD(3))`.
///
/// What it does for compression. On log-like text (a row starts with variable fields such as a timestamp or identifiers
/// and continues with a repeated part), compressing the reversed bytes with the fast LZ codecs (`LZ4`, `ZSTD` at low
/// levels) produced fewer and longer matches in measurements, i.e. a smaller output and fewer sequences to decode. The effect
/// shrinks as the compression level grows and is zero when only the order of the rows is reversed (and not the bytes).
/// It may be neutral or negative on data that is not text-like; the codec is therefore experimental.
///
/// The transformation needs no knowledge of the value boundaries, so it is applied to the raw bytes of a block and
/// is valid for any column type. Reversing the order of the rows only (and not the bytes) does not help.
///
/// Format: the block of `source_size` bytes is stored as is, reversed. There is no header.
class CompressionCodecByteReverse : public ICompressionCodec
{
public:
    CompressionCodecByteReverse() = default;

    uint8_t getMethodByte() const override;
    ASTPtr getCodecDescription() const override;
    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    bool isCompression() const override { return false; }
    bool isGenericCompression() const override { return false; }
    String getDescription() const override
    {
        return "Preprocessor (should be followed by some compression codec). Reverses the bytes of every block; "
               "may improve the ratio of fast LZ codecs on log-like text.";
    }
};

namespace
{

/// Copies `size` bytes from `source` to `dest` in the reverse order. The ranges must not overlap.
/// The reversal is fused with the copy, so the codec costs one pass over the block in both directions.
///
/// The body works on 64-byte vectors and a vector shuffle, which the compiler lowers to the widest registers of the target:
/// `zmm` for x86-64-v4 (AVX-512), `ymm` for x86-64-v3 (AVX2), `xmm` with `pshufb` for the baseline, and NEON on ARM.
MULTITARGET_FUNCTION_X86_V4_V3(
MULTITARGET_FUNCTION_HEADER(
void), reverseCopyImpl, MULTITARGET_FUNCTION_BODY((const char * __restrict source, UInt32 size, char * __restrict dest) /// NOLINT
{
    using Vector64 = char __attribute__((vector_size(64)));

    const char * const source_end = source + size;
    size_t i = 0;

    for (; i + 64 <= size; i += 64)
    {
        Vector64 chunk;
        __builtin_memcpy(&chunk, source_end - i - 64, 64);
        chunk = __builtin_shufflevector(chunk, chunk,
                63, 62, 61, 60, 59, 58, 57, 56, 55, 54, 53, 52, 51, 50, 49, 48,
                47, 46, 45, 44, 43, 42, 41, 40, 39, 38, 37, 36, 35, 34, 33, 32,
                31, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21, 20, 19, 18, 17, 16,
                15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0);
        __builtin_memcpy(dest + i, &chunk, 64);
    }

    for (; i + 8 <= size; i += 8)
        unalignedStore<UInt64>(dest + i, __builtin_bswap64(unalignedLoad<UInt64>(source_end - i - 8)));

    for (; i < size; ++i)
        dest[i] = source_end[-1 - static_cast<ptrdiff_t>(i)];
})
)

void reverseCopy(const char * source, UInt32 size, char * dest)
{
#if USE_MULTITARGET_CODE
    if (isArchSupported(TargetArch::x86_64_v4))
    {
        reverseCopyImpl_x86_64_v4(source, size, dest);
        return;
    }
    if (isArchSupported(TargetArch::x86_64_v3))
    {
        reverseCopyImpl_x86_64_v3(source, size, dest);
        return;
    }
#endif
    reverseCopyImpl(source, size, dest);
}

}

uint8_t CompressionCodecByteReverse::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::ByteReverse);
}

ASTPtr CompressionCodecByteReverse::getCodecDescription() const
{
    return makeCodecDescription("ByteReverse");
}

void CompressionCodecByteReverse::updateHash(SipHash & hash) const
{
    getCodecDescription()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecByteReverse::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    reverseCopy(source, source_size, dest);
    return source_size;
}

UInt32 CompressionCodecByteReverse::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size != uncompressed_size)
        throw Exception(decompression_error_code,
            "Cannot decompress with ByteReverse codec: compressed size {} does not match uncompressed size {}",
            source_size, uncompressed_size);

    reverseCopy(source, source_size, dest);
    return uncompressed_size;
}

void registerCodecByteReverse(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::ByteReverse);
    factory.registerCompressionCodec("ByteReverse", method_code, [&](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        if (arguments && !arguments->children.empty())
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                "ByteReverse codec must have 0 parameters, given {}", arguments->children.size());

        return std::make_shared<CompressionCodecByteReverse>();
    });
}

}
