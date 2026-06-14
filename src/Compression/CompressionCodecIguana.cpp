#include <Compression/CompressionCodecIguana.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Compression/registerCompressionCodecs.h>
#include <Common/Exception.h>
#include <Parsers/IAST.h>

#include <cstring>

#include <iguana/encoder.h>
#include <iguana/decoder.h>
#include <iguana/output_stream.h>
#include <iguana/input_stream.h>
#include <iguana/entropy.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
}

/// Extra bytes the encoder may add on top of the input size: the serialized frequency table
/// (at most iguana::ans::byte_statistics::dense_table_max_length == 480 bytes) plus a handful of
/// control bytes. Rounded up generously.
static constexpr UInt32 ENCODER_OVERHEAD = 1024;

CompressionCodecIguana::CompressionCodecIguana()
{
    setCodecDescription("Iguana");
}

uint8_t CompressionCodecIguana::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::Iguana);
}

void CompressionCodecIguana::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecIguana::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return uncompressed_size + ENCODER_OVERHEAD;
}

UInt32 CompressionCodecIguana::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    iguana::output_stream out;
    out.reserve(source_size + ENCODER_OVERHEAD);

    const iguana::encoder::part part
    {
        .m_data = reinterpret_cast<const std::uint8_t *>(source),
        .m_size = source_size,
        /// Per-substream entropy stage: 32-way interleaved 8-bit rANS (the reference default).
        .m_entropy_mode = iguana::entropy_mode::ans32,
        /// Full Iguana pipeline: LZ structural compression followed by the entropy stage above.
        .m_encoding = iguana::encoding::iguana,
        /// With a threshold of 1.0 the encoder stores a block (or substream) verbatim whenever
        /// compression would not make it smaller, so the output never grows beyond the input by
        /// more than the small control header.
        .m_rejection_threshold = iguana::encoder::default_rejection_threshold,
    };

    try
    {
        iguana::encoder encoder;
        encoder.encode(out, part);
    }
    catch (const std::exception & e)
    {
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with Iguana codec: {}", e.what());
    }

    const std::size_t compressed_size = out.size();
    if (compressed_size > getMaxCompressedDataSize(source_size))
        throw Exception(ErrorCodes::CANNOT_COMPRESS,
            "Iguana codec produced {} bytes, which exceeds the reserved size {} for an input of {} bytes",
            compressed_size, getMaxCompressedDataSize(source_size), source_size);

    memcpy(dest, out.data(), compressed_size);
    return static_cast<UInt32>(compressed_size);
}

UInt32 CompressionCodecIguana::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    iguana::output_stream out;
    out.reserve(uncompressed_size);

    try
    {
        iguana::decoder decoder;
        iguana::input_stream in(reinterpret_cast<const std::uint8_t *>(source), source_size);
        decoder.decode(out, in);
    }
    catch (const std::exception & e)
    {
        throw Exception(decompression_error_code, "Cannot decompress Iguana-encoded data: {}", e.what());
    }

    if (out.size() != uncompressed_size)
        throw Exception(decompression_error_code,
            "Iguana codec decompressed {} bytes, but {} were expected", out.size(), uncompressed_size);

    memcpy(dest, out.data(), out.size());
    return static_cast<UInt32>(out.size());
}

void registerCodecIguana(CompressionCodecFactory & factory)
{
    factory.registerSimpleCompressionCodec("Iguana", static_cast<char>(CompressionMethodByte::Iguana), [&]()
    {
        return std::make_shared<CompressionCodecIguana>();
    });
}

}
