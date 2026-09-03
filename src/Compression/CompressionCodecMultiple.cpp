#include <Compression/CompressionCodecMultiple.h>
#include <Compression/CompressionInfo.h>
#include <Compression/registerCompressionCodecs.h>
#include <Common/PODArray.h>
#include <Compression/CompressionFactory.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/IAST.h>

#include <limits>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
}

namespace
{

/// A codec never reserves less than its own input, so a result below the input means the UInt32
/// arithmetic wrapped. The check does not depend on where inside the callee the wrap happened.
UInt32 getCheckedReserveSize(const CompressionCodecPtr & codec, UInt32 size, size_t codec_index, size_t codecs_count)
{
    UInt32 reserve_size = codec->getCompressedReserveSize(size);
    if (reserve_size < size)
        throw Exception(ErrorCodes::CANNOT_COMPRESS,
            "Too many codecs in the codec chain: the size reserved for compressing {} bytes overflows 4 GiB "
            "at codec {} of {} ({}). Use fewer codecs.",
            size, codec_index + 1, codecs_count, codec->getCodecDesc()->formatForErrorMessage());
    return reserve_size;
}

}

CompressionCodecMultiple::CompressionCodecMultiple(Codecs codecs_)
    : codecs(codecs_)
{
    ASTs arguments;
    for (const auto & codec : codecs)
        arguments.push_back(codec->getCodecDesc());
    /// Special case, codec doesn't have name and contain list of codecs.
    setCodecDescription("", arguments);
}

uint8_t CompressionCodecMultiple::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::Multiple);
}

void CompressionCodecMultiple::updateHash(SipHash & hash) const
{
    for (const auto & codec : codecs)
        codec->updateHash(hash);
}

UInt32 CompressionCodecMultiple::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    /// doCompressData stores the number of codecs in one byte, and doDecompressData reads it back
    /// from there, so a longer chain would write a part that cannot be read.
    if (codecs.size() > std::numeric_limits<UInt8>::max())
        throw Exception(ErrorCodes::CANNOT_COMPRESS,
            "Too many codecs in the codec chain: {}. The number of codecs is stored in one byte, "
            "so at most {} are supported.",
            codecs.size(), static_cast<size_t>(std::numeric_limits<UInt8>::max()));

    UInt32 compressed_size = uncompressed_size;
    for (size_t idx = 0; idx < codecs.size(); ++idx)
        compressed_size = getCheckedReserveSize(codecs[idx], compressed_size, idx, codecs.size());

    ///    TotalCodecs  ByteForEachCodec       data
    size_t total_size = sizeof(UInt8) + codecs.size() + compressed_size;
    /// getCompressedReserveSize adds getHeaderSize() to this in UInt32, so leave room for it.
    static constexpr size_t max_total_size = std::numeric_limits<UInt32>::max() - ICompressionCodec::getHeaderSize();
    if (total_size > max_total_size)
        throw Exception(ErrorCodes::CANNOT_COMPRESS,
            "Too many codecs in the codec chain: the size reserved for compressing {} bytes ({}) exceeds 4 GiB. "
            "Use fewer codecs.",
            uncompressed_size, total_size);

    return static_cast<UInt32>(total_size);
}

UInt32 CompressionCodecMultiple::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    /// The caller sized dest from getMaxCompressedDataSize(source_size).
    const UInt32 dest_size = getMaxCompressedDataSize(source_size);

    PODArray<char> compressed_buf;
    PODArray<char> uncompressed_buf(source, source + source_size);

    dest[0] = static_cast<UInt8>(codecs.size());

    size_t codecs_byte_pos = 1;
    for (size_t idx = 0; idx < codecs.size(); ++idx, ++codecs_byte_pos)
    {
        const auto codec = codecs[idx];
        dest[codecs_byte_pos] = codec->getMethodByte();
        compressed_buf.resize(getCheckedReserveSize(codec, source_size, idx, codecs.size()));

        UInt32 size_compressed = codec->compress(uncompressed_buf.data(), source_size, compressed_buf.data());

        uncompressed_buf.swap(compressed_buf);
        source_size = size_compressed;
    }

    /// source_size is now each codec's actual output, computed independently of the bounds above.
    size_t written_size = sizeof(UInt8) + codecs.size() + source_size;
    if (written_size > dest_size)
        throw Exception(ErrorCodes::CANNOT_COMPRESS,
            "Compressed data of size {} does not fit the reserved buffer of size {}", written_size, dest_size);

    memcpy(&dest[1 + codecs.size()], uncompressed_buf.data(), source_size);

    return static_cast<UInt32>(written_size);
}

UInt32 CompressionCodecMultiple::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 decompressed_size) const
{
    if (source_size < 1 || !source[0])
        throw Exception(decompression_error_code, "Wrong compression methods list");

    UInt8 compression_methods_size = source[0];
    /// +1 for the compression_methods_size byte itself
    if (static_cast<UInt32>(compression_methods_size) + 1 > source_size)
        throw Exception(decompression_error_code, "Wrong compression methods list: header claims {} codecs"
                        " but compressed data is only {} bytes",
                        static_cast<UInt32>(compression_methods_size), source_size);

    PODArray<char> compressed_buf(&source[compression_methods_size + 1], &source[source_size]);
    PODArray<char> uncompressed_buf;
    /// Insert all data into compressed buf
    source_size -= (compression_methods_size + 1);

    for (int idx = compression_methods_size - 1; idx >= 0; --idx)
    {
        UInt8 compression_method = source[idx + 1];
        const auto codec = CompressionCodecFactory::instance().get(compression_method);
        auto additional_size_at_the_end_of_buffer = codec->getAdditionalSizeAtTheEndOfBuffer();

        if (compressed_buf.size() >= 1_GiB)
            throw Exception(decompression_error_code, "Too large compressed size: {}", compressed_buf.size());

        if (source_size < COMPRESSED_BLOCK_HEADER_SIZE)
            throw Exception(decompression_error_code, "Compressed data is too short to contain a block header: {} bytes",
                            source_size);

        {
            UInt32 bytes_to_resize = 0;
            if (common::addOverflow(static_cast<UInt32>(compressed_buf.size()), additional_size_at_the_end_of_buffer, bytes_to_resize))
                throw Exception(decompression_error_code, "Too large compressed size: {}", compressed_buf.size());

            compressed_buf.resize(compressed_buf.size() + additional_size_at_the_end_of_buffer);
        }

        UInt32 uncompressed_size = readDecompressedBlockSize(compressed_buf.data());

        if (uncompressed_size >= 1_GiB)
            throw Exception(decompression_error_code, "Too large uncompressed size: {}", uncompressed_size);

        if (idx == 0 && uncompressed_size != decompressed_size)
            throw Exception(decompression_error_code, "Wrong final decompressed size in codec Multiple, got {}, expected {}",
                uncompressed_size, decompressed_size);

        {
            UInt32 bytes_to_resize = 0;
            if (common::addOverflow(uncompressed_size, additional_size_at_the_end_of_buffer, bytes_to_resize))
                throw Exception(decompression_error_code, "Too large uncompressed size: {}", uncompressed_size);

            uncompressed_buf.resize(bytes_to_resize);
        }

        codec->decompress(compressed_buf.data(), source_size, uncompressed_buf.data());
        uncompressed_buf.swap(compressed_buf);
        /// The call to decompress will validate uncompressed_size (same readDecompressedBlockSize call as here)
        source_size = uncompressed_size;
    }

    memcpy(dest, compressed_buf.data(), decompressed_size);
    return decompressed_size;
}

VectorWithMemoryTracking<uint8_t> CompressionCodecMultiple::getCodecsBytesFromData(const char * source)
{
    VectorWithMemoryTracking<uint8_t> result;
    uint8_t compression_methods_size = source[0];
    result.reserve(compression_methods_size);

    for (size_t i = 0; i < compression_methods_size; ++i)
        result.push_back(source[1 + i]);

    return result;
}

bool CompressionCodecMultiple::isCompression() const
{
    for (const auto & codec : codecs)
        if (codec->isCompression())
            return true;
    return false;
}

bool CompressionCodecMultiple::isEncryption() const
{
    for (const auto & codec : codecs)
        if (codec->isEncryption())
            return true;
    return false;
}

bool CompressionCodecMultiple::isLossyCompression() const
{
    for (const auto & codec : codecs)
        if (codec->isLossyCompression())
            return true;
    return false;
}


void registerCodecMultiple(CompressionCodecFactory & factory)
{
    factory.registerSimpleCompressionCodec("Multiple", static_cast<UInt8>(CompressionMethodByte::Multiple), [&] ()
    {
        return std::make_shared<CompressionCodecMultiple>();
    });
}

}
