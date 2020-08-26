#include <Compression/CompressionCodecMultiple.h>
#include <Compression/CompressionInfo.h>
#include <Common/PODArray.h>
#include <common/unaligned.h>
#include <Compression/CompressionFactory.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>
#include <Common/hex.h>


namespace DB
{


namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int BAD_ARGUMENTS;
}

CompressionCodecMultiple::CompressionCodecMultiple(Codecs codecs_, bool sanity_check)
    : codecs(codecs_)
{
    if (sanity_check)
    {
        /// It does not make sense to apply any transformations after generic compression algorithm
        /// So, generic compression can be only one and only at the end.
        bool has_generic_compression = false;
        for (const auto & codec : codecs)
        {
            if (codec->isNone())
                throw Exception("It does not make sense to have codec NONE along with other compression codecs: " + getCodecDescImpl()
                    + ". (Note: you can enable setting 'allow_suspicious_codecs' to skip this check).",
                    ErrorCodes::BAD_ARGUMENTS);

            if (has_generic_compression)
                throw Exception("The combination of compression codecs " + getCodecDescImpl() + " is meaningless,"
                    " because it does not make sense to apply any transformations after generic compression algorithm."
                    " (Note: you can enable setting 'allow_suspicious_codecs' to skip this check).", ErrorCodes::BAD_ARGUMENTS);

            if (codec->isGenericCompression())
                has_generic_compression = true;
        }
    }
}

uint8_t CompressionCodecMultiple::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::Multiple);
}

String CompressionCodecMultiple::getCodecDesc() const
{
    return getCodecDescImpl();
}

String CompressionCodecMultiple::getCodecDescImpl() const
{
    WriteBufferFromOwnString out;
    for (size_t idx = 0; idx < codecs.size(); ++idx)
    {
        if (idx != 0)
            out << ", ";

        out << codecs[idx]->getCodecDesc();
    }
    return out.str();
}

UInt32 CompressionCodecMultiple::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    UInt32 compressed_size = uncompressed_size;
    for (const auto & codec : codecs)
        compressed_size = codec->getCompressedReserveSize(compressed_size);

    ///    TotalCodecs  ByteForEachCodec       data
    return sizeof(UInt8) + codecs.size() + compressed_size;
}

UInt32 CompressionCodecMultiple::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    PODArray<char> compressed_buf;
    PODArray<char> uncompressed_buf(source, source + source_size);

    dest[0] = static_cast<UInt8>(codecs.size());

    size_t codecs_byte_pos = 1;
    for (size_t idx = 0; idx < codecs.size(); ++idx, ++codecs_byte_pos)
    {
        const auto codec = codecs[idx];
        dest[codecs_byte_pos] = codec->getMethodByte();
        compressed_buf.resize(codec->getCompressedReserveSize(source_size));

        UInt32 size_compressed = codec->compress(uncompressed_buf.data(), source_size, compressed_buf.data());

        uncompressed_buf.swap(compressed_buf);
        source_size = size_compressed;
    }

    memcpy(&dest[1 + codecs.size()], uncompressed_buf.data(), source_size);

    return 1 + codecs.size() + source_size;
}

void CompressionCodecMultiple::useInfoAboutType(const DataTypePtr & data_type)
{
    for (auto & codec : codecs)
        codec->useInfoAboutType(data_type);
}

void CompressionCodecMultiple::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 decompressed_size) const
{
    if (source_size < 1 || !source[0])
        throw Exception("Wrong compression methods list", ErrorCodes::CORRUPTED_DATA);

    UInt8 compression_methods_size = source[0];

    PODArray<char> compressed_buf(&source[compression_methods_size + 1], &source[source_size]);
    PODArray<char> uncompressed_buf;
    /// Insert all data into compressed buf
    source_size -= (compression_methods_size + 1);

    for (int idx = compression_methods_size - 1; idx >= 0; --idx)
    {
        UInt8 compression_method = source[idx + 1];
        const auto codec = CompressionCodecFactory::instance().get(compression_method);
        auto additional_size_at_the_end_of_buffer = codec->getAdditionalSizeAtTheEndOfBuffer();

        compressed_buf.resize(compressed_buf.size() + additional_size_at_the_end_of_buffer);
        UInt32 uncompressed_size = ICompressionCodec::readDecompressedBlockSize(compressed_buf.data());

        if (idx == 0 && uncompressed_size != decompressed_size)
            throw Exception("Wrong final decompressed size in codec Multiple, got " + toString(uncompressed_size) +
                ", expected " + toString(decompressed_size), ErrorCodes::CORRUPTED_DATA);

        uncompressed_buf.resize(uncompressed_size + additional_size_at_the_end_of_buffer);
        codec->decompress(compressed_buf.data(), source_size, uncompressed_buf.data());
        uncompressed_buf.swap(compressed_buf);
        source_size = uncompressed_size;
    }

    memcpy(dest, compressed_buf.data(), decompressed_size);
}

bool CompressionCodecMultiple::isCompression() const
{
    for (const auto & codec : codecs)
        if (codec->isCompression())
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
