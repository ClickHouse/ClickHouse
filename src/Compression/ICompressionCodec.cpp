#include <Compression/ICompressionCodec.h>

#include <Compression/CompressionCodecMultiple.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <base/unaligned.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/typeid_cast.h>

namespace CurrentMetrics
{
    extern const Metric Compressing;
    extern const Metric Decompressing;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void ICompressionCodec::setAndCheckVectorDimension(size_t /*dimension*/)
{
    if (!needsVectorDimensionUpfront())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not set dimensions for a non-vector codec");
}

ASTPtr ICompressionCodec::makeCodecDescription(const String & name, const ASTs & arguments)
{
    if (arguments.empty())
        return make_intrusive<ASTIdentifier>(name);
    return makeASTFunction(name, arguments);
}

ASTPtr ICompressionCodec::getFullCodecDesc() const
{
    if (const auto * multiple = typeid_cast<const CompressionCodecMultiple *>(this))
        return multiple->getFullCodecDesc();
    return makeASTFunction("CODEC", getCodecDesc());
}

UInt64 ICompressionCodec::getHash() const
{
    SipHash hash;
    updateHash(hash);
    return hash.get64();
}

UInt32 ICompressionCodec::compress(const char * source, UInt32 source_size, char * dest) const
{
    chassert(source != nullptr && dest != nullptr);

    CurrentMetrics::Increment metric_increment(CurrentMetrics::Compressing);

    dest[0] = getMethodByte();
    UInt8 header_size = getHeaderSize();
    /// Write data from header_size
    UInt32 compressed_bytes_written = doCompressData(source, source_size, &dest[header_size]);
    unalignedStoreLittleEndian<UInt32>(&dest[1], compressed_bytes_written + header_size);
    unalignedStoreLittleEndian<UInt32>(&dest[5], source_size);
    return header_size + compressed_bytes_written;
}

UInt32 ICompressionCodec::decompress(const char * source, UInt32 source_size, char * dest) const
{
    chassert(source != nullptr && dest != nullptr);

    CurrentMetrics::Increment metric_increment(CurrentMetrics::Decompressing);

    UInt8 header_size = getHeaderSize();
    if (source_size < header_size)
        throw Exception(decompression_error_code,
                        "Can't decompress data: the compressed data size ({}, this should include header size) "
                        "is less than the header size ({})", source_size, static_cast<size_t>(header_size));

    uint8_t our_method = getMethodByte();
    uint8_t method = source[0];
    if (method != our_method)
        throw Exception(decompression_error_code, "Can't decompress data with codec byte {} using codec with byte {}", method, our_method);

    UInt32 decompressed_size = readDecompressedBlockSize(source);
    UInt32 final_decompressed_size = doDecompressData(&source[header_size], source_size - header_size, dest, decompressed_size);
    if (decompressed_size != final_decompressed_size)
        throw Exception(
            decompression_error_code,
            "Can't decompress data: The size after decompression ({}) is different than the expected size ({}) for codec '{}'",
            final_decompressed_size,
            decompressed_size,
            getCodecDesc()->formatForErrorMessage());

    return final_decompressed_size;
}

UInt32 ICompressionCodec::readCompressedBlockSize(const char * source) const
{
    UInt32 compressed_block_size = unalignedLoadLittleEndian<UInt32>(&source[1]);
    if (compressed_block_size == 0)
        throw Exception(decompression_error_code, "Can't decompress data: header is corrupt with compressed block size 0");
    return compressed_block_size;
}


UInt32 ICompressionCodec::readDecompressedBlockSize(const char * source) const
{
    UInt32 decompressed_block_size = unalignedLoadLittleEndian<UInt32>(&source[5]);
    if (decompressed_block_size == 0)
        throw Exception(decompression_error_code, "Can't decompress data: header is corrupt with decompressed block size 0");
    return decompressed_block_size;
}


uint8_t ICompressionCodec::readMethod(const char * source)
{
    return static_cast<uint8_t>(source[0]);
}

}
