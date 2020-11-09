#include "ICompressionCodec.h"

#include <cassert>

#include <common/unaligned.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_DECOMPRESS;
    extern const int CORRUPTED_DATA;
}


UInt32 ICompressionCodec::compress(const char * source, UInt32 source_size, char * dest) const
{
    assert(source != nullptr && dest != nullptr);

    dest[0] = getMethodByte();
    UInt8 header_size = getHeaderSize();
    /// Write data from header_size
    UInt32 compressed_bytes_written = doCompressData(source, source_size, &dest[header_size]);
    unalignedStore<UInt32>(&dest[1], compressed_bytes_written + header_size);
    unalignedStore<UInt32>(&dest[5], source_size);
    return header_size + compressed_bytes_written;
}


UInt32 ICompressionCodec::decompress(const char * source, UInt32 source_size, char * dest) const
{
    assert(source != nullptr && dest != nullptr);

    UInt8 header_size = getHeaderSize();
    if (source_size < header_size)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Can't decompress data: the compressed data size ({}), this should include header size) is less than the header size ({})", source_size, size_t(header_size));

    uint8_t our_method = getMethodByte();
    uint8_t method = source[0];
    if (method != our_method)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Can't decompress data with codec byte {} using codec with byte {}", method, our_method);

    UInt32 decompressed_size = readDecompressedBlockSize(source);
    doDecompressData(&source[header_size], source_size - header_size, dest, decompressed_size);

    return decompressed_size;
}

UInt32 ICompressionCodec::readCompressedBlockSize(const char * source)
{
    return unalignedLoad<UInt32>(&source[1]);
}


UInt32 ICompressionCodec::readDecompressedBlockSize(const char * source)
{
    return unalignedLoad<UInt32>(&source[5]);
}


uint8_t ICompressionCodec::readMethod(const char * source)
{
    return static_cast<uint8_t>(source[0]);
}

}
