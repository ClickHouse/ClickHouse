#include "ICompressionCodec.h"

#include <Compression/LZ4_decompress_faster.h>
#include <common/unaligned.h>
#include <Common/hex.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Common/typeid_cast.h>
#include <Compression/CompressionFactory.h>


namespace ProfileEvents
{
    extern const Event ReadCompressedBytes;
    extern const Event CompressedReadBufferBlocks;
    extern const Event CompressedReadBufferBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int TOO_LARGE_SIZE_COMPRESSED;
    extern const int UNKNOWN_COMPRESSION_METHOD;
    extern const int CANNOT_DECOMPRESS;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int CORRUPTED_DATA;
}


UInt32 ICompressionCodec::compress(const char * source, UInt32 source_size, char * dest) const
{
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
    UInt8 header_size = getHeaderSize();

    if (source_size < header_size)
        throw Exception("Can't decompress data: the compressed data size (" + toString(source_size)
            + ", this should include header size) is less than the header size (" + toString(header_size) + ")", ErrorCodes::CORRUPTED_DATA);

    UInt8 method = source[0];
    if (method != getMethodByte())
        throw Exception("Can't decompress data with codec byte " + toString(method) + " from codec with byte " + toString(method), ErrorCodes::CANNOT_DECOMPRESS);

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


UInt8 ICompressionCodec::readMethod(const char * source)
{
    return static_cast<UInt8>(source[0]);
}

}
