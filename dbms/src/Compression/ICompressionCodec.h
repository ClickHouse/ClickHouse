#pragma once

#include <memory>
#include <Core/Field.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <Common/PODArray.h>
#include <DataTypes/IDataType.h>
#include <boost/noncopyable.hpp>
#include <IO/UncompressedCache.h>
#include <Compression/LZ4_decompress_faster.h>
#include <Compression/CompressionInfo.h>

namespace DB
{

class ICompressionCodec;

using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;
using Codecs = std::vector<CompressionCodecPtr>;

/**
* Represents interface for compression codecs like LZ4, ZSTD, etc.
*/
class ICompressionCodec : private boost::noncopyable
{
public:
    virtual ~ICompressionCodec() = default;

    /// Byte which indicates codec in compressed file
    virtual UInt8 getMethodByte() const = 0;

    /// Codec description, for example "ZSTD(2)" or "LZ4,LZ4HC(5)"
    virtual String getCodecDesc() const = 0;

    /// Compressed bytes from uncompressed source to dest. Dest should preallocate memory
    virtual UInt32 compress(char * source, UInt32 source_size, char * dest) const;

    /// Decompress bytes from compressed source to dest. Dest should preallocate memory
    virtual UInt32 decompress(char * source, UInt32 source_size, char * dest) const;

    /// Number of bytes, that will be used to compress uncompressed_size bytes with current codec
    virtual UInt32 getCompressedReserveSize(UInt32 uncompressed_size) const { return getHeaderSize() + getMaxCompressedDataSize(uncompressed_size); }

    /// Some codecs (LZ4, for example) require additional bytes at end of buffer
    virtual UInt32 getAdditionalSizeAtTheEndOfBuffer() const { return 0; }

    /// Size of header in compressed data on disk
    static UInt8 getHeaderSize() { return COMPRESSED_BLOCK_HEADER_SIZE; }

    /// Read size of compressed block from compressed source
    static UInt32 readCompressedBlockSize(const char * source);

    /// Read size of decompressed block from compressed source
    static UInt32 readDecompressedBlockSize(const char * source);

    /// Read method byte from compressed source
    static UInt8 readMethod(const char * source);

    /// Some codecs may use information about column type which appears after codec creation
    virtual void useInfoAboutType(DataTypePtr /* data_type */) { }

protected:

    /// Return size of compressed data without header
    virtual UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const { return uncompressed_size; }

    /// Actually compress data, without header
    virtual UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const = 0;

    /// Actually decompress data without header
    virtual void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const = 0;
};

}
