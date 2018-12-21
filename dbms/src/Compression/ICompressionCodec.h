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
#include <IO/LZ4_decompress_faster.h>

#include <Compression/CompressionInfo.h>

namespace DB
{

class ICompressionCodec;

using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;
using Codecs = std::vector<CompressionCodecPtr>;

/**
*
*/
class ICompressionCodec : private boost::noncopyable
{
public:
    virtual ~ICompressionCodec() = default;

    virtual UInt8 getMethodByte() const = 0;

    virtual String getCodecDesc() const = 0;

    virtual UInt32 compress(char * source, UInt32 source_size, char * dest) const;

    virtual UInt32 decompress(char * source, UInt32 source_size, char * dest) const;

    virtual UInt32 getCompressedReserveSize(UInt32 uncompressed_size) const { return getHeaderSize() + getCompressedDataSize(uncompressed_size); }

    virtual UInt32 getAdditionalSizeAtTheEndOfBuffer() const { return 0; }

    static UInt8 getHeaderSize() { return COMPRESSED_BLOCK_HEADER_SIZE; }

    static UInt32 readCompressedBlockSize(const char * source);

    static UInt32 readDecompressedBlockSize(const char * source);

    static UInt8 readMethod(const char * source);

protected:

    virtual UInt32 getCompressedDataSize(UInt32 uncompressed_size) const { return uncompressed_size; }

    virtual UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const = 0;

    virtual void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const = 0;
};

}
