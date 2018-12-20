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

namespace DB
{

class ICompressionCodec;

using CompressionCodecPtr = std::shared_ptr<ICompressionCodec>;
using Codecs = std::vector<CompressionCodecPtr>;

class CompressionCodecReadBuffer;
class CompressionCodecWriteBuffer;

using CompressionCodecReadBufferPtr = std::shared_ptr<CompressionCodecReadBuffer>;
using CompressionCodecWriteBufferPtr = std::shared_ptr<CompressionCodecWriteBuffer>;

class CompressionCodecWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
public:
    CompressionCodecWriteBuffer(CompressionCodecPtr codec_, WriteBuffer & out_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE);

    ~CompressionCodecWriteBuffer() override;

private:
    void nextImpl() override;

private:
    WriteBuffer & out;
    CompressionCodecPtr codec;
    PODArray<char> compressed_buffer;
};

class CompressionCodecReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{

    UInt32 read_compressed_bytes_for_last_time = 0;

public:
    std::pair<UInt32, UInt32> readCompressedData();

    void decompress(char * to, UInt32 size_compressed);

    CompressionCodecReadBuffer(CompressionCodecPtr codec_, ReadBuffer & origin_);

    void seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block);
private:
    CompressionCodecPtr codec;
    ReadBuffer & origin;
    char * compressed_buffer;
    PODArray<char> own_compressed_buffer;

    bool nextImpl() override;
};

CompressionCodecReadBufferPtr liftCompressed(CompressionCodecPtr codec, ReadBuffer & origin);

CompressionCodecWriteBufferPtr liftCompressed(CompressionCodecPtr codec, WriteBuffer & origin);

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

    static UInt8 getHeaderSize() { return 1 + 8; }

    static UInt32 readCompressedBlockSize(const char * source);

    static UInt32 readDecompressedBlockSize(const char * source);

    static UInt8 readMethod(const char * source);

protected:

    virtual UInt32 getCompressedDataSize(UInt32 uncompressed_size) const { return uncompressed_size; }

    virtual UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const = 0;

    virtual void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const = 0;
};

}
