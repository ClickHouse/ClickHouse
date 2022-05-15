#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferDecorator.h>
#include <Common/ThreadPool.h>

#include <zlib.h>


namespace DB
{

/// Performs compression using zlib library, compress data in parallel and writes it to out_ WriteBuffer.
class PigzDeflatingWriteBuffer : public WriteBufferWithOwnMemoryDecorator
{
public:
    PigzDeflatingWriteBuffer(
        std::unique_ptr<WriteBuffer> out_,
        int compression_level_,
        std::string filename_ = "");

    ~PigzDeflatingWriteBuffer() override;

private:
    void nextImpl() override;

    virtual void finalizeBefore() override;
    virtual void finalizeAfter() override;

    struct CompressedBuf {
        std::shared_ptr<Memory<>> mem;
        size_t len;
    };

    void compressAndWrite(unsigned char * in_buf, size_t in_len, bool final_compression_flag);
    void writeHeader();
    void writeTrailer();
    void deflateEngine(z_stream & strm, WriteBuffer & out_buf, int flush);
    CompressedBuf compressSlice(unsigned char * in_buf, size_t in_len, bool last_block_flag);
    size_t calcCheck(unsigned char * buf, size_t len);

    int compression_level;
    std::string filename;
    uint64_t check = crc32_z(0L, Z_NULL, 0);
    uintmax_t ulen = 0;
    ThreadPool pool;
};

}
