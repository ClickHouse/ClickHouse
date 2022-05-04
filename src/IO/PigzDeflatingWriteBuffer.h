#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferDecorator.h>

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
        std::string filename_ = "",
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~PigzDeflatingWriteBuffer() override;

private:
    void nextImpl() override;

    virtual void finalizeBefore() override;
    virtual void finalizeAfter() override;

    struct CompressedBuf {
        std::shared_ptr<Memory<>> mem;
        size_t len;
    };

    void writeHeader();
    void writeTrailer(uintmax_t ulen, uint64_t check);
    static void deflateEngine(z_stream & strm, WriteBuffer & out_buf, int flush);
    CompressedBuf compressSlice(unsigned char * in_buf, size_t in_len, bool last_block_flag);
    static size_t calcCheck(unsigned char * buf, size_t len);

    int compression_level;
    std::string filename;
};

}
