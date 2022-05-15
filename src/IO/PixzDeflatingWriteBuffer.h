#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferDecorator.h>
#include <Common/ThreadPool.h>

#include <lzma.h>


namespace DB
{

/// Performs compression using lzma library and writes compressed data to out_ WriteBuffer.
class PixzDeflatingWriteBuffer : public WriteBufferWithOwnMemoryDecorator
{
public:
    PixzDeflatingWriteBuffer(
        std::unique_ptr<WriteBuffer> out_,
        int compression_level_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr,
        size_t alignment = 0);

    ~PixzDeflatingWriteBuffer() override;

private:
    struct CompressedBuf {
        std::shared_ptr<Memory<>> mem;
        size_t len;
        lzma_block block;
    };
    
    size_t LZMA_CHUNK_MAX = 1024;
    size_t CHUNKSIZE = 4 * 1024;
    size_t BLOCK_SIZE = 32 * 1024 * 1024;
    
    void nextImpl() override;

    void finalizeBefore() override;
    void finalizeAfter() override;

    double gBlockFraction = 2;
    size_t gBlockInSize = 0;
    size_t gBlockOutSize = 0;
    lzma_index *gIndex = nullptr;
    lzma_stream gStream = LZMA_STREAM_INIT;
    lzma_options_lzma lzma_opts;
    lzma_filter gFilters[LZMA_FILTERS_MAX + 1];

    ThreadPool pool;
    
    void writeHeader();
    CompressedBuf compressBlock(uint8_t * block_buf, size_t block_len);
    lzma_block createBlock(size_t block_len);
    size_t calcCompressedSize(size_t block_len);
    void encodeUncompressible(lzma_block *block, uint8_t* in_data, size_t in_size, uint8_t* out_data);
    void writeTrailer(lzma_vli backward_size);
};

}
