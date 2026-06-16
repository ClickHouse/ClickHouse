#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferDecorator.h>
#include <Common/threadPoolCallbackRunner.h>

#include <lzma.h>

#include <memory>


namespace DB
{

/// Performs xz (LZMA2) compression, encoding independent blocks in parallel on the shared IO
/// thread pool and writing the result to the underlying `out` WriteBuffer. The parallel-block
/// layout follows pixz: https://github.com/vasi/pixz
class PixzDeflatingWriteBuffer : public WriteBufferWithOwnMemoryDecorator
{
public:
    template <typename WriteBufferT>
    explicit PixzDeflatingWriteBuffer(
        WriteBufferT && out_,
        int compression_level_,
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        char * existing_memory = nullptr, /// NOLINT(readability-non-const-parameter)
        size_t alignment = 0)
        : WriteBufferWithOwnMemoryDecorator(std::move(out_), buf_size, existing_memory, alignment) /// NOLINT(bugprone-move-forwarding-reference)
    {
        init(compression_level_);
    }

    ~PixzDeflatingWriteBuffer() override;

private:
    struct CompressedBuf
    {
        std::shared_ptr<Memory<>> mem;
        size_t len = 0;
        lzma_block block;
    };

    static constexpr size_t LZMA_CHUNK_MAX = 1024;
    static constexpr size_t CHUNKSIZE = 4 * 1024;
    static constexpr size_t BLOCK_SIZE = 32 * 1024 * 1024;

    /// Set up the lzma encoder state and write the stream header. Called from the constructor.
    void init(int compression_level);

    void nextImpl() override;
    void finalFlushBefore() override;

    void writeHeader();
    CompressedBuf compressBlock(uint8_t * block_buf, size_t block_len);
    lzma_block createBlock(size_t block_len);
    static size_t calcCompressedSize(size_t block_len);
    void encodeUncompressible(lzma_block * block, uint8_t * in_data, size_t in_size, uint8_t * out_data);
    void writeTrailer(lzma_vli backward_size);

    double gBlockFraction = 2;
    size_t gBlockInSize = 0;
    size_t gBlockOutSize = 0;
    lzma_index * gIndex = nullptr;
    lzma_stream gStream = LZMA_STREAM_INIT;
    lzma_options_lzma lzma_opts;
    lzma_filter gFilters[LZMA_FILTERS_MAX + 1];

    /// Runs block encoding on the shared IO thread pool (created lazily on first use).
    ThreadPoolCallbackRunnerUnsafe<CompressedBuf> runner;
};

}
