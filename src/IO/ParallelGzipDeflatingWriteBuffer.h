#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/CompressionMethod.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferDecorator.h>
#include <Common/threadPoolCallbackRunner.h>

#include <zlib.h>

#include <algorithm>
#include <memory>
#include <string>


namespace DB
{

/// Performs gzip (RFC 1952, https://datatracker.ietf.org/doc/html/rfc1952#section-2.3.1) compression,
/// deflating independent blocks in parallel on the shared IO thread pool, and writes the result to the
/// underlying `out` WriteBuffer. The output is an ordinary gzip stream readable by any decompressor;
/// only the writing side is parallelized. The parallel-block scheme follows pigz: https://zlib.net/pigz/pigz.pdf
class ParallelGzipDeflatingWriteBuffer : public WriteBufferWithOwnMemoryDecorator
{
public:
    /// Size of a block that is deflated independently. Blocks are compressed in parallel on the
    /// shared IO thread pool and then concatenated into a single raw-deflate stream.
    static constexpr size_t BLOCK_SIZE = 256 * 1024;

    /// Upper bound on the number of blocks staged (and therefore compressed concurrently) per pass,
    /// to keep memory bounded even with a very large thread count.
    static constexpr size_t MAX_STAGING_BLOCKS = 256;

    template <typename WriteBufferT>
    explicit ParallelGzipDeflatingWriteBuffer(
        WriteBufferT && out_,
        int compression_level_,
        size_t num_threads_,
        std::string filename_ = "")
        /// Stage about `num_threads` blocks per pass so a single parallel pass keeps the pool busy
        /// while bounding the working memory. NOLINT: std::move on a forwarding reference is intentional.
        : WriteBufferWithOwnMemoryDecorator(
              std::move(out_), /// NOLINT(bugprone-move-forwarding-reference)
              std::min(std::max<size_t>(num_threads_, 1), MAX_STAGING_BLOCKS) * BLOCK_SIZE)
        , compression_level(compression_level_)
        , filename(std::move(filename_))
    {
        writeHeader();
    }

private:
    struct CompressedBuf
    {
        std::shared_ptr<Memory<>> mem;
        size_t len = 0;
    };

    void nextImpl() override;

    void finalFlushBefore() override;

    void writeHeader();
    void writeTrailer();
    void deflateEngine(z_stream & strm, WriteBuffer & out_buf, int flush);
    CompressedBuf compressBlock(unsigned char * in_buf, size_t in_len, bool last_block_flag);
    static size_t calcCheck(const unsigned char * buf, size_t len);
    void compressAndWrite(unsigned char * in_buf, size_t in_len, bool final_compression_flag);

    int compression_level;
    std::string filename;
    uint64_t check = crc32_z(0L, Z_NULL, 0);
    uintmax_t ulen = 0;

    /// Runs block deflation on the shared IO thread pool (created lazily on first use).
    ThreadPoolCallbackRunnerUnsafe<CompressedBuf> runner;
};

}
