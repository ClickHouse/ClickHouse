#include <Common/filesystemHelpers.h>
#include <Compression/CompressionCodecNone.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>

#include <gtest/gtest.h>

#include <memory>
#include <random>
#include <vector>


namespace
{
using namespace DB;

/// Round-trips `data` through a CompressedWriteBuffer with the NONE codec and the zero-copy
/// (out_buffer_is_exclusive) path enabled, then reads it back and checks it is byte-identical.
/// `out_buf_size` controls the size of the output file buffer, which is what decides whether the
/// direct write window fits or the owned-buffer fallback kicks in. `write_chunk` controls how the
/// data is split across write() calls.
void roundTrip(const std::vector<char> & data, size_t block_size, size_t out_buf_size, size_t write_chunk)
{
    auto tmp_file = createTemporaryFile("/tmp/");

    {
        WriteBufferFromFile out(tmp_file->path(), out_buf_size);
        CompressedWriteBuffer compressed_out(
            out,
            std::make_shared<CompressionCodecNone>(),
            block_size,
            /*use_adaptive_buffer_size_=*/ false,
            DBMS_DEFAULT_INITIAL_ADAPTIVE_BUFFER_SIZE,
            /*out_buffer_is_exclusive_=*/ true);

        size_t written = 0;
        while (written < data.size())
        {
            size_t to_write = std::min(write_chunk, data.size() - written);
            compressed_out.write(data.data() + written, to_write);
            written += to_write;
        }

        compressed_out.finalize();
        out.finalize();
    }

    CompressedReadBufferFromFile in(std::make_unique<ReadBufferFromFile>(tmp_file->path()));
    std::vector<char> read_back(data.size());
    size_t bytes_read = data.empty() ? 0 : in.readBig(read_back.data(), read_back.size());

    ASSERT_EQ(bytes_read, data.size());
    ASSERT_EQ(read_back, data);
}

std::vector<char> makeData(size_t size)
{
    std::vector<char> data(size);
    std::mt19937 rng(static_cast<unsigned>(size * 2654435761u + 1));
    for (auto & c : data)
        c = static_cast<char>(rng());
    return data;
}

TEST(CompressedWriteBufferNone, RoundTripVariousSizes)
{
    /// Sizes chosen around the block size and the 25-byte service prefix to exercise the boundaries.
    const size_t block_size = 1024;
    for (size_t size : {size_t(0), size_t(1), size_t(24), size_t(25), size_t(26), size_t(1023),
                        size_t(1024), size_t(1025), size_t(4096), size_t(100000)})
    {
        auto data = makeData(size);
        /// Large output buffer: the zero-copy direct path is used.
        roundTrip(data, block_size, /*out_buf_size=*/ 1 << 20, /*write_chunk=*/ size ? size : 1);
        /// Output buffer equal to the block size: blocks fill it, forcing frequent out flushes.
        roundTrip(data, block_size, /*out_buf_size=*/ block_size, /*write_chunk=*/ 64);
    }
}

TEST(CompressedWriteBufferNone, TinyOutputBufferFallback)
{
    /// A very small output buffer cannot fit the 25-byte prefix plus data, so the writer must fall
    /// back to the owned buffer + copy path. The result must still be correct.
    auto data = makeData(50000);
    for (size_t out_buf_size : {size_t(1), size_t(10), size_t(25), size_t(26), size_t(40), size_t(128)})
        roundTrip(data, /*block_size=*/ 8192, out_buf_size, /*write_chunk=*/ 333);
}

TEST(CompressedWriteBufferNone, ChunkedWritesAcrossBlocks)
{
    /// Many small writes that cross block and output-buffer boundaries.
    auto data = makeData(1 << 18);
    for (size_t chunk : {size_t(1), size_t(7), size_t(63), size_t(997), size_t(65536)})
        roundTrip(data, /*block_size=*/ 16384, /*out_buf_size=*/ 65536, chunk);
}

}
