#include <base/defines.h>
#include <base/unaligned.h>
#include <Common/filesystemHelpers.h>
#include <Compression/CompressionCodecNone.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <random>
#include <vector>


namespace
{
using namespace DB;

/// Round-trips `data` through a CompressedWriteBuffer with the NONE codec and the zero-copy
/// path enabled (declareOutBufferExclusive), then reads it back and checks it is byte-identical.
/// `out_buf_size` controls the size of the output file buffer, which is what decides whether the
/// direct write window fits or the owned-buffer fallback kicks in. `write_chunk` controls how the
/// data is split across write() calls.
void roundTrip(const std::vector<char> & data, size_t block_size, size_t out_buf_size, size_t write_chunk)
{
    auto tmp_file = createTemporaryFile("/tmp/");

    {
        WriteBufferFromFile out(tmp_file->path(), out_buf_size);
        CompressedWriteBuffer compressed_out(out, std::make_shared<CompressionCodecNone>(), block_size);
        compressed_out.declareOutBufferExclusive();

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

/// Writes `data` through a CompressedWriteBuffer with the NONE codec and the zero-copy path enabled,
/// then returns the uncompressed size of every frame found in the resulting file.
std::vector<size_t> writeAndGetFrameSizes(
    const std::vector<char> & data, size_t block_size, size_t out_buf_size, bool use_adaptive_buffer_size)
{
    auto tmp_file = createTemporaryFile("/tmp/");

    {
        WriteBufferFromFile out(tmp_file->path(), out_buf_size);
        CompressedWriteBuffer compressed_out(
            out, std::make_shared<CompressionCodecNone>(), block_size, use_adaptive_buffer_size, DBMS_DEFAULT_INITIAL_ADAPTIVE_BUFFER_SIZE);
        compressed_out.declareOutBufferExclusive();

        compressed_out.write(data.data(), data.size());
        compressed_out.finalize();
        out.finalize();
    }

    std::vector<size_t> frame_sizes;

    ReadBufferFromFile in(tmp_file->path());
    while (!in.eof())
    {
        in.ignore(sizeof(CityHash_v1_0_2::uint128));

        char header[ICompressionCodec::getHeaderSize()];
        in.readStrict(header, sizeof(header));

        UInt32 compressed_size = unalignedLoadLittleEndian<UInt32>(&header[1]);
        UInt32 uncompressed_size = unalignedLoadLittleEndian<UInt32>(&header[5]);

        frame_sizes.push_back(uncompressed_size);
        in.ignore(compressed_size - ICompressionCodec::getHeaderSize());
    }

    return frame_sizes;
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

TEST(CompressedWriteBufferNone, BlockSizeIsHonoredForAnyOutputBufferSize)
{
    /// The zero-copy path must never shrink the configured block size: it is used only while the
    /// nested buffer can expose a window for a whole block plus the service prefix, and otherwise
    /// the data goes through the owned buffer. Backends that start with a smaller buffer than the
    /// requested one (adaptive local files, S3 and Azure blob writers) exercise exactly this case.
    const size_t block_size = 65536;
    const size_t data_size = 10 * block_size;
    auto data = makeData(data_size);

    for (size_t out_buf_size : {size_t(4096),                                    /// far too small
                                block_size,                                      /// one prefix short
                                block_size + COMPRESSED_BLOCK_PREFIX_SIZE, /// exact fit
                                size_t(1) << 20})                                /// plenty of room
    {
        auto frame_sizes = writeAndGetFrameSizes(data, block_size, out_buf_size, /*use_adaptive_buffer_size=*/ false);

        ASSERT_EQ(frame_sizes.size(), data_size / block_size) << "out_buf_size = " << out_buf_size;
        for (size_t frame_size : frame_sizes)
            ASSERT_EQ(frame_size, block_size) << "out_buf_size = " << out_buf_size;
    }
}

TEST(CompressedWriteBufferNone, AdaptiveBlockSizeGrowsToTheMaximum)
{
    /// With the adaptive buffer size the block size ramps up from the initial size to the maximum,
    /// on the zero-copy path as well as on the owned-buffer path.
    const size_t block_size = 1 << 20;
    auto data = makeData(16 * block_size);

    for (size_t out_buf_size : {size_t(4096), block_size + COMPRESSED_BLOCK_PREFIX_SIZE})
    {
        auto frame_sizes = writeAndGetFrameSizes(data, block_size, out_buf_size, /*use_adaptive_buffer_size=*/ true);

        ASSERT_FALSE(frame_sizes.empty());
        for (size_t frame_size : frame_sizes)
            ASSERT_LE(frame_size, block_size) << "out_buf_size = " << out_buf_size;

        /// The ramp must reach the maximum and stay there: every frame but the last (which is
        /// whatever is left over) is a full block by the time the maximum is reached.
        ASSERT_EQ(*std::max_element(frame_sizes.begin(), frame_sizes.end()), block_size)
            << "out_buf_size = " << out_buf_size;
        ASSERT_EQ(frame_sizes[frame_sizes.size() - 2], block_size) << "out_buf_size = " << out_buf_size;
    }
}

TEST(CompressedWriteBufferNone, ChunkedWritesAcrossBlocks)
{
    /// Many small writes that cross block and output-buffer boundaries.
    auto data = makeData(1 << 18);
    for (size_t chunk : {size_t(1), size_t(7), size_t(63), size_t(997), size_t(65536)})
        roundTrip(data, /*block_size=*/ 16384, /*out_buf_size=*/ 65536, chunk);
}

TEST(CompressedWriteBufferNone, ViolatedExclusivityIsDetected)
{
    /// The exclusivity declared by declareOutBufferExclusive is enforced in release builds too:
    /// the working buffer aliases `out`, so a foreign write while a block is assembled in place
    /// must abort the block instead of committing a well-formed frame of wrong bytes.
#ifdef DEBUG_OR_SANITIZER_BUILD
    GTEST_SKIP() << "this test triggers LOGICAL_ERROR, runs only if DEBUG_OR_SANITIZER_BUILD is not defined";
#else
    auto tmp_file = createTemporaryFile("/tmp/");
    DB::WriteBufferFromFile out(tmp_file->path(), 1 << 20);
    DB::CompressedWriteBuffer compressed_out(out, std::make_shared<DB::CompressionCodecNone>(), 1024);
    compressed_out.declareOutBufferExclusive();

    compressed_out.write("hello", 5);

    /// Someone else writes to `out` even though it was declared exclusive.
    out.write("x", 1);

    EXPECT_THROW(compressed_out.next(), DB::Exception);
    EXPECT_TRUE(compressed_out.isCanceled());
#endif
}

}
