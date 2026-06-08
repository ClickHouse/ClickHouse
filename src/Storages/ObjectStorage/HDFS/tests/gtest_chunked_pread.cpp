#include <Storages/ObjectStorage/HDFS/chunkedPread.h>

#include <gtest/gtest.h>

#include <cstring>
#include <vector>


using namespace DB;

namespace
{

/// A fake positioned-read source: it owns `content` and serves chunked reads from it,
/// recording the size of every chunk it is asked for so a test can assert that no single
/// chunk exceeds the cap. Each call copies the requested bytes (clamped to what is left)
/// and returns the number of bytes copied, mimicking `hdfsPread`'s short-read semantics.
struct FakeReader
{
    std::vector<char> content;
    std::vector<size_t> requested_sizes;

    size_t operator()(char * chunk_buffer, size_t chunk_size, size_t chunk_offset)
    {
        requested_sizes.push_back(chunk_size);

        if (chunk_offset >= content.size())
            return 0;

        const size_t available = std::min(chunk_size, content.size() - chunk_offset);
        memcpy(chunk_buffer, content.data() + chunk_offset, available);
        return available;
    }
};

std::vector<char> makeContent(size_t size)
{
    std::vector<char> content(size);
    for (size_t i = 0; i < size; ++i)
        content[i] = static_cast<char>(i & 0xFF);
    return content;
}

}

TEST(ChunkedPread, SplitsLargeReadIntoCappedChunks)
{
    constexpr size_t size = 1000;
    constexpr size_t max_chunk = 256;

    FakeReader reader{makeContent(size), {}};
    std::vector<char> out(size, 0);

    const size_t total = chunkedPread(out.data(), size, 0, max_chunk, std::ref(reader));

    EXPECT_EQ(total, size);
    EXPECT_EQ(out, reader.content);

    /// Without the chunk loop a single read of `size` would be issued, exceeding the cap.
    ASSERT_EQ(reader.requested_sizes.size(), 4u);
    for (size_t requested : reader.requested_sizes)
        EXPECT_LE(requested, max_chunk);
    EXPECT_EQ(reader.requested_sizes.back(), size % max_chunk);
}

TEST(ChunkedPread, ExactMultipleOfCap)
{
    constexpr size_t size = 512;
    constexpr size_t max_chunk = 256;

    FakeReader reader{makeContent(size), {}};
    std::vector<char> out(size, 0);

    const size_t total = chunkedPread(out.data(), size, 0, max_chunk, std::ref(reader));

    EXPECT_EQ(total, size);
    EXPECT_EQ(out, reader.content);
    /// Exactly two full chunks; the loop stops once `total_read == size` without issuing an
    /// extra read.
    ASSERT_EQ(reader.requested_sizes.size(), 2u);
    EXPECT_EQ(reader.requested_sizes[0], max_chunk);
    EXPECT_EQ(reader.requested_sizes[1], max_chunk);
}

TEST(ChunkedPread, SingleReadWhenSmallerThanCap)
{
    constexpr size_t size = 100;
    constexpr size_t max_chunk = 256;

    FakeReader reader{makeContent(size), {}};
    std::vector<char> out(size, 0);

    const size_t total = chunkedPread(out.data(), size, 0, max_chunk, std::ref(reader));

    EXPECT_EQ(total, size);
    EXPECT_EQ(out, reader.content);
    ASSERT_EQ(reader.requested_sizes.size(), 1u);
    EXPECT_EQ(reader.requested_sizes[0], size);
}

TEST(ChunkedPread, ShortReadStopsTheLoop)
{
    constexpr size_t size = 1000;
    constexpr size_t max_chunk = 256;

    /// The source only has 300 bytes, fewer than the requested 1000.
    FakeReader reader{makeContent(300), {}};
    std::vector<char> out(size, 0);

    const size_t total = chunkedPread(out.data(), size, 0, max_chunk, std::ref(reader));

    EXPECT_EQ(total, 300u);
    /// First chunk: 256 bytes (full), second chunk: 44 bytes (< requested 256) -> stop.
    ASSERT_EQ(reader.requested_sizes.size(), 2u);
    EXPECT_EQ(reader.requested_sizes[0], max_chunk);
    EXPECT_EQ(reader.requested_sizes[1], max_chunk);
}

TEST(ChunkedPread, OffsetIsAdvancedPerChunk)
{
    constexpr size_t size = 600;
    constexpr size_t max_chunk = 200;
    constexpr size_t base_offset = 50;

    /// Source large enough to cover base_offset + size, with distinct bytes so that a
    /// mis-advanced offset would corrupt the output.
    FakeReader reader{makeContent(base_offset + size), {}};
    std::vector<char> out(size, 0);

    const size_t total = chunkedPread(out.data(), size, base_offset, max_chunk, std::ref(reader));

    EXPECT_EQ(total, size);
    for (size_t i = 0; i < size; ++i)
        EXPECT_EQ(out[i], reader.content[base_offset + i]) << "mismatch at byte " << i;
}

TEST(ChunkedPread, ZeroSizeReadsNothing)
{
    FakeReader reader{makeContent(100), {}};
    char dummy = 0;

    const size_t total = chunkedPread(&dummy, 0, 0, 256, std::ref(reader));

    EXPECT_EQ(total, 0u);
    EXPECT_TRUE(reader.requested_sizes.empty());
}
