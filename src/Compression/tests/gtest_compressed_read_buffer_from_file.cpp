#include <Common/Exception.h>
#include <Common/filesystemHelpers.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>

#include <gtest/gtest.h>

#include <fstream>
#include <memory>
#include <random>


namespace DB::ErrorCodes
{
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int CHECKSUM_DOESNT_MATCH;
}

namespace
{
using namespace DB;

TEST(CompressedReadBufferFromFile, readBigChecksSeekOffsetBounds)
{
    auto tmp_file = createTemporaryFile("/tmp/");
    {
        WriteBufferFromFile out(tmp_file->path());
        CompressedWriteBuffer compressed_out(out);
        compressed_out.write("abc", 3);
        compressed_out.finalize();
        out.finalize();
    }

    CompressedReadBufferFromFile in(std::make_unique<ReadBufferFromFile>(tmp_file->path()));
    in.seek(0, 4);

    char byte = 0;
    try
    {
        [[maybe_unused]] const auto bytes_read = in.readBig(&byte, 1);
        FAIL() << "Expected exception, but read " << bytes_read << " bytes";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);
    }
}

std::string randomBytes(size_t size, UInt64 seed)
{
    std::mt19937_64 rng(seed);
    std::string res(size, '\0');
    for (auto & c : res)
        c = static_cast<char>(rng());
    return res;
}

/// Writes `data` as one CODEC(NONE) block per element of `block_sizes`.
void writeNoneBlocks(const std::string & path, const std::string & data, const std::vector<size_t> & block_sizes)
{
    WriteBufferFromFile out(path);
    CompressedWriteBuffer compressed_out(out, CompressionCodecFactory::instance().get("NONE", {}), 2 << 20);
    size_t written = 0;
    for (size_t block_size : block_sizes)
    {
        compressed_out.write(data.data() + written, block_size);
        compressed_out.next();
        written += block_size;
    }
    compressed_out.finalize();
    out.finalize();
}

template <typename CompressedBuffer>
std::unique_ptr<CompressedBuffer> openNoneBlocks(const std::string & path, size_t buffer_size)
{
    return std::make_unique<CompressedBuffer>(std::make_unique<ReadBufferFromFile>(path, buffer_size));
}

template <typename CompressedBuffer>
void checkReadBigOfNoneBlocks(const std::string & path, const std::string & data)
{
    for (size_t buffer_size : {16UL, 1025UL, 4097UL, 70001UL, 2UL << 20})
    {
        SCOPED_TRACE(buffer_size);
        {
            auto in = openNoneBlocks<CompressedBuffer>(path, buffer_size);
            std::string res(data.size(), '\0');
            EXPECT_EQ(in->readBig(res.data(), res.size()), data.size());
            EXPECT_EQ(in->count(), data.size());
            EXPECT_TRUE(in->eof());
            EXPECT_EQ(res, data);
        }
        {
            auto in = openNoneBlocks<CompressedBuffer>(path, buffer_size);
            std::string res;
            std::string chunk(100000, '\0');
            while (size_t bytes_read = in->readBig(chunk.data(), chunk.size()))
            {
                res.append(chunk.data(), bytes_read);
                EXPECT_EQ(in->count(), res.size());
            }
            EXPECT_EQ(res, data);
        }
    }
}

TEST(CompressedReadBufferFromFile, readBigReadsNoneBlocksStraightIntoDestination)
{
    /// Blocks smaller than, equal to and larger than the 16 bytes the split checksum needs, and than the read buffers.
    const std::vector<size_t> block_sizes = {3, 6, 7, 8, 200, 4096, 70000, 1 << 20};
    size_t total_size = 0;
    for (size_t block_size : block_sizes)
        total_size += block_size;
    const std::string data = randomBytes(total_size, 42);

    auto tmp_file = createTemporaryFile("/tmp/");
    writeNoneBlocks(tmp_file->path(), data, block_sizes);

    checkReadBigOfNoneBlocks<CompressedReadBufferFromFile>(tmp_file->path(), data);
    checkReadBigOfNoneBlocks<CompressedReadBuffer>(tmp_file->path(), data);
}

TEST(CompressedReadBufferFromFile, readBigNoneBlockChecksumMismatchIsReported)
{
    constexpr size_t checksum_and_header_size = 16 + 9;
    const std::string data = randomBytes(300000, 43);

    /// Inside the 16 bytes that are assembled for the checksum, and after them.
    for (size_t data_offset : {3UL, 1000UL})
    {
        SCOPED_TRACE(data_offset);
        auto tmp_file = createTemporaryFile("/tmp/");
        writeNoneBlocks(tmp_file->path(), data, {data.size()});
        {
            std::fstream file(tmp_file->path(), std::ios::in | std::ios::out | std::ios::binary);
            file.seekg(checksum_and_header_size + data_offset);
            char c = 0;
            file.get(c);
            file.seekp(checksum_and_header_size + data_offset);
            file.put(static_cast<char>(c ^ 1));
        }

        auto error_message = [&](bool read_big)
        {
            CompressedReadBufferFromFile in(std::make_unique<ReadBufferFromFile>(tmp_file->path(), 4097));
            std::string res(data.size(), '\0');
            try
            {
                if (read_big)
                    in.readBigStrict(res.data(), res.size());
                else
                    in.readStrict(res.data(), res.size());
            }
            catch (const Exception & e)
            {
                EXPECT_EQ(e.code(), ErrorCodes::CHECKSUM_DOESNT_MATCH);
                return e.message();
            }
            ADD_FAILURE() << "Expected an exception";
            return std::string{};
        };

        const std::string read_big_message = error_message(true);
        EXPECT_NE(read_big_message.find(fmt::format("single bit flip in data block at byte {}, bit 0", 9 + data_offset)), std::string::npos)
            << read_big_message;
        EXPECT_EQ(read_big_message, error_message(false));
    }
}

}
