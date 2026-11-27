#include <Common/Exception.h>
#include <Common/filesystemHelpers.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Compression/CompressionFactory.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <base/unaligned.h>

#include <gtest/gtest.h>

#include <memory>


namespace DB::ErrorCodes
{
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int CORRUPTED_DATA;
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

/// A partially read block must be checked against its header like a fully read one.
TEST(CompressedReadBufferFromFile, partialReadChecksDecompressedSize)
{
    String data(512 * 1024, '\0');
    for (size_t i = 0; i < data.size(); ++i)
        data[i] = static_cast<char>((i * 2654435761u) >> 24);

    WriteBufferFromOwnString compressed;
    {
        CompressedWriteBuffer compressed_out(compressed, CompressionCodecFactory::instance().get("ZSTD", 1));
        compressed_out.write(data.data(), data.size());
        compressed_out.finalize();
    }
    compressed.finalize();
    String block = compressed.str();

    unalignedStoreLittleEndian<UInt32>(block.data() + 21, static_cast<UInt32>(data.size() + 1));

    auto tmp_file = createTemporaryFile("/tmp/");
    {
        WriteBufferFromFile out(tmp_file->path());
        out.write(block.data(), block.size());
        out.finalize();
    }

    CompressedReadBufferFromFile in(std::make_unique<ReadBufferFromFile>(tmp_file->path()));
    in.disableChecksumming();

    String prefix(1000, '\0');
    try
    {
        [[maybe_unused]] const auto bytes_read = in.readBig(prefix.data(), prefix.size());
        FAIL() << "Expected exception, but read " << bytes_read << " bytes";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::CORRUPTED_DATA);
    }
}

}
