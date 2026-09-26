#include <Common/Exception.h>
#include <Common/filesystemHelpers.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <Compression/CompressedWriteBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>

#include <gtest/gtest.h>

#include <memory>


namespace DB::ErrorCodes
{
    extern const int SEEK_POSITION_OUT_OF_BOUND;
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

}
