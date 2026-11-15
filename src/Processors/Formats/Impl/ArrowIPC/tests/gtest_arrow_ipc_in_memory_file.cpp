#include <gtest/gtest.h>
#include <config.h>
#if USE_ARROW

#include <IO/ReadHelpers.h>
#include <IO/SeekableReadBuffer.h>
#include <Processors/Formats/Impl/ArrowIPC/MessageReader.h>

using namespace DB;

namespace
{

/// The chunks are 64 KiB, 128 KiB, 256 KiB, ...
void checkReadBack(size_t size, bool call_next)
{
    std::string data(size, '\0');
    for (size_t i = 0; i < size; ++i)
        data[i] = static_cast<char>(i % 251);

    ArrowIPC::InMemoryFile file;
    file.write(data.data(), size);
    if (call_next)
        file.next();
    file.finalize();
    EXPECT_EQ(file.count(), size);

    std::string read;
    readStringUntilEOF(read, *file.getSeekableReadBuffer());
    EXPECT_EQ(read, data);
}

}

TEST(ArrowIPCInMemoryFile, PartialLastChunk)
{
    checkReadBack((192 << 10) + 1000, false);
}

TEST(ArrowIPCInMemoryFile, FullLastChunk)
{
    checkReadBack(192 << 10, false);
}

TEST(ArrowIPCInMemoryFile, EmptyLastChunkAfterNext)
{
    checkReadBack(192 << 10, true);
}

#endif
