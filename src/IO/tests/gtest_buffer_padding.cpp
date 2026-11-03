#include "config.h"

#include <gtest/gtest.h>

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>

#if USE_SNAPPY
#include <snappy.h>
#include <IO/SnappyBasicReadBuffer.h>
#endif

#include <string>
#include <type_traits>

using namespace DB;

static_assert(!std::is_constructible_v<BufferBase::WorkingBuffer, char *, char *, bool>);
static_assert(PaddedBufferStorage<Memory<>>);
static_assert(!PaddedBufferStorage<char *>);
static_assert(!PaddedBufferStorage<std::string>);

namespace
{

class PaddingTestReadBuffer : public BufferWithOwnMemory<ReadBuffer>
{
public:
    PaddingTestReadBuffer()
        : BufferWithOwnMemory<ReadBuffer>(32)
    {
    }

    void exposeOwnMemory()
    {
        working_buffer = WorkingBuffer::fromPaddedStorage(memory, 0, memory.size());
        pos = working_buffer.begin();
    }

    void exposeRawMemory(char * data, size_t size)
    {
        working_buffer = Buffer(data, data + size);
        pos = working_buffer.begin();
    }

    void exposeOwnInternalMemory()
    {
        setPaddedInternalBuffer(memory, 0, memory.size());
        setWorkingBufferFromInternal();
        pos = working_buffer.begin();
    }

    void exposeRawInternalMemory(char * data, size_t size)
    {
        setInternalBuffer(Buffer(data, data + size));
        setWorkingBufferFromInternal();
        pos = working_buffer.begin();
    }

    void exposeBuffer(ReadBuffer & source)
    {
        working_buffer = source.buffer();
        pos = source.position();
    }

private:
    bool nextImpl() override { return false; }
};

}

TEST(BufferPadding, RawRangesAreUnpaddedByDefault)
{
    Memory<> empty_memory;
    EXPECT_FALSE(BufferBase::WorkingBuffer::fromPaddedStorage(empty_memory, 0, 0).isPadded());

    PaddingTestReadBuffer buffer;
    buffer.exposeOwnMemory();
    ASSERT_TRUE(buffer.isPadded());

    buffer.buffer().resize(16);
    EXPECT_TRUE(buffer.isPadded());
    buffer.buffer().resize(32);
    EXPECT_TRUE(buffer.isPadded());
    buffer.buffer().resize(33);
    EXPECT_FALSE(buffer.isPadded());

    std::string exact_memory(32, 'x');
    buffer.exposeRawMemory(exact_memory.data(), exact_memory.size());
    EXPECT_FALSE(buffer.isPadded());

    buffer.exposeOwnMemory();
    buffer.set(exact_memory.data(), exact_memory.size());
    EXPECT_FALSE(buffer.isPadded());

    buffer.exposeOwnInternalMemory();
    ASSERT_TRUE(buffer.isPadded());

    buffer.exposeRawInternalMemory(exact_memory.data(), exact_memory.size());
    EXPECT_FALSE(buffer.isPadded());
}

TEST(BufferPadding, ForwardingAndSwapPreserveTheProperty)
{
    PaddingTestReadBuffer padded;
    padded.exposeOwnMemory();

    PaddingTestReadBuffer forwarded;
    forwarded.exposeBuffer(padded);
    EXPECT_TRUE(forwarded.isPadded());

    std::string exact_memory(32, 'x');
    PaddingTestReadBuffer unpadded;
    unpadded.exposeRawMemory(exact_memory.data(), exact_memory.size());

    padded.swap(unpadded);
    EXPECT_FALSE(padded.isPadded());
    EXPECT_TRUE(unpadded.isPadded());
}

#if USE_SNAPPY
TEST(BufferPadding, SnappyBasicOutputIsUnpadded)
{
    std::string compressed;
    snappy::Compress("1.25", 4, &compressed);

    auto source = std::make_unique<ReadBufferFromMemory>(std::string_view(compressed));
    SnappyBasicReadBuffer decompressed(std::move(source));

    ASSERT_TRUE(decompressed.next());
    EXPECT_FALSE(decompressed.isPadded());
}
#endif
