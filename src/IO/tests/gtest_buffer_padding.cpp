#include "config.h"

#include <gtest/gtest.h>

#include <IO/BufferWithOwnMemory.h>
#include <IO/ParallelReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <base/scope_guard.h>

#if USE_SNAPPY
#include <snappy.h>
#include <IO/SnappyBasicReadBuffer.h>
#endif

#include <cstring>
#include <future>
#include <string>
#include <thread>
#include <type_traits>
#include <vector>

using namespace DB;

static_assert(!std::is_constructible_v<BufferBase::WorkingBuffer, char *, char *, bool>);
/// Mutating through the raw base would bypass the padding bookkeeping.
static_assert(!std::is_convertible_v<BufferBase::WorkingBuffer &, BufferBase::Buffer &>);
static_assert(std::is_convertible_v<const BufferBase::WorkingBuffer &, const BufferBase::Buffer &>);
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

/// Publishes the first `first_piece` bytes of a segment, then blocks until `release` before writing the rest.
class GatedReadAtBuffer : public SeekableReadBuffer
{
public:
    GatedReadAtBuffer(std::string data_, size_t first_piece_)
        : SeekableReadBuffer(nullptr, 0)
        , data(std::move(data_))
        , first_piece(first_piece_)
    {
    }

    bool supportsReadAt() override { return true; }
    off_t seek(off_t, int) override { return 0; }
    off_t getPosition() override { return 0; }

    size_t readBigAt(char * to, size_t n, size_t offset, const std::function<bool(size_t)> & progress_callback) const override
    {
        size_t first = std::min(first_piece, n);
        memcpy(to, data.data() + offset, first);
        if (progress_callback(first))
            return first;

        gate.wait();
        memcpy(to + first, data.data() + offset + first, n - first);
        progress_callback(n);
        return n;
    }

    void open()
    {
        if (!released)
        {
            released = true;
            release.set_value();
        }
    }

private:
    bool nextImpl() override { return false; }

    std::string data;
    size_t first_piece;
    std::promise<void> release;
    std::shared_future<void> gate = release.get_future().share();
    bool released = false;
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

TEST(BufferPadding, ParallelReadBufferPartialSegmentIsUnpadded)
{
    GatedReadAtBuffer input(std::string(64, 'x'), 16);

    std::vector<std::thread> threads;
    SCOPE_EXIT({
        for (auto & thread : threads)
            thread.join();
    });

    auto schedule = [&threads](std::function<void()> && job, Priority)
    {
        std::promise<void> done;
        auto future = done.get_future();
        threads.emplace_back([task = std::move(job), promise = std::move(done)]() mutable
        {
            task();
            promise.set_value();
        });
        return future;
    };

    ParallelReadBuffer buffer(input, schedule, 1, 64, 64);
    SCOPE_EXIT({ input.open(); });

    /// The worker is still writing the rest of the segment right after this range.
    ASSERT_TRUE(buffer.next());
    EXPECT_EQ(buffer.buffer().size(), 16);
    EXPECT_FALSE(buffer.isPadded());

    buffer.position() = buffer.buffer().end();
    input.open();
    ASSERT_TRUE(buffer.next());
    EXPECT_EQ(buffer.buffer().size(), 48);
    EXPECT_TRUE(buffer.isPadded());
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
