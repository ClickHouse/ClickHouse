#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/BufferWithOwnMemoryImpl.h>
#include <gtest/gtest.h>
#include <Common/ErrnoException.h>

#define EXPECT_THROW_ERROR_CODE(statement, expected_exception, expected_code)    \
    EXPECT_THROW(                                                                \
        try                                                                      \
        {                                                                        \
            statement;                                                           \
        }                                                                        \
        catch (const expected_exception & e)                                     \
        {                                                                        \
            EXPECT_EQ(expected_code, e.code());                                  \
            throw;                                                               \
        }                                                                        \
    , expected_exception)

namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_ALLOCATE_MEMORY;
}

}

using namespace DB;

class DummyAllocator
{
    void * dummy_address = reinterpret_cast<void *>(1);

public:
    void * alloc(size_t size, size_t /*alignment*/ = 0)
    {
        checkSize(size);
        if (size)
            return dummy_address;
        else
            return nullptr;
    }

    void * realloc(void * /*buf*/, size_t /*old_size*/, size_t new_size, size_t /*alignment*/ = 0)
    {
        checkSize(new_size);
        return dummy_address;
    }

    void free([[maybe_unused]] void * buf, size_t /*size*/, size_t /*alignment*/ = 0)
    {
        chassert(buf == dummy_address);
    }

    // the same check as in Common/Allocator.h
    void static checkSize(size_t size)
    {
        /// More obvious exception in case of possible overflow (instead of just "Cannot mmap").
        if (size >= 0x8000000000000000ULL)
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Too large size ({}) passed to allocator. It indicates an error.", size);
    }
};

TEST(MemoryResizeTest, SmallInitAndSmallResize)
{
    {
        auto memory = Memory<DummyAllocator>(0);
        ASSERT_EQ(memory.m_data, nullptr);
        ASSERT_EQ(memory.m_capacity, 0);
        ASSERT_EQ(memory.m_size, 0);

        memory.resize(0);
        ASSERT_EQ(memory.m_data, nullptr);
        ASSERT_EQ(memory.m_capacity, 0);
        ASSERT_EQ(memory.m_size, 0);

        memory.resize(1);
        ASSERT_TRUE(memory.m_data);
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD);
        ASSERT_EQ(memory.m_size, 1);
    }

    {
        auto memory = Memory<DummyAllocator>(1);
        ASSERT_TRUE(memory.m_data);
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD);
        ASSERT_EQ(memory.m_size, 1);

        memory.resize(0);
        ASSERT_TRUE(memory.m_data);
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD);
        ASSERT_EQ(memory.m_size, 0);

        memory.resize(1);
        ASSERT_TRUE(memory.m_data);
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD);
        ASSERT_EQ(memory.m_size, 1);
    }
}

TEST(MemoryResizeTest, SmallInitAndBigResizeOverflowWhenPadding)
{
    {
        auto memory = Memory<DummyAllocator>(0);
        ASSERT_EQ(memory.m_data, nullptr);
        ASSERT_EQ(memory.m_capacity, 0);
        ASSERT_EQ(memory.m_size, 0);

        EXPECT_THROW_ERROR_CODE(memory.resize(std::numeric_limits<size_t>::max()), Exception, ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        ASSERT_EQ(memory.m_data, nullptr); // state is intact after exception
        ASSERT_EQ(memory.m_size, 0);
        ASSERT_EQ(memory.m_capacity, 0);

        memory.resize(1);
        ASSERT_TRUE(memory.m_data);
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD);
        ASSERT_EQ(memory.m_size, 1);

        memory.resize(2);
        ASSERT_TRUE(memory.m_data);
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD + 1);
        ASSERT_EQ(memory.m_size, 2);

        EXPECT_THROW_ERROR_CODE(memory.resize(std::numeric_limits<size_t>::max()), Exception, ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        ASSERT_TRUE(memory.m_data); // state is intact after exception
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD + 1);
        ASSERT_EQ(memory.m_size, 2);

        memory.resize(0x8000000000000000ULL - PADDING_FOR_SIMD);
        ASSERT_TRUE(memory.m_data);
        ASSERT_EQ(memory.m_capacity, 0x8000000000000000ULL - 1);
        ASSERT_EQ(memory.m_size, 0x8000000000000000ULL - PADDING_FOR_SIMD);

#ifndef DEBUG_OR_SANITIZER_BUILD
        EXPECT_THROW_ERROR_CODE(memory.resize(0x8000000000000000ULL - (PADDING_FOR_SIMD - 1)), Exception, ErrorCodes::LOGICAL_ERROR);
        ASSERT_TRUE(memory.m_data);  // state is intact after exception
        ASSERT_EQ(memory.m_capacity, 0x8000000000000000ULL - 1);
        ASSERT_EQ(memory.m_size, 0x8000000000000000ULL - PADDING_FOR_SIMD);
#endif
    }

    {
        auto memory = Memory<DummyAllocator>(1);
        ASSERT_TRUE(memory.m_data);
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD);
        ASSERT_EQ(memory.m_size, 1);

        EXPECT_THROW_ERROR_CODE(memory.resize(std::numeric_limits<size_t>::max()), Exception, ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        ASSERT_TRUE(memory.m_data); // state is intact after exception
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD);
        ASSERT_EQ(memory.m_size, 1);

        memory.resize(1);
        ASSERT_TRUE(memory.m_data);
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD);
        ASSERT_EQ(memory.m_size, 1);

#ifndef DEBUG_OR_SANITIZER_BUILD
        EXPECT_THROW_ERROR_CODE(memory.resize(0x8000000000000000ULL - (PADDING_FOR_SIMD - 1)), Exception, ErrorCodes::LOGICAL_ERROR);
        ASSERT_TRUE(memory.m_data); // state is intact after exception
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD);
        ASSERT_EQ(memory.m_size, 1);
#endif
    }
}


TEST(MemoryResizeTest, BigInitAndSmallResizeOverflowWhenPadding)
{
    {
        EXPECT_THROW_ERROR_CODE(
        {
            auto memory = Memory<DummyAllocator>(std::numeric_limits<size_t>::max());
        }
        , Exception
        , ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    {
        EXPECT_THROW_ERROR_CODE(
        {
            auto memory = Memory<DummyAllocator>(std::numeric_limits<size_t>::max() - 1);
        }
        , Exception
        , ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

    {
        EXPECT_THROW_ERROR_CODE(
        {
            auto memory = Memory<DummyAllocator>(std::numeric_limits<size_t>::max() - 10);
        }
        , Exception
        , ErrorCodes::ARGUMENT_OUT_OF_BOUND);
    }

#ifndef DEBUG_OR_SANITIZER_BUILD
    {
        EXPECT_THROW_ERROR_CODE(
        {
            auto memory = Memory<DummyAllocator>(std::numeric_limits<size_t>::max() - (PADDING_FOR_SIMD - 1));
        }
        , Exception
        , ErrorCodes::LOGICAL_ERROR);
    }

    {
        EXPECT_THROW_ERROR_CODE(
        {
            auto memory = Memory<DummyAllocator>(0x8000000000000000ULL - (PADDING_FOR_SIMD - 1));
        }
        , Exception
        , ErrorCodes::LOGICAL_ERROR);
    }
#endif

    {
        auto memory = Memory<DummyAllocator>(0x8000000000000000ULL - PADDING_FOR_SIMD);
        ASSERT_TRUE(memory.m_data);
        ASSERT_EQ(memory.m_capacity, 0x8000000000000000ULL - 1);
        ASSERT_EQ(memory.m_size, 0x8000000000000000ULL - PADDING_FOR_SIMD);

        memory.resize(1);
        ASSERT_TRUE(memory.m_data);
        ASSERT_EQ(memory.m_capacity, 0x8000000000000000ULL - 1);
        ASSERT_EQ(memory.m_size, 1);
    }
}

TEST(MemoryResizeTest, AlignmentWithRealAllocator)
{
    {
        auto memory
            = Memory<>(0, 3); // not a power of 2 but less than MALLOC_MIN_ALIGNMENT so user-defined alignment is ignored at Allocator
        ASSERT_EQ(memory.m_data, nullptr);
        ASSERT_EQ(memory.m_capacity, 0);
        ASSERT_EQ(memory.m_size, 0);

        memory.resize(1);
        ASSERT_TRUE(memory.m_data);
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD);
        ASSERT_EQ(memory.m_size, 1);

        memory.resize(2);
        ASSERT_TRUE(memory.m_data);
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD + 1);
        ASSERT_EQ(memory.m_size, 2);

        memory.resize(3);
        ASSERT_TRUE(memory.m_data);
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD + 2);
        ASSERT_EQ(memory.m_size, 3);

        memory.resize(4);
        ASSERT_TRUE(memory.m_data);
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD + 3);
        ASSERT_EQ(memory.m_size, 4);

        memory.resize(0);
        ASSERT_TRUE(memory.m_data);
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD + 3);
        ASSERT_EQ(memory.m_size, 0);

        memory.resize(1);
        ASSERT_TRUE(memory.m_data);
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD + 3);
        ASSERT_EQ(memory.m_size, 1);
    }

#if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER) && !defined(MEMORY_SANITIZER) && !defined(UNDEFINED_BEHAVIOR_SANITIZER)
    {
        auto memory = Memory<>(0, 20); // not a power of 2, but more than MALLOC_MIN_ALIGNMENT
        ASSERT_EQ(memory.m_data, nullptr);
        ASSERT_EQ(memory.m_capacity, 0);
        ASSERT_EQ(memory.m_size, 0);

        EXPECT_THROW_ERROR_CODE(memory.resize(1), ErrnoException, ErrorCodes::CANNOT_ALLOCATE_MEMORY);
        ASSERT_EQ(memory.m_data, nullptr); // state is intact after exception
        ASSERT_EQ(memory.m_capacity, 0);
        ASSERT_EQ(memory.m_size, 0);
    }
#endif

    {
        auto memory = Memory<>(0, 32);
        ASSERT_EQ(memory.m_data, nullptr);
        ASSERT_EQ(memory.m_capacity, 0);
        ASSERT_EQ(memory.m_size, 0);

        memory.resize(1);
        ASSERT_TRUE(memory.m_data);
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD);
        ASSERT_EQ(memory.m_size, 1);

        memory.resize(32);
        ASSERT_TRUE(memory.m_data);
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD + 31);
        ASSERT_EQ(memory.m_size, 32);
    }
}

TEST(MemoryResizeTest, SomeAlignmentOverflowWhenAlignment)
{
    {
        auto memory = Memory<DummyAllocator>(0, 31);
        ASSERT_EQ(memory.m_data, nullptr);
        ASSERT_EQ(memory.m_capacity, 0);
        ASSERT_EQ(memory.m_size, 0);

        memory.resize(0);
        ASSERT_EQ(memory.m_data, nullptr);
        ASSERT_EQ(memory.m_capacity, 0);
        ASSERT_EQ(memory.m_size, 0);

        memory.resize(1);
        ASSERT_TRUE(memory.m_data);
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD);
        ASSERT_EQ(memory.m_size, 1);

        EXPECT_THROW_ERROR_CODE(memory.resize(std::numeric_limits<size_t>::max()), Exception, ErrorCodes::ARGUMENT_OUT_OF_BOUND);
        ASSERT_TRUE(memory.m_data); // state is intact after exception
        ASSERT_EQ(memory.m_capacity, PADDING_FOR_SIMD);
        ASSERT_EQ(memory.m_size, 1);
    }
}

namespace
{
/// Hands out the data `chunk_size` bytes per `next`, like a file read with a tiny `max_read_buffer_size`.
class ChunkedReadBuffer : public ReadBuffer
{
public:
    ChunkedReadBuffer(std::string_view data_, size_t chunk_size_) : ReadBuffer(nullptr, 0), data(data_), chunk_size(chunk_size_) {}

private:
    bool nextImpl() override
    {
        if (offset == data.size())
            return false;
        const size_t size = std::min(chunk_size, data.size() - offset);
        BufferBase::set(const_cast<char *>(data.data()) + offset, size, 0);
        offset += size;
        return true;
    }

    std::string_view data;
    size_t chunk_size;
    size_t offset = 0;
};
}

TEST(MemoryResizeTest, SaveUpToPositionSmallAppendsDoNotReallocateEachTime)
{
    const std::string data(1 << 20, 'a');
    ChunkedReadBuffer in(data, 16);
    Memory<> memory;

    size_t capacity_changes = 0;
    char * pos = in.position();
    while (true)
    {
        const size_t capacity_before = memory.m_capacity;
        const bool loaded = loadAtPosition(in, memory, pos);
        capacity_changes += memory.m_capacity != capacity_before;
        if (!loaded)
            break;
        pos = in.buffer().end();
    }

    ASSERT_EQ(std::string_view(memory.data(), memory.size()), data);
    /// 65536 appends of 16 bytes; growing the capacity by exactly the appended size reallocates on each of them.
    EXPECT_LT(capacity_changes, 200);
}
