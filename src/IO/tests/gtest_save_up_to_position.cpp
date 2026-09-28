#include <gtest/gtest.h>

#include <IO/EmptyReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

#include <cstring>
#include <string_view>

using namespace DB;

/// Regression for https://github.com/ClickHouse/ClickHouse/issues/122626
///
/// `saveUpToPosition` used to early-return only when `new_bytes == 0`. When
/// `memory` already holds data and the working buffer is empty
/// (`additional_bytes == 0`, `in.position() == nullptr`), that guard did not
/// fire and the function called `memcpy(..., nullptr, 0)` — undefined behavior
/// that UBSan reports from `ParallelParsingInputFormat`.
TEST(SaveUpToPosition, EmptyBufferWithExistingMemoryDoesNotMemcpyNull)
{
    /// EmptyReadBuffer starts with pos == nullptr (same class of buffers that
    /// trigger the bug in the parallel-parse path before the first next()).
    EmptyReadBuffer in;
    Memory<> memory;
    memory.resize(4);
    std::memcpy(memory.data(), "abcd", 4);

    char * current = in.position();
    ASSERT_EQ(current, nullptr);

    EXPECT_NO_THROW(saveUpToPosition(in, memory, current));
    EXPECT_EQ(memory.size(), 4u);
    EXPECT_EQ(std::string_view(memory.data(), 4), "abcd");
}

TEST(SaveUpToPosition, AppendsBytesFromMaterializedBuffer)
{
    const char data[] = "hello";
    ReadBufferFromMemory in(data, sizeof(data) - 1);
    Memory<> memory;

    char * current = in.position() + 3; /// "hel"
    saveUpToPosition(in, memory, current);

    EXPECT_EQ(memory.size(), 3u);
    EXPECT_EQ(std::string_view(memory.data(), 3), "hel");
    EXPECT_EQ(in.position(), current);
}
