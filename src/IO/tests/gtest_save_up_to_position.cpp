#include <gtest/gtest.h>

#include <IO/EmptyReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

#include <cstring>
#include <string_view>

using namespace DB;

TEST(SaveUpToPosition, EmptyBufferWithExistingMemory)
{
    EmptyReadBuffer in;
    Memory<> memory;
    memory.resize(4);
    std::memcpy(memory.data(), "abcd", 4);

    ASSERT_EQ(in.position(), nullptr);
    EXPECT_NO_THROW(saveUpToPosition(in, memory, in.position()));
    EXPECT_EQ(memory.size(), 4u);
    EXPECT_EQ(std::string_view(memory.data(), 4), "abcd");
}

TEST(SaveUpToPosition, AppendsFromMaterializedBuffer)
{
    const char data[] = "hello";
    ReadBufferFromMemory in(data, sizeof(data) - 1);
    Memory<> memory;

    char * current = in.position() + 3;
    saveUpToPosition(in, memory, current);

    EXPECT_EQ(memory.size(), 3u);
    EXPECT_EQ(std::string_view(memory.data(), 3), "hel");
    EXPECT_EQ(in.position(), current);
}
