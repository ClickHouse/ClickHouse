#include <Access/GSSAcceptor.h>

#if USE_KRB5

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

using namespace DB;

namespace
{

gss_buffer_desc makeBuffer(std::vector<char> & storage)
{
    gss_buffer_desc buf;
    buf.length = storage.size();
    buf.value = storage.empty() ? nullptr : storage.data();
    return buf;
}

}

/// Regression test for https://github.com/ClickHouse/ClickHouse/issues/97414
/// Opaque GSS tokens (e.g. the AP-REP from gss_accept_sec_context) may legitimately end
/// in 0x00 bytes. These must be preserved verbatim, not stripped.
TEST(GSSBufferToString, PreservesTrailingNulls)
{
    std::vector<char> storage = {'a', 'b', 'c', '\0'};
    auto buf = makeBuffer(storage);
    const auto str = bufferToString(buf);

    ASSERT_EQ(str.size(), 4u);
    EXPECT_EQ(std::memcmp(str.data(), "abc\0", 4), 0);
}

TEST(GSSBufferToString, PreservesMultipleTrailingNulls)
{
    std::vector<char> storage = {'x', '\0', '\0', '\0'};
    auto buf = makeBuffer(storage);
    const auto str = bufferToString(buf);

    ASSERT_EQ(str.size(), 4u);
    EXPECT_EQ(std::memcmp(str.data(), "x\0\0\0", 4), 0);
}

TEST(GSSBufferToString, PreservesEmbeddedNulls)
{
    std::vector<char> storage = {'a', '\0', 'b'};
    auto buf = makeBuffer(storage);
    const auto str = bufferToString(buf);

    ASSERT_EQ(str.size(), 3u);
    EXPECT_EQ(std::memcmp(str.data(), "a\0b", 3), 0);
}

TEST(GSSBufferToString, CopiesPlainStringVerbatim)
{
    std::vector<char> storage = {'h', 'e', 'l', 'l', 'o'};
    auto buf = makeBuffer(storage);
    const auto str = bufferToString(buf);

    EXPECT_EQ(str, "hello");
}

TEST(GSSBufferToString, HandlesEmptyBuffer)
{
    std::vector<char> storage;
    auto buf = makeBuffer(storage);
    const auto str = bufferToString(buf);

    EXPECT_TRUE(str.empty());
}

TEST(GSSBufferToString, HandlesNullValueWithZeroLength)
{
    gss_buffer_desc buf;
    buf.length = 0;
    buf.value = nullptr;
    const auto str = bufferToString(buf);

    EXPECT_TRUE(str.empty());
}

#endif
