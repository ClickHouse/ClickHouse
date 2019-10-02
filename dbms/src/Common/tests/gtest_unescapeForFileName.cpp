#include <Common/escapeForFileName.h>

#if !__clang__
#pragma GCC diagnostic ignored "-Wsuggest-override"
#endif
#include <gtest/gtest.h>


using namespace DB;


TEST(Common, unescapeForFileName)
{
    EXPECT_EQ(unescapeForFileName(escapeForFileName("172.19.0.6")), "172.19.0.6");
    EXPECT_EQ(unescapeForFileName(escapeForFileName("abcd.")), "abcd.");
    EXPECT_EQ(unescapeForFileName(escapeForFileName("abcd")), "abcd");
    EXPECT_EQ(unescapeForFileName(escapeForFileName("..::")), "..::");
}
