#include <Common/escapeForFileName.h>

#pragma GCC diagnostic ignored "-Wsign-compare"
#ifdef __clang__
    #pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"
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
