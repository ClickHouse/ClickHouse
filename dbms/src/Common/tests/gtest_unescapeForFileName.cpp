#include <Common/escapeForFileName.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#include <gtest/gtest.h>
#pragma GCC diagnostic pop

using namespace DB;


TEST(Common, unescapeForFileName)
{
    EXPECT_EQ(unescapeForFileName(escapeForFileName("172.19.0.6")), "172.19.0.6");
    EXPECT_EQ(unescapeForFileName(escapeForFileName("abcd.")), "abcd.");
    EXPECT_EQ(unescapeForFileName(escapeForFileName("abcd")), "abcd");
    EXPECT_EQ(unescapeForFileName(escapeForFileName("..::")), "..::");
}
