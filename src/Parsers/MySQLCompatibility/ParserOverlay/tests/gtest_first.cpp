#include <gtest/gtest.h>

#pragma GCC diagnostic ignored "-Wunused-function"

// MOO
#define GTEST_COUT std::cerr << "[          ] [ INFO ]"

TEST(MySQLParserOverlayCorrectness, Select1)
{
    std::string input1 = "select 1";
	GTEST_COUT << input1 << std::endl;
}
