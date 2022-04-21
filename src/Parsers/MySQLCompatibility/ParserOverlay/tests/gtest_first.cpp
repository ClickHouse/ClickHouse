#include <gtest/gtest.h>

#include "../Printer.h"

#pragma GCC diagnostic ignored "-Wunused-function"

// MOO
#define GTEST_COUT std::cerr << "[          ] [ INFO ]"

TEST(MySQLParserOverlayCorrectness, Select1)
{
    std::string input1 = "select 1";
	MySQLParserOverlay::ParseTreePrinter printer;
	std::string ast_str = printer.Print(input1);
	GTEST_COUT << ast_str << std::endl;
}
