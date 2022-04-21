#include <gtest/gtest.h>
#include <fstream>

#include "../Printer.h"

#pragma GCC diagnostic ignored "-Wunused-function"

// MOO
#define GTEST_COUT std::cerr << "[          ] [ INFO ]"

// MOO: I want to do it from file
TEST(MySQLParserOverlayStress, Select1)
{
	std::ifstream input_file("statements_ascii.txt");
	if (!input_file.is_open())
	{
		GTEST_COUT << "NO FILE\n" << std::endl;
	}
    std::string input1 = "select 1";
	MySQLParserOverlay::ParseTreePrinter printer;
	std::string ast_str = printer.Print(input1);
	GTEST_COUT << ast_str << std::endl;
}
