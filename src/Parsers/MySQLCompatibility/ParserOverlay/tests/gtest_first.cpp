#include <gtest/gtest.h>
#include "../AST.h"

#pragma GCC diagnostic ignored "-Wunused-function"

using MySQLParserOverlay::AST;
using MySQLParserOverlay::ASTPtr;

// TODO: check that AST is correct
TEST(MySQLParserOverlayCorrectness, Select1)
{
    std::string input1 = "select 1";
	std::string error = "";
	ASTPtr res = std::make_shared<AST>();
	AST::FromQuery(input1, res, error);
}
