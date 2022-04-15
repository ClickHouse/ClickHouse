#include "Conversion.h"

#include "ANTLR/MySQLLexer.h"
#include "ANTLR/MySQLParser.h"

std::string Exp::Test(const std::string & query)
{
	antlr4::ANTLRInputStream input(query);

	MySQLLexer lexer(&input);

	antlr4::CommonTokenStream tokens(&lexer);

	tokens.fill();

	MySQLParser parser(&tokens);
	antlr4::tree::ParseTree* tree = parser.query();

	std::string result = tree->toStringTree(&parser);
	return result;
}
