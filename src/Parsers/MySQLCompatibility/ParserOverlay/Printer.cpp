#include "Internals.h"

#include "Printer.h"

namespace MySQLParserOverlay
{
std::string ParseTreePrinter::Print(const std::string & query) const
{
	antlr4::ANTLRInputStream input(query);

	MySQLLexer lexer(&input);

	antlr4::CommonTokenStream tokens(&lexer);

	tokens.fill();

	MySQLParser parser(&tokens);
	MySQLParser::QueryContext * tree = parser.query();

	std::string result = tree->toStringTree(&parser);
	return result;
}
}
