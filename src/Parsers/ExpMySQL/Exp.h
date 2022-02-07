#include "MySQLLexer.h"
#include "MySQLParser.h"

class Exp
{
public:
	std::string Test(const std::string & query)
	{
		antlr4::ANTLRInputStream input(query);

		MySQLLexer lexer(&input);

		// MOO: hack
		// lexer.fixInterpreter();
		antlr4::CommonTokenStream tokens(&lexer);

		tokens.fill();

		MySQLParser parser(&tokens);
		antlr4::tree::ParseTree* tree = parser.query();

		std::string result = tree->toStringTree(&parser);
		return result;

	}
};
