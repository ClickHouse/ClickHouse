#include <iostream>

#include <mysqlxx/mysqlxx.h>

#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ParserCreateQuery.h>
#include <DB/Parsers/formatAST.h>


int main(int argc, char ** argv)
{
	using namespace DB;
	
	ParserCreateQuery parser;
	ASTPtr ast;
	std::string input = "CREATE TABLE hits (URL String, UserAgentMinor2 FixedString(2), EventTime DateTime) ENGINE = Log";
	Expected expected = "";

	const char * begin = input.data();
	const char * end = begin + input.size();
	const char * pos = begin;

	if (parser.parse(pos, end, ast, max_parsed_pos, expected))
	{
		std::cout << "Success." << std::endl;
		formatAST(*ast, std::cout);
		std::cout << std::endl;

		std::cout << std::endl << ast->getTreeID() << std::endl;
	}
	else
	{
		std::cout << "Failed at position " << (pos - begin) << ": "
			<< mysqlxx::quote << input.substr(pos - begin, 10)
			<< ", expected " << expected << "." << std::endl;
	}

	return 0;
}
