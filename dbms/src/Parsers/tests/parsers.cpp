#include <iostream>

#include <strconvert/escape_manip.h>

#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ParserSelectQuery.h>
#include <DB/Parsers/formatAST.h>


int main(int argc, char ** argv)
{
	DB::ParserSelectQuery parser;
	DB::ASTPtr ast;
	std::string input = "SELECT f(1), '\\\\', [a, b, c], (a, b, c), 1 + 2 * -3, a = b OR c > d.1 + 2 * -g[0] AND NOT e < f * (x + y)";
	std::string expected;

	const char * begin = input.data();
	const char * end = begin + input.size();
	const char * pos = begin;

	if (parser.parse(pos, end, ast, expected))
	{
		std::cout << "Success." << std::endl;
		DB::formatAST(*ast, std::cout);
		std::cout << std::endl;
	}
	else
	{
		std::cout << "Failed at position " << (pos - begin) << ": "
			<< strconvert::quote_fast << input.substr(pos - begin, 10)
			<< ", expected " << expected << "." << std::endl;
	}

	return 0;
}
