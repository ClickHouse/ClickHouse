// do not include this header in code that interracts with clickhouse codebase!

#pragma once

#include "../ANTLR/MySQLLexer.h"
#include "../ANTLR/MySQLParser.h"

class MySQLAnalyzer
{
public:
	MySQLAnalyzer(const std::string & query, uint32_t settings = 0)
	{
		input = std::make_shared<antlr4::ANTLRInputStream>(query);

		lexer = std::make_shared<MySQLLexer>(input.get());
		tokens = std::make_shared<antlr4::CommonTokenStream>(lexer.get());
		tokens->fill();

		parser = std::make_shared<MySQLParser>(tokens.get());
		parser->setMode(settings);
	}

	const MySQLParser & getParser() const
	{
		return (*parser);
	}
	
	const antlr4::RuleContext * getParseTree() const
	{
		return dynamic_cast<antlr4::RuleContext *>(parser->query());
	}

private:
	std::shared_ptr<antlr4::ANTLRInputStream> input;
	std::shared_ptr<MySQLLexer> lexer;

	std::shared_ptr<antlr4::CommonTokenStream> tokens;
	std::shared_ptr<MySQLParser> parser;
};
