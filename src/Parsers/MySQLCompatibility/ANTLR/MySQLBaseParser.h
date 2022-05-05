#pragma once

#include "antlr4-runtime.h"
#include "SqlMode.h"

class MySQLBaseParser : public antlr4::Parser
{
public:
    MySQLBaseParser(antlr4::TokenStream *input) : Parser(input) {} 
	virtual void setMode(uint32_t);
	virtual bool isSqlModeActive(SqlMode) const;
protected:
    int serverVersion = 50707;
private:
	uint32_t sqlMode;
};

class MySQLParserErrorListner : public antlr4::BaseErrorListener
{
public:
	virtual void syntaxError(antlr4::Recognizer *recognizer, antlr4::Token * offendingSymbol, size_t line, size_t charPositionInLine, const std::string &msg, std::exception_ptr e) override
	{
		std::stringstream ss;
		ss << msg << " ";
		ss << " at " << line << ":" << charPositionInLine;
		error = ss.str();
	}

	const std::string & getError() const
	{
		return error;
	}
private:
	std::string error;
};
