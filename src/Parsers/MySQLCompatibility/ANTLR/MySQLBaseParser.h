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
