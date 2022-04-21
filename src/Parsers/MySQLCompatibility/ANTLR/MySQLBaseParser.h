#pragma once

#include "antlr4-runtime.h"
#include "SqlMode.h"

class MySQLBaseParser : public antlr4::Parser
{
public:
    MySQLBaseParser(antlr4::TokenStream *input) : Parser(input) {}
    bool isSqlModeActive(int);
protected:
    int serverVersion = 50707;
private:
	SqlMode sqlMode;
};
