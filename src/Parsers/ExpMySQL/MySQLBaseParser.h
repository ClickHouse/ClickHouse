#pragma once

#include "antlr4-runtime.h"

class MySQLBaseParser : public antlr4::Parser
{
public:
    MySQLBaseParser(antlr4::TokenStream *input) : Parser(input) {}
    bool isSqlModeActive(int)
    {
        return false; // some logic here from ts
    }
public:
    int serverVersion = 0; // MOO: default value??
};
