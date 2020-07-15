#pragma once

#include <Parsers/New/AST/INode.h>

#include <tree/TerminalNode.h>


namespace DB::AST
{

class Literal : public INode
{
    public:
        explicit Literal(const antlr4::tree::TerminalNode * literal);

    private:
        const antlr4::tree::TerminalNode * token;
};

class NumberLiteral : public Literal
{

};

class StringLiteral : public Literal
{

};

}
