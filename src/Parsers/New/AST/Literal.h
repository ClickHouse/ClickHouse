#pragma once

#include <Parsers/New/AST/INode.h>

#include <tree/TerminalNode.h>


namespace DB::AST
{

class Literal : public INode
{
    public:
        static PtrTo<Literal> createNull(const antlr4::tree::TerminalNode * literal);
        static PtrTo<Literal> createNumber(const antlr4::tree::TerminalNode * literal);
        static PtrTo<Literal> createString(const antlr4::tree::TerminalNode * literal);

        ASTPtr convertToOld() const override;

    private:
        enum class LiteralType
        {
            NULL_LITERAL,
            NUMBER,
            STRING,
        };

        LiteralType type;
        const antlr4::tree::TerminalNode * token;

        Literal(LiteralType type, const antlr4::tree::TerminalNode * literal);
};

}
