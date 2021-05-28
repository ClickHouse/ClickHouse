#pragma once

#include <Parsers/IAST.h>
#include <Parsers/IParserBase.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace MySQLParser
{

class ASTDeclareConstraint : public IAST
{
public:
    bool enforced{true};
    String constraint_name;
    ASTPtr check_expression;

    ASTPtr clone() const override;

    String getID(char /*delimiter*/) const override { return "constraint declaration"; }

protected:
    void formatImpl(const FormatSettings & /*settings*/, FormatState & /*state*/, FormatStateStacked /*frame*/) const override
    {
        throw Exception("Method formatImpl is not supported by MySQLParser::ASTDeclareConstraint.", ErrorCodes::NOT_IMPLEMENTED);
    }
};

class ParserDeclareConstraint : public IParserBase
{
protected:
    const char * getName() const override { return "constraint declaration"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

}

