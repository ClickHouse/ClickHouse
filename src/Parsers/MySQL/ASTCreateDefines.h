#pragma once

#include <Parsers/IAST.h>
#include <Parsers/IParserBase.h>
#include <Parsers/ASTExpressionList.h>

namespace DB
{

namespace MySQLParser
{


class ASTCreateDefines : public IAST
{
public:
    ASTExpressionList * columns = nullptr;
    ASTExpressionList * indices = nullptr;
    ASTExpressionList * constraints = nullptr;

    ASTPtr clone() const override;

    String getID(char) const override { return "Create definitions"; }
};

class ParserCreateDefines : public IParserBase
{
protected:
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    const char * getName() const override { return "table property list (column, index, constraint)"; }
};

}

}

