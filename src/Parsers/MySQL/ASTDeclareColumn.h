#pragma once

#include <Parsers/IAST.h>
#include <Parsers/IParserBase.h>

namespace DB
{

namespace MySQLParser
{

class ASTDeclareColumn : public IAST
{
public:
    String name;
    ASTPtr data_type;
    ASTPtr column_options;

    ASTPtr clone() const override;

    String getID(char /*delimiter*/) const override { return "Column definition"; }
};

class ParserDeclareColumn : public IParserBase
{
protected:
    const char * getName() const override { return "index declaration"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    bool parseColumnDeclareOptions(Pos & pos, ASTPtr & node, Expected & expected);
};


}

}
