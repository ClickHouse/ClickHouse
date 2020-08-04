#pragma once

#include <Parsers/IAST.h>
#include <Parsers/IParserBase.h>

namespace DB
{

namespace MySQLParser
{

class ASTDeclareSubPartition : public IAST
{
public:
    ASTPtr options;
    String logical_name;

    ASTPtr clone() const override;

    String getID(char /*delimiter*/) const override { return "subpartition declaration"; }
};

class ParserDeclareSubPartition : public IParserBase
{
protected:
    const char * getName() const override { return "subpartition declaration"; }

    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}

}
