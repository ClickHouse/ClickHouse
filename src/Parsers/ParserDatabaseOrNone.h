#pragma once
#include <Parsers/IParserBase.h>

namespace DB
{

class ParserDatabaseOrNone : public IParserBase
{
protected:
    const char * getName() const override { return "DatabaseOrNone"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

};

}


