#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{


class ParserSelectQuery : public IParserBase
{
protected:
    const char * getName() const override { return "SELECT query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
