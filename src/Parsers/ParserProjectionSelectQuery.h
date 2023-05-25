#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{


class ParserProjectionSelectQuery : public IParserBase
{
protected:
    const char * getName() const override { return "PROJECTION SELECT query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
