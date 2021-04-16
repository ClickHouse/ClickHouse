#pragma once
#include <Parsers/IParserBase.h>


namespace DB
{
class ParserIntersectOrExcept : public IParserBase
{
protected:
    const char * getName() const override { return "INTERSECT or EXCEPT"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
