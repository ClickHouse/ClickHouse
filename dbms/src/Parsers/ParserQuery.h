#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{


class ParserQuery : public IParserBase
{
protected:
    const char * getName() const { return "Query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};

}
