#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

class ParserDropNamedCollectionQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP NAMED COLLECTION query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};
}
