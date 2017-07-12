#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

class ParserEnumElement : public IParserBase
{
protected:
    const char * getName() const override { return "Enum element"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


}
