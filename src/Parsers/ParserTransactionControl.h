#pragma once
#include <Parsers/IParserBase.h>

namespace DB
{

class ParserTransactionControl : public IParserBase
{
public:
    const char * getName() const override { return "TCL query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
