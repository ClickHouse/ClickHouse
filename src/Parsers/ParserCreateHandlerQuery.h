#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

class ParserCreateHandlerQuery : public IParserBase
{
protected:
    const char * getName() const override { return "CREATE HANDLER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
