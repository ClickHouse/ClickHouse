#pragma once

#include <Parsers/IParserBase.h>

namespace DB
{

class ParserAlterHandlerQuery : public IParserBase
{
protected:
    const char * getName() const override { return "ALTER HANDLER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
