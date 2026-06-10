#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/** Parser for the `RESET SESSION` statement. */
class ParserResetSessionQuery : public IParserBase
{
protected:
    const char * getName() const override { return "ResetSessionQuery"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
