#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

/// Parses DROP HANDLER queries, producing an ASTDropHandlerQuery.
class ParserDropHandlerQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DROP HANDLER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
