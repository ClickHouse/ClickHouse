#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Query (EXISTS | SHOW CREATE) [TABLE|DICTIONARY] [db.]name [FORMAT format]
  */
class ParserTablePropertiesQuery : public IParserBase
{
protected:
    const char * getName() const override { return "EXISTS or SHOW CREATE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
