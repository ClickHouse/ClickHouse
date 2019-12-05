#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Query FREEZE TABLE [db.]name [PARTITION partition]
  */
class ParserFreezeQuery : public IParserBase
{
protected:
    const char * getName() const { return "FREEZE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);
};

}
