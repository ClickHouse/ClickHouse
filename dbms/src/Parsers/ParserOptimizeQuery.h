#pragma once

#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IParserBase.h>


namespace DB
{
/** Query OPTIMIZE TABLE [db.]name [PARTITION partition] [FINAL] [DEDUPLICATE]
  */
class ParserOptimizeQuery : public IParserBase
{
protected:
    const char * getName() const override { return "OPTIMIZE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
