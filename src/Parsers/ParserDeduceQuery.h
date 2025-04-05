#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

class ParserDeduceQueryColumnsSpecification : public IParserBase
{
protected:
    const char * getName() const override { return "column specification for DEDUCE ... DEDUPLICATE BY"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

/** Query DEDUCE TABLE name BY col_to_deduce
  */
class ParserDeduceQuery : public IParserBase
{
protected:
    const char * getName() const override { return "DEDUCE query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
