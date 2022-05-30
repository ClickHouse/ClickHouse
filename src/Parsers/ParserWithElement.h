#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** WITH (scalar query) AS identifier
  *  or WITH identifier [(col_name [, col_name] ...)] AS (subquery)
  */
class ParserWithElement : public IParserBase
{
protected:
    const char * getName() const override { return "WITH element"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};

}
