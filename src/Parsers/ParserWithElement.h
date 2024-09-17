#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{
/** WITH (scalar query) AS identifier
  *  or WITH identifier AS (subquery)
  */
class ParserWithElement : public IParserBase
{
public:
    ParserWithElement(bool with_recursive_ = false) : with_recursive(with_recursive_) {}
protected:
    const char * getName() const override { return "WITH element"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

    /// for `WITH RECURSIVE` CTE, we don't allow expression with alias.
    bool with_recursive;
};

}
