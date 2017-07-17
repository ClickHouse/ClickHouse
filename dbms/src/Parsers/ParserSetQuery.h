#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Query like this:
  * SET [GLOBAL] name1 = value1, name2 = value2, ...
  */
class ParserSetQuery : public IParserBase
{
public:
    ParserSetQuery(bool parse_only_internals_ = false) : parse_only_internals(parse_only_internals_) {}

protected:
    const char * getName() const { return "SET query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected);

    /// Parse the list `name = value` pairs, without SET [GLOBAL].
    bool parse_only_internals;
};

}
