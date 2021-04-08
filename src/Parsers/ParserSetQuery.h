#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>


namespace DB
{

/** Query like this:
  * SET name1 = value1, name2 = value2, ...
  */
class ParserSetQuery : public IParserBase
{
public:
    explicit ParserSetQuery(bool parse_only_internals_ = false) : parse_only_internals(parse_only_internals_) {}
    static bool parseNameValuePair(SettingChange & change, IParser::Pos & pos, Expected & expected);
protected:
    const char * getName() const override { return "SET query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    /// Parse the list `name = value` pairs, without SET.
    bool parse_only_internals;
};

}
