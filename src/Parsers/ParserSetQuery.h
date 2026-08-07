#pragma once

#include <Parsers/IParserBase.h>


namespace DB
{

struct SettingChange;

/** Query like this:
  * SET name1 = value1, name2 = value2, ...
  * SET name1,... (shorthand for 'name1 = 1')
  */
class ParserSetQuery : public IParserBase
{
public:
    using Parameter = std::pair<std::string, std::string>;

    explicit ParserSetQuery(bool parse_only_internals_ = false, bool shorthand_syntax_ = true) : parse_only_internals(parse_only_internals_), shorthand_syntax(shorthand_syntax_) {}

    static bool parseNameValuePair(SettingChange & change, IParser::Pos & pos, Expected & expected);

    static bool parseNameValuePairWithParameterOrDefault(SettingChange & change,
                                                         String & default_settings,
                                                         Parameter & parameter,
                                                         IParser::Pos & pos,
                                                         Expected & expected,
                                                         bool enable_shorthand_syntax);

protected:
    const char * getName() const override { return "SET query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    /// Parse the list `name = value` pairs, without SET.
    bool parse_only_internals;
    bool shorthand_syntax;
};

}
