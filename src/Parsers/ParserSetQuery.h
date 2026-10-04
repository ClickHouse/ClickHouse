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
    std::map<String, Documentation> getDocumentation() const override;

protected:
    const char * getName() const override { return "SET query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
    /// Parse the list `name = value` pairs, without SET.
    bool parse_only_internals;
    bool shorthand_syntax;
};

/// True if the input starts with an unambiguous SET statement prefix. Raw-text dialect parsers
/// (PromQL, polyglot) use this to decide between `ParserSetQuery` and their own grammar, because
/// the `SET <setting>` shorthand would otherwise swallow queries over a metric named `set`.
bool isCommittedToSetQuery(IParser::Pos pos);

/** Probe the text of a query for a SQL `SET` statement and return its AST, or `nullptr`
  * if it is not one. The `SET` must make up the whole text, save for a single trailing `;`. A non-SQL dialect uses it to let a session always run `SET dialect = ...`
  * to leave the dialect, and its experimental gate uses it to decide the same way the dialect's
  * own parser does - a first-token heuristic would let a statement such as `set.users.find({})`
  * through the gate as well.
  */
ASTPtr tryParseLeadingSetQuery(
    const char * begin, const char * end, size_t max_query_size, size_t max_parser_depth, size_t max_parser_backtracks);

}
