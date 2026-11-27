#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateRewriteRuleQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTCreateRewriteRuleQuery.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

bool ParserCreateRewriteRuleQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_create(Keyword::CREATE);
    ParserKeyword s_rule(Keyword::RULE);
    ParserKeyword s_as(Keyword::AS);
    ParserKeyword s_rewrite(Keyword::REWRITE);
    ParserKeyword s_to(Keyword::TO);
    ParserKeyword s_reject(Keyword::REJECT);
    ParserKeyword s_with(Keyword::WITH);
    ParserIdentifier rule_name_p;
    ParserStringLiteral reject_message_p;
    ParserQuery source_query_p(end, allow_settings_after_format_in_insert);
    ParserQuery resulting_query_p(end, allow_settings_after_format_in_insert);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);

    ASTPtr rule_name;
    ASTPtr reject_message;
    ASTPtr source_query;
    ASTPtr resulting_query;
    bool is_reject = false;

    Pos begin_pos = pos;

    if (!s_create.ignore(pos, expected))
        return false;

    if (!s_rule.ignore(pos, expected))
        return false;

    if (!rule_name_p.parse(pos, rule_name, expected))
        return false;

    if (!s_as.ignore(pos, expected))
        return false;

    if (!s_lparen.ignore(pos, expected))
        return false;

    if (!source_query_p.parse(pos, source_query, expected))
        return false;

    if (!s_rparen.ignore(pos, expected))
        return false;

    if (s_rewrite.ignore(pos, expected) && s_to.ignore(pos, expected))
    {
        if (!s_lparen.ignore(pos, expected))
            return false;

        if (!resulting_query_p.parse(pos, resulting_query, expected))
            return false;

        if (!s_rparen.ignore(pos, expected))
            return false;

    } else if (s_reject.ignore(pos, expected) && s_with.ignore(pos, expected))
    {
        if (!reject_message_p.parse(pos, reject_message, expected))
            return false;

        is_reject = true;
    } else
    {
        return false;
    }

    Pos end_pos = pos;

    /// `end_pos` is the first token after the statement (e.g. the enclosing `)` when the rule is a template of another rule),
    /// so it must not be included.
    String whole_query = String(begin_pos->begin, begin_pos->end);
    for (++begin_pos; begin_pos < end_pos; ++begin_pos)
        whole_query += " " + String(begin_pos->begin, begin_pos->end);

    auto query = make_intrusive<ASTCreateRewriteRuleQuery>();

    tryGetIdentifierNameInto(rule_name, query->rule_name);
    if (auto* literal = reject_message ? reject_message->as<ASTLiteral>() : nullptr)
    {
        query->reject_message = literal->value.safeGet<String>();
    }
    query->is_reject = is_reject;
    query->source_query = std::move(source_query);
    query->resulting_query = std::move(resulting_query);
    query->whole_query = std::move(whole_query);

    node = std::move(query);
    return true;
}

std::map<String, Documentation> ParserCreateRewriteRuleQuery::getDocumentation() const
{
    std::map<String, Documentation> documentation;

    documentation["CREATE RULE"] =
    {
        .description = R"DOCS_MD(
Creates a query rewrite rule. A rule either rewrites a matching query into another query, or rejects it with a message.

The source template and the result template can be any query. A `{name:Type}` placeholder in the source template matches a part of the query and captures it, and the same placeholder in the result template substitutes the captured part. The supported placeholder types are `String`, `Int`, `Expression`, `ExpressionList` and `Subquery`. Matching is structural and ignores aliases.

A rule is applied only to the queries of a session where it is listed in the `query_rules` setting. See [Query rewrite rules](/operations/query-rules) for the details and the limitations.
)DOCS_MD",
        .syntax = R"(
CREATE RULE rule_name AS (source_query) REWRITE TO (result_query)
CREATE RULE rule_name AS (source_query) REJECT WITH 'message'
)",
        .examples = {
            {"Rewrite a query",
             R"(
CREATE RULE rule_1 AS
(
    SELECT date, sum(hits) FROM stats WHERE page = {name:String} GROUP BY date
)
REWRITE TO
(
    SELECT date, hits FROM totals WHERE page = {name:String}
);

SET query_rules = 'rule_1';
)", ""},
            {"Reject a query",
             R"(
CREATE RULE rule_2 AS (SELECT * FROM stats) REJECT WITH 'Full scans of stats are not allowed';
)", ""},
        },
        .parent = "CREATE",
        .related = {"ALTER RULE", "DROP RULE"},
    };

    return documentation;
}

}
