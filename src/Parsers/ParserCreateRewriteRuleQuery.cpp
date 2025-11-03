#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserCreateRewriteRuleQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTCreateRewriteRuleQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Common/FieldVisitorToString.h>

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
    ParserLiteral reject_message_p;
    ParserQuery source_query_p(end, allow_settings_after_format_in_insert);
    ParserQuery resulting_query_p(end, allow_settings_after_format_in_insert);
    ParserToken s_lparen(TokenType::OpeningRoundBracket);
    ParserToken s_rparen(TokenType::ClosingRoundBracket);

    ASTPtr rule_name;
    ASTPtr reject_message;
    ASTPtr source_query;
    ASTPtr resulting_query;

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

    } else
    {
        return false;
    }

    Pos end_pos = pos;

    String whole_query = String(begin_pos->begin, begin_pos->end);
    while (begin_pos < end_pos)
    {
        ++begin_pos;
        whole_query += " " + String(begin_pos->begin, begin_pos->end);
    }

    auto query = std::make_shared<ASTCreateRewriteRuleQuery>();

    tryGetIdentifierNameInto(rule_name, query->rule_name);
    if (auto* literal = reject_message ? reject_message->as<ASTLiteral>() : nullptr;
        literal && literal->value.getType() == Field::Types::String)
    {
        query->reject_message = applyVisitor(FieldVisitorToString(), literal->value);
    }
    query->source_query = std::move(source_query);
    query->resulting_query = std::move(resulting_query);
    query->whole_query = std::move(whole_query);

    node = std::move(query);
    return true;
}

}
