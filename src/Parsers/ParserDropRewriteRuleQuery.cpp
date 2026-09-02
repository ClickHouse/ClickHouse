#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDropRewriteRuleQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTDropRewriteRuleQuery.h>

namespace DB
{

bool ParserDropRewriteRuleQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_rule(Keyword::RULE);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserIdentifier rule_name_p;

    ASTPtr rule_name;

    if (!s_drop.ignore(pos, expected))
        return false;

    if (!s_rule.ignore(pos, expected))
        return false;

    const bool if_exists = s_if_exists.ignore(pos, expected);

    if (!rule_name_p.parse(pos, rule_name, expected))
        return false;

    auto query = make_intrusive<ASTDropRewriteRuleQuery>();
    query->if_exists = if_exists;

    tryGetIdentifierNameInto(rule_name, query->rule_name);

    node = std::move(query);
    return true;
}

}
