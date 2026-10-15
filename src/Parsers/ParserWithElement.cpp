#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/ParserWithElement.h>


namespace DB
{
bool ParserWithElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier s_ident;
    ParserKeyword s_as(Keyword::AS);
    ParserKeyword s_materialized(Keyword::MATERIALIZED);
    ParserSubquery s_subquery;
    ParserAliasesExpressionList exp_list_for_aliases;
    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);

    auto old_pos = pos;

    // Trying to parse structure: identifier [(alias1, alias2, ...)] AS (subquery)
    if (ASTPtr cte_name, aliases;
        s_ident.parse(pos, cte_name, expected) &&
        (
            [&]() -> bool {
                if (open_bracket.ignore(pos, expected))
                {
                    if (ASTPtr expression_list_for_aliases; exp_list_for_aliases.parse(pos, expression_list_for_aliases, expected))
                    {
                        aliases = expression_list_for_aliases;
                        return close_bracket.ignore(pos, expected);
                    }
                    else
                    {
                        return false;
                    }
                }
                return true;
            }()
        ) &&
        s_as.ignore(pos, expected))
    {
        bool has_materialized_keyword = s_materialized.ignore(pos, expected);

        if (ASTPtr subquery; s_subquery.parse(pos, subquery, expected))
        {
            auto with_element = make_intrusive<ASTWithElement>();

            tryGetIdentifierNameInto(cte_name, with_element->name);
            with_element->aliases = std::move(aliases);
            with_element->is_materialized = has_materialized_keyword;
            with_element->subquery = std::move(subquery);
            with_element->children.push_back(with_element->subquery);

            node = with_element;
            return true;
        }
    }

    /// CTE parsing failed, rollback and try to parse ordinary expression
    pos = old_pos;
    ParserExpressionWithOptionalAlias s_expr(false);
    if (!s_expr.parse(pos, node, expected))
        return false;

    return true;
}


}
