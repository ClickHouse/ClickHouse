#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserWithElement.h>


namespace DB
{
bool ParserWithElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier s_ident;
    ParserKeyword s_as(Keyword::AS);
    ParserSubquery s_subquery;
    ParserAliasesExpressionList exp_list_for_aliases;
    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);

    auto old_pos = pos;
    auto with_element = std::make_shared<ASTWithElement>();

    // Trying to parse structure: identifier [(alias1, alias2, ...)] AS (subquery)
    if (ASTPtr name_or_expr;
        (s_ident.parse(pos, name_or_expr, expected) || ParserExpressionWithOptionalAlias(false).parse(pos, name_or_expr, expected)) &&
        (
            [&]() -> bool {
                auto saved_pos = pos;
                if (open_bracket.ignore(pos, expected))
                {
                    if (ASTPtr expression_list_for_aliases; exp_list_for_aliases.parse(pos, expression_list_for_aliases, expected))
                    {
                        with_element->aliases = expression_list_for_aliases;
                        if (!close_bracket.ignore(pos, expected))
                            return false;
                        return true;
                    }
                    else
                    {
                        pos = saved_pos;
                        return false;
                    }
                }
                return true;
            }()
        ) &&
        s_as.ignore(pos, expected) &&
        s_subquery.parse(pos, with_element->subquery, expected))
    {
        if (name_or_expr)
            tryGetIdentifierNameInto(name_or_expr, with_element->name);

        with_element->children.push_back(with_element->subquery);

        node = with_element;
    }
    else
    {
        pos = old_pos;
        ParserExpressionWithOptionalAlias s_expr(false);
        if (!s_expr.parse(pos, node, expected))
            return false;
    }
    return true;
}


}
