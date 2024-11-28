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
    if (ASTPtr name, subquery;
        s_ident.parse(pos, name, expected))
    {
        auto with_element = std::make_shared<ASTWithElement>();
        if (open_bracket.ignore(pos, expected))
        {
            if (ASTPtr expression_list_for_aliases; exp_list_for_aliases.parse(pos, expression_list_for_aliases, expected))
                with_element->aliases = expression_list_for_aliases;

            if (!close_bracket.ignore(pos, expected))
                return false;
        }

        if (s_as.ignore(pos, expected) && s_subquery.parse(pos, subquery, expected))
        {
            tryGetIdentifierNameInto(name, with_element->name);
            with_element->subquery = subquery;
            with_element->children.push_back(with_element->subquery);
            node = with_element;
            return true;
        }
    }

    pos = old_pos;
    ParserExpressionWithOptionalAlias s_expr(false);
    if (!s_expr.parse(pos, node, expected))
        return false;

    return true;
}


}
