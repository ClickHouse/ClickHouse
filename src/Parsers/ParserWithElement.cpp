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
    ParserKeyword s_using(Keyword::USING);
    ParserKeyword s_key(Keyword::KEY);
    ParserSubquery s_subquery;
    ParserAliasesExpressionList exp_list_for_aliases;
    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ParserToken close_bracket(TokenType::ClosingRoundBracket);

    auto old_pos = pos;

    // `select` (case-insensitive) as a bareword is disallowed as a CTE name
    // because it creates ambiguity with the SELECT keyword that follows the WITH clause.
    // This check must happen before the CTE/expression parsing below, because if CTE parsing
    // rejects `select` and falls through to the expression path, `WITH select AS foo` would be
    // silently reinterpreted as an expression alias instead of producing an error.
    if (ASTPtr ident; s_ident.parse(pos, ident, expected))
    {
        String name;
        if (tryGetIdentifierNameInto(ident, name)
            && old_pos->type == TokenType::BareWord
            && strcasecmp(name.c_str(), "select") == 0)
            return false;
    }
    pos = old_pos;

    // Trying to parse structure: identifier [(alias1, alias2, ...)] [USING KEY (key1, key2, ...)] AS (subquery)
    if (ASTPtr cte_name, aliases, key_columns;
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
        (
            [&]() -> bool {
                if (s_using.ignore(pos, expected))
                {
                    if (!s_key.ignore(pos, expected))
                        return false;
                    if (!open_bracket.ignore(pos, expected))
                        return false;
                    ParserList key_columns_list_parser(std::make_unique<ParserIdentifier>(), std::make_unique<ParserToken>(TokenType::Comma), false);
                    if (ASTPtr key_columns_list; key_columns_list_parser.parse(pos, key_columns_list, expected))
                    {
                        key_columns = key_columns_list;
                        return close_bracket.ignore(pos, expected);
                    }
                    return false;
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
            with_element->key_columns = std::move(key_columns);
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
