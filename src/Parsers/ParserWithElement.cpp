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
    ParserKeyword s_as("AS");
    ParserKeyword s_materialized("MATERIALIZED");
    ParserSubquery s_subquery;
    ParserKeyword s_engine("ENGINE");
    ParserToken s_eq(TokenType::Equals);
    ParserIdentifierWithOptionalParameters ident_with_optional_params_p;

    auto old_pos = pos;
    if (ASTPtr name, subquery;
        s_ident.parse(pos, name, expected) && s_as.ignore(pos, expected))
    {
        auto with_element = std::make_shared<ASTWithElement>();
        if (s_materialized.ignore(pos, expected))
            with_element->has_materialized_keyword = true;
        tryGetIdentifierNameInto(name, with_element->name);
        if (s_subquery.parse(pos, subquery, expected))
        {
            with_element->subquery = subquery;
            with_element->children.push_back(with_element->subquery);
            if (s_engine.ignore(pos, expected))
            {
                ASTPtr engine;
                if (s_eq.ignore(pos, expected) && ident_with_optional_params_p.parse(pos, engine, expected))
                {
                    with_element->engine = std::move(engine);
                    with_element->children.push_back(with_element->engine);
                }
                else
                    return false;
            }
            node = with_element;
        }
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
