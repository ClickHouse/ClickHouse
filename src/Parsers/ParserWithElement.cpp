#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
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
    ParserSubquery s_subquery;

    auto old_pos = pos;
    if (ASTPtr name, subquery;
        s_ident.parse(pos, name, expected) && s_as.ignore(pos, expected) && s_subquery.parse(pos, subquery, expected))
    {
        auto with_element = std::make_shared<ASTWithElement>();
        tryGetIdentifierNameInto(name, with_element->name);
        with_element->subquery = subquery;
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
