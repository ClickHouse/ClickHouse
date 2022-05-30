#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/ParserWithElement.h>
#include <Parsers/ParserWithOptionalColumnNames.h>


namespace DB
{
bool ParserWithElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserIdentifier s_ident;
    ParserKeyword s_as("AS");
    ParserSubquery s_subquery;
    ParserWithOptionalColumnNames s_column_names;

    auto old_pos = pos;
    if (ASTPtr name, subquery, column_names;
        s_ident.parse(pos, name, expected) && s_column_names.parse(pos, column_names, expected) 
        && s_as.ignore(pos, expected) && s_subquery.parse(pos, subquery, expected))
    {
        auto with_element = std::make_shared<ASTWithElement>();
        tryGetIdentifierNameInto(name, with_element->name);
        
        with_element->subquery = subquery;
        with_element->children.push_back(with_element->subquery);
        
        if (column_names != nullptr)
        {
            with_element->column_names = column_names;
            with_element->children.push_back(with_element->column_names);
        }
        
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
