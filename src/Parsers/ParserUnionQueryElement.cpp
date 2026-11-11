#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserSelectQuery.h>
#include <Parsers/ParserUnionQueryElement.h>


namespace DB
{

bool ParserUnionQueryElement::parseImpl(Pos & pos, ASTPtr & node, Expected & expected)
{
    if (ParserSubquery().parse(pos, node, expected))
    {
        if (const auto * ast_subquery = node->as<ASTSubquery>())
            node = ast_subquery->children.at(0);

        /// If the subquery contained a single SELECT (no UNION/INTERSECT/EXCEPT inside),
        /// flatten the wrapping ASTSelectWithUnionQuery to just the ASTSelectQuery.
        /// This is safe because (SELECT ...) with no set operations is semantically identical to SELECT ...
        /// This is needed for AST formatting consistency: the EXCEPT formatter wraps children in parentheses,
        /// and without flattening, reparsing would introduce extra ASTSelectWithUnionQuery nesting.
        if (const auto * union_query = node->as<ASTSelectWithUnionQuery>())
            if (union_query->list_of_selects->children.size() == 1)
                node = union_query->list_of_selects->children[0];

        return true;
    }

    if (ParserSelectQuery().parse(pos, node, expected))
        return true;

    return false;
}

}
