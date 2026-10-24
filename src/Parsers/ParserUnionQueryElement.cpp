#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTSelectQuery.h>
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
        /// Not if the SELECT has its own `SETTINGS` clause: the parentheses are then the boundary of that clause.
        /// The trailing clause of the last operand of a set operation applies to the whole set operation
        /// (see `QueryTreeBuilder::buildSelectWithUnionExpression`), so without the wrapper the clause of
        /// `SELECT a UNION ALL (SELECT b SETTINGS x = 1)` would also apply to `SELECT a`. The formatter does not
        /// wrap a bare SELECT with a `SETTINGS` clause in parentheses, so this keeps the formatting roundtrip.
        if (const auto * union_query = node->as<ASTSelectWithUnionQuery>())
        {
            if (union_query->list_of_selects->children.size() == 1)
            {
                const auto * select = union_query->list_of_selects->children[0]->as<ASTSelectQuery>();
                if (!select || !select->settings())
                    node = union_query->list_of_selects->children[0];
            }
        }

        return true;
    }

    if (ParserSelectQuery().parse(pos, node, expected))
        return true;

    return false;
}

}
