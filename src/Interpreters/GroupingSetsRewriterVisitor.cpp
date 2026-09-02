#include <Interpreters/GroupingSetsRewriterVisitor.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTExpressionList.h>


namespace DB
{

void GroupingSetsRewriterData::visit(ASTSelectQuery & select_query, ASTPtr &)
{
    const ASTPtr group_by = select_query.groupBy();
    if (!group_by || !select_query.group_by_with_grouping_sets)
        return;

    if (group_by->children.size() != 1)
        return;

    select_query.setExpression(ASTSelectQuery::Expression::GROUP_BY, std::move(group_by->children.front()));
    select_query.group_by_with_grouping_sets = false;
}

}
