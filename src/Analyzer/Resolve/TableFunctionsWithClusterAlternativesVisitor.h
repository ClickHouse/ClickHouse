#pragma once

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/TableFunctionNode.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionFile.h>

namespace DB
{

class TableFunctionsWithClusterAlternativesVisitor : public InDepthQueryTreeVisitor<TableFunctionsWithClusterAlternativesVisitor, /*const_visitor=*/true>
{
public:
    void visitImpl(const QueryTreeNodePtr & node)
    {
        if (node->getNodeType() == QueryTreeNodeType::TABLE_FUNCTION)
            ++table_function_count;
        else if (node->getNodeType() == QueryTreeNodeType::TABLE)
            ++table_count;
        else if (node->getNodeType() == QueryTreeNodeType::QUERY && node->as<QueryNode>()->isSubquery())
            ++subquery_count;
        else if (node->getNodeType() == QueryTreeNodeType::JOIN)
            has_join = true;
    }

    bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr &) { return true; }

    bool shouldReplaceWithClusterAlternatives() const
    {
        return subquery_count <= 1 && !has_join && ((table_count + table_function_count) == 1 || (table_function_count == 0));
    }

private:
    size_t table_count = 0;
    size_t table_function_count = 0;
    // Number of subqueries that appear
    size_t subquery_count = 0;

    bool has_join = false;
};

}
