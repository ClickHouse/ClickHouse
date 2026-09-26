#pragma once

#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/Utils.h>
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
        else if (const auto * function_node = node->as<FunctionNode>(); function_node && isNameOfInFunction(function_node->getFunctionName()))
        {
            /// The tree is not resolved yet, so `x IN t` and `x IN cte` are only identifiers here. Count every identifier:
            /// it can also be an alias of a constant set, but then the read merely stays local.
            const auto & arguments = function_node->getArguments().getNodes();
            if (arguments.size() == 2)
            {
                const auto set_type = arguments[1]->getNodeType();
                if (set_type == QueryTreeNodeType::QUERY || set_type == QueryTreeNodeType::UNION
                    || set_type == QueryTreeNodeType::IDENTIFIER || set_type == QueryTreeNodeType::TABLE_FUNCTION)
                    has_in_with_subquery = true;
            }
        }
    }

    bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr &) { return true; }

    /// Whether the query may have `IN` with a subquery (`x IN (SELECT ...)`, `x IN t`, `x IN cte`), which a cluster engine would
    /// execute on every replica.
    bool hasInWithSubquery() const { return has_in_with_subquery; }

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
    bool has_in_with_subquery = false;
};

}
