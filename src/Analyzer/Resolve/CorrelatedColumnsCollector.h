#pragma once

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/Utils.h>

#include <Analyzer/Resolve/IdentifierResolveScope.h>

#include <unordered_map>
#include <vector>

namespace DB
{

class IQueryTreeNode;
using QueryTreeNodePtr = std::shared_ptr<IQueryTreeNode>;
using QueryTreeNodes = std::vector<QueryTreeNodePtr>;

struct CorrelatedColumnsCollector
{
    explicit CorrelatedColumnsCollector(
        const QueryTreeNodePtr & expression,
        const IdentifierResolveScope * current_scope,
        const std::unordered_map<QueryTreeNodePtr, IdentifierResolveScope> & map
    )
        : node_to_scope_map(map)
        , correlated_columns()
    {
        visitExpression(expression, current_scope);
    }

    bool has() const { return !correlated_columns.empty(); }

    const QueryTreeNodes & get() const { return correlated_columns; }

private:
    void visitExpression(const QueryTreeNodePtr & expression, const IdentifierResolveScope * current_scope)
    {
        QueryTreeNodes nodes_to_process = { expression };
        while (!nodes_to_process.empty())
        {
            auto current = nodes_to_process.back();
            nodes_to_process.pop_back();

            auto current_node_type = current->getNodeType();

            /// Change scope if expression is a QueryNode.
            /// Columns in these subqueries can appear from table expressions
            /// that are registered in the child scope.
            if (current_node_type == QueryTreeNodeType::QUERY)
            {
                auto it = node_to_scope_map.find(current);
                if (it != node_to_scope_map.end() && current != current_scope->scope_node)
                {
                    visitExpression(current, &it->second);
                    continue;
                }
            }

            if (current_node_type == QueryTreeNodeType::COLUMN)
            {
                if (checkCorrelatedColumn(current_scope, current))
                    correlated_columns.push_back(current);
            }

            for (const auto & child : current->getChildren())
            {
                if (child)
                    nodes_to_process.push_back(child);
            }
        }
    }

    const std::unordered_map<QueryTreeNodePtr, IdentifierResolveScope> & node_to_scope_map;
    QueryTreeNodes correlated_columns;
};

}
