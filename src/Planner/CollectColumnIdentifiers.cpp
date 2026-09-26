#include <Planner/CollectColumnIdentifiers.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>

#include <Planner/PlannerContext.h>


namespace DB
{

namespace
{

class CollectTopLevelColumnIdentifiersVisitor : public ConstInDepthQueryTreeVisitor<CollectTopLevelColumnIdentifiersVisitor>
{
public:

    explicit CollectTopLevelColumnIdentifiersVisitor(const PlannerContextPtr & planner_context_, ColumnIdentifierSet & used_identifiers_)
        : used_identifiers(used_identifiers_)
        , planner_context(planner_context_)
    {}

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child)
    {
        auto child_node_type = child->getNodeType();
        return child_node_type != QueryTreeNodeType::TABLE
            && child_node_type != QueryTreeNodeType::TABLE_FUNCTION
            && child_node_type != QueryTreeNodeType::QUERY
            && child_node_type != QueryTreeNodeType::UNION
            && child_node_type != QueryTreeNodeType::JOIN
            && child_node_type != QueryTreeNodeType::CROSS_JOIN
            && child_node_type != QueryTreeNodeType::ARRAY_JOIN;
    }

    void visitImpl(const QueryTreeNodePtr & node)
    {
        if (node->getNodeType() == QueryTreeNodeType::FUNCTION)
        {
            auto * function_node = node->as<FunctionNode>();
            for (const auto & argument : function_node->getArguments().getNodes())
            {
                if (!isCorrelatedQueryOrUnionNode(argument))
                    continue;

                auto * query_node = argument->as<QueryNode>();
                auto * union_node = argument->as<UnionNode>();

                const auto & correlated_columns = query_node != nullptr ? query_node->getCorrelatedColumns() : union_node->getCorrelatedColumns();
                for (const auto & column : correlated_columns)
                {
                    const auto * column_identifier = planner_context->getColumnNodeIdentifierOrNull(column);
                    if (!column_identifier)
                        return;

                    used_identifiers.insert(*column_identifier);
                }
            }
            return;
        }

        if (node->getNodeType() != QueryTreeNodeType::COLUMN)
            return;

        const auto * column_identifier = planner_context->getColumnNodeIdentifierOrNull(node);
        if (!column_identifier)
            return;

        used_identifiers.insert(*column_identifier);
    }

    ColumnIdentifierSet & used_identifiers;
    const PlannerContextPtr & planner_context;
};

}

void collectTopLevelColumnIdentifiers(const QueryTreeNodePtr & node, const PlannerContextPtr & planner_context, ColumnIdentifierSet & out)
{
    CollectTopLevelColumnIdentifiersVisitor visitor(planner_context, out);
    visitor.visit(node);
}

ColumnIdentifierSet collectTopLevelColumnIdentifiers(const QueryTreeNodePtr & node, const PlannerContextPtr & planner_context)
{
    ColumnIdentifierSet out;
    collectTopLevelColumnIdentifiers(node, planner_context, out);
    return out;
}

}
