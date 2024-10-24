#include <Planner/CollectColumnIdentifiers.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/JoinNode.h>

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
            && child_node_type != QueryTreeNodeType::ARRAY_JOIN;
    }

    void visitImpl(const QueryTreeNodePtr & node)
    {
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

