#include <Planner/CollectColumnIdentifiers.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>

#include <Planner/PlannerContext.h>

namespace DB
{

namespace
{

class CollectTopLevelColumnIdentifiersVisitor : public InDepthQueryTreeVisitor<CollectTopLevelColumnIdentifiersVisitor, true>
{
public:

    explicit CollectTopLevelColumnIdentifiersVisitor(const PlannerContextPtr & planner_context_, ColumnIdentifierSet & used_identifiers_)
        : used_identifiers(used_identifiers_)
        , planner_context(planner_context_)
    {}

    static bool needChildVisit(VisitQueryTreeNodeType &, VisitQueryTreeNodeType & child)
    {
        const auto & node_type = child->getNodeType();
        return node_type != QueryTreeNodeType::TABLE
            && node_type != QueryTreeNodeType::TABLE_FUNCTION
            && node_type != QueryTreeNodeType::QUERY
            && node_type != QueryTreeNodeType::UNION
            && node_type != QueryTreeNodeType::JOIN
            && node_type != QueryTreeNodeType::ARRAY_JOIN;
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

