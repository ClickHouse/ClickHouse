#include <Planner/CollectUsedIndetifiers.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>

#include <Planner/PlannerContext.h>

namespace DB
{

namespace
{

class CollectUsedIdentifiersVisitor : public InDepthQueryTreeVisitor<CollectUsedIdentifiersVisitor, true>
{
public:

    explicit CollectUsedIdentifiersVisitor(const PlannerContextPtr & planner_context_, ColumnIdentifierSet & used_identifiers_)
        : used_identifiers(used_identifiers_)
        , planner_context(planner_context_)
    {}

    bool needChildVisit(VisitQueryTreeNodeType & parent [[maybe_unused]], VisitQueryTreeNodeType & child [[maybe_unused]])
    {
        const auto & node_type = child->getNodeType();
        return node_type !=  QueryTreeNodeType::TABLE
            && node_type !=  QueryTreeNodeType::TABLE_FUNCTION
            && node_type !=  QueryTreeNodeType::QUERY
            && node_type !=  QueryTreeNodeType::UNION
            && node_type !=  QueryTreeNodeType::JOIN
            && node_type !=  QueryTreeNodeType::ARRAY_JOIN;
    }

    void visitImpl(const QueryTreeNodePtr & node)
    {
        if (node->getNodeType() != QueryTreeNodeType::COLUMN)
            return;

        const auto * column_ident = planner_context->getColumnNodeIdentifierOrNull(node);
        if (!column_ident)
            return;

        used_identifiers.insert(*column_ident);
    }

    ColumnIdentifierSet & used_identifiers;
    const PlannerContextPtr & planner_context;
};

}

void collectUsedIdentifiers(const QueryTreeNodePtr & node, const PlannerContextPtr & planner_context, ColumnIdentifierSet & out)
{
    CollectUsedIdentifiersVisitor visitor(planner_context, out);
    visitor.visit(node);
}

ColumnIdentifierSet collectUsedIdentifiers(const QueryTreeNodePtr & node, const PlannerContextPtr & planner_context)
{
    ColumnIdentifierSet out;
    collectUsedIdentifiers(node, planner_context, out);
    return out;
}

}

