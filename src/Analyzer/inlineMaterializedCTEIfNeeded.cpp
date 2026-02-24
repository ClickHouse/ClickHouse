#include <Analyzer/inlineMaterializedCTEIfNeeded.h>

#include <Analyzer/TableNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

namespace DB
{

namespace
{

class InlineMaterializedCTEsVisitor : public InDepthQueryTreeVisitorWithContext<InlineMaterializedCTEsVisitor>
{
    using Base = InDepthQueryTreeVisitorWithContext<InlineMaterializedCTEsVisitor>;
public:

    explicit InlineMaterializedCTEsVisitor(const ReusedMaterializedCTEs & reused_materialized_cte_, ContextPtr context_)
        : Base(std::move(context_))
        , reused_materialized_cte(reused_materialized_cte_)
    {
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (auto * table_node = node->as<TableNode>())
        {
            if (table_node->isMaterializedCTE())
            {
                const auto & materialized_cte = table_node->getMaterializedCTE();
                if (!reused_materialized_cte.contains(materialized_cte))
                {
                    replacement_map.emplace(table_node, table_node->getMaterializedCTESubquery());
                }
            }
        }
    }

    const IQueryTreeNode::ReplacementMap & getReplacementMap() const
    {
        return replacement_map;
    }

private:
    const ReusedMaterializedCTEs & reused_materialized_cte;
    IQueryTreeNode::ReplacementMap replacement_map;
};

}

void inlineMaterializedCTEIfNeeded(QueryTreeNodePtr & node, const ReusedMaterializedCTEs & reused_materialized_cte, ContextPtr context)
{
    InlineMaterializedCTEsVisitor visitor(reused_materialized_cte, std::move(context));
    visitor.visit(node);

    const auto & replacement_map = visitor.getReplacementMap();
    if (!replacement_map.empty())
        node = node->cloneAndReplace(replacement_map);
}

}
