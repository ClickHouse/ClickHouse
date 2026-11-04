#include <Analyzer/inlineMaterializedCTEIfNeeded.h>

#include <Analyzer/TableNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <Interpreters/MaterializedCTE.h>

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
                    /// If the materialized CTE is not reused, we can inline it and remove the temporary table from the context.
                    /// If table is not removed from Context, it would cause an exception in distributed queries,
                    /// because the temporary table would already exist in the Context
                    /// and there would be an attempt to read it to send the temporary table to remote servers.
                    getContext()->getQueryContext()->removeExternalTable(table_node->getTemporaryTableName());
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

void inlineMaterializedCTEIfNeeded(QueryTreeNodePtr & node, ReusedMaterializedCTEs & reused_materialized_cte, ContextPtr context)
{
    if (context->hasQueryContext())
    {
        /// Register Materialized CTEs as External tables
        auto query_context = context->getQueryContext();
        for (const auto & materialized_cte : reused_materialized_cte)
            query_context->addExternalTable(materialized_cte->temporary_table_name, materialized_cte->extractTableHolder());
    }
    else
    {
        reused_materialized_cte.clear();
    }

    InlineMaterializedCTEsVisitor visitor(reused_materialized_cte, std::move(context));
    visitor.visit(node);

    const auto & replacement_map = visitor.getReplacementMap();
    if (!replacement_map.empty())
        node = node->cloneAndReplace(replacement_map);
}

}
