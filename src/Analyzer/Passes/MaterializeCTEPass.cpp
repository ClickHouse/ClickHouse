#include <Analyzer/Passes/MaterializeCTEPass.h>
#include "Analyzer/CTENode.h"
#include "Analyzer/InDepthQueryTreeVisitor.h"
#include "Analyzer/QueryTreePassManager.h"
#include "Interpreters/MaterializedTableFromCTE.h"
#include "Interpreters/SelectQueryOptions.h"
#include "Planner/Planner.h"


namespace DB
{
class MaterializeCTEVisitor : public InDepthQueryTreeVisitor<MaterializeCTEVisitor>
{
public:
    bool shouldTraverseTopToBottom() const
    {
        return false;
    }

    static void visitImpl(QueryTreeNodePtr & node)
    {
        auto * cte_node = node->as<CTENode>();
        if (!cte_node || !cte_node->isMaterializedCTE() || cte_node->getCTEReferenceCounter() <= 1)
            return;
        auto query_context = cte_node->getMutableContext()->getQueryContext();
        auto columns = cte_node->getProjectionColumns();
        node->getChildren().clear();

        TemporaryTableHolder external_storage_holder(
            query_context,
            ColumnsDescription::fromNamesAndTypes(std::move(columns)),
            ConstraintsDescription{},
            /*query*/ nullptr,
            /*delay_read*/ true,
            cte_node->getMaterializedCTEEngine());

        auto source_query_node = cte_node->getQueryNode();
        // QueryTreePassManager pass_manager(query_context);
        // addQueryTreePasses(pass_manager);
        // pass_manager.run(source_query_node);
        Planner planner(source_query_node, SelectQueryOptions().subquery().setInternal());

        FutureTableFromCTEPtr future_table = std::make_shared<FutureTableFromCTE>();
        future_table->name = cte_node->getUniqueCTEName();
        future_table->external_table = external_storage_holder.getTable();
        future_table->source = std::make_unique<QueryPlan>(std::move(planner.getQueryPlan()));
        future_table->query_tree = std::move(source_query_node);

        query_context->addExternalTableFromCTE(future_table, std::move(external_storage_holder));
        cte_node->setFutureTable(std::move(future_table));
    }
};

void MaterializeCTEPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr /*context*/)
{
    MaterializeCTEVisitor().visit(query_tree_node);
}

}
