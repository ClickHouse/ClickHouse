#include <Parsers/ASTWindowDefinition.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SettingQuotaAndLimitsStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ArrayJoinAction.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/TableJoin.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Columns/IColumn.h>
#include <base/logger_useful.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::QueryPlanOptimizations
{

size_t tryReuseStorageOrdering(QueryPlan::Node * parent_node, QueryPlan::Nodes & /*nodes*/)
{
    auto log = &Poco::Logger::get("tryReuseStorageOrdering()");

    auto * window_node = parent_node;
    auto * window = typeid_cast<WindowStep *>(window_node->step.get());
    if (!window)
        return 0;
    if (window_node->children.size() != 1)
        return 0;

    auto * sorting_node = window_node->children.front();
    auto * sorting = typeid_cast<SortingStep *>(sorting_node->step.get());
    if (!sorting)
        return 0;
    if (sorting_node->children.size() != 1)
        return 0;

    auto * possible_read_from_merge_tree_node = sorting_node->children.front();

    auto * expression = typeid_cast<ExpressionStep *>(possible_read_from_merge_tree_node->step.get());
    if (expression)
    {
        if (possible_read_from_merge_tree_node->children.size() != 1)
            return 0;

        possible_read_from_merge_tree_node = possible_read_from_merge_tree_node->children.front();
    }

    auto * quota_and_limits = typeid_cast<SettingQuotaAndLimitsStep *>(possible_read_from_merge_tree_node->step.get());
    if (quota_and_limits)
    {
        if (possible_read_from_merge_tree_node->children.size() != 1)
            return 0;

        possible_read_from_merge_tree_node = possible_read_from_merge_tree_node->children.front();
    }

    auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(possible_read_from_merge_tree_node->step.get());
    if (!read_from_merge_tree)
    {
        return 0;
    }

    /// Window <- Sorting <- [Expression] <- [SettingQuotaAndLimits] <- ReadFromMergeTree

    auto context = read_from_merge_tree->getContext();
    if (!context->getSettings().optimize_read_in_window_order)
    {
        return 0;
    }

    const auto & query_info = read_from_merge_tree->getQueryInfo();
    const auto * select_query = query_info.query->as<ASTSelectQuery>();

    auto analyzer = std::make_unique<ExpressionAnalyzer>(query_info.query, query_info.syntax_analyzer_result, context);

    if (select_query->window()) // FIXME
    {
        LOG_DEBUG(log, "Query has window");

        ManyExpressionActions order_by_elements_actions;

        auto process_children = [&](ASTPtr node)
        {
            for (const auto & column_ast : node->children)
            {
                LOG_DEBUG(log, "After join: {}", analyzer->getColumnsAfterJoin().toString());
                LOG_DEBUG(log, "After window: {}", analyzer->getColumnsAfterWindow().toString());
                auto actions_dag = std::make_shared<ActionsDAG>(analyzer->getColumnsAfterJoin());
                analyzer->getRootActions(column_ast, false, actions_dag);
                order_by_elements_actions.emplace_back(
                        std::make_shared<ExpressionActions>(actions_dag, ExpressionActionsSettings::fromContext(context, CompileExpressions::yes)));
            }
        };

        for (const auto & ptr : select_query->window()->children)
        {
            const auto & elem = ptr->as<const ASTWindowListElement &>();
            auto ast = elem.definition.get();
            const auto & definition = ast->as<const ASTWindowDefinition &>();

            if (definition.partition_by)
            {
                process_children(definition.partition_by);
            }

            if (definition.order_by)
            {
                process_children(definition.order_by);
            }
        }

        auto order_optimizer = std::make_shared<ReadInOrderOptimizer>(
                *select_query,
                order_by_elements_actions,
                //InterpreterSelectQuery::getSortDescription(*select_query, context),
                window->getSortDescription(),
                query_info.syntax_analyzer_result);

        read_from_merge_tree->setQueryInfoOrderOptimizer(order_optimizer);

        LOG_DEBUG(log, "Order optimizer is set");

        /// If we don't have filtration, we can pushdown limit to reading stage for optimizations.
        UInt64 limit = (select_query->hasFiltration() || select_query->groupBy()) ? 0 : InterpreterSelectQuery::getLimitForSorting(*select_query, context);
        auto order_info = order_optimizer->getInputOrder(
                query_info.projection ? query_info.projection->desc->metadata : read_from_merge_tree->getStorageMetadata(),
                context,
                limit);

        read_from_merge_tree->setQueryInfoInputOrderInfo(order_info);
        /// FIXME Window+Sorting may repeat few times.
        sorting->convertToFinishSorting(order_info->order_key_prefix_descr);

        LOG_DEBUG(log, "Input order info is set");
    }
    else
    {
        LOG_DEBUG(log, "Query has no window");
    }

    return 0;
}

}
