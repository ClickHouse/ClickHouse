#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Interpreters/ActionsDAG.h>
#include <Common/typeid_cast.h>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::QueryPlanOptimizations
{

size_t tryPushDownLimit(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes)
{
    if (parent_node->children.size() != 1)
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent = parent_node->step;
    auto & child = child_node->step;
    auto * filter = typeid_cast<FilterStep *>(parent.get());

    if (!filter)
        return 0;

    const auto & expression = filter->getExpression();
    const auto & filter_column_name = filter->getFilterColumnName();
    bool removes_filter = filter->removesFilterColumn();

    if (auto * aggregating = typeid_cast<AggregatingStep *>(child.get()))
    {
        const auto & params = aggregating->getParams();

        Names keys;
        keys.reserve(params.keys.size());
        for (auto pos : params.keys)
            keys.push_back(params.src_header.getByPosition(pos).name);

        if (auto split_filter = expression->splitActionsForFilter(filter_column_name, removes_filter, keys))
        {
            auto it = expression->getIndex().find(filter_column_name);
            if (it == expression->getIndex().end())
            {
                if (!removes_filter)
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                                    "Filter column {} was removed from ActionsDAG but it is needed in result. DAG:\n{}",
                                    filter_column_name, expression->dumpDAG());

                parent = std::make_unique<ExpressionStep>(child->getOutputStream(), expression);
            }

            /// Add new Filter step before Aggregating.
            /// Expression/Filter -> Aggregating -> Something
            auto & node = nodes.emplace_back();
            node.children.swap(child_node->children);
            child_node->children.emplace_back(&node);
            /// Expression/Filter -> Aggregating -> Filter -> Something

            /// New filter column is added to the end.
            auto split_filter_column_name = (*split_filter->getIndex().rbegin())->result_name;
            node.step = std::make_unique<FilterStep>(
                    node.children.at(0)->step->getOutputStream(),
                    std::move(split_filter), std::move(split_filter_column_name), true);

            return 3;
        }
    }

    return 0;
}

}
