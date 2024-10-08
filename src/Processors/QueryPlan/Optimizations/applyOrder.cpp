#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>

#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>

#include <Functions/IFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace QueryPlanOptimizations
{

struct SortingProperty
{
    /// Sorting scope.
    enum class SortScope : uint8_t
    {
        Stream = 0, /// Each data steam is sorted
        Global = 1, /// Data is globally sorted
    };

    SortDescription sort_description = {};
    SortScope sort_scope = SortScope::Stream;
};

SortingProperty applyOrder(QueryPlan::Node * parent, SortingProperty * properties, const QueryPlanOptimizationSettings & optimization_settings)
{
    if (const auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree*>(parent->step.get()))
        return {read_from_merge_tree->getSortDescription(), SortingProperty::SortScope::Stream};

    if (const auto * aggregating_step = typeid_cast<AggregatingStep *>(parent->step.get()))
    {
        /// TODO: here we can apply aggregation-in-order after some sorting.

        auto sort_description = aggregating_step->getSortDescription();
        if (!sort_description.empty())
            return {std::move(sort_description), SortingProperty::SortScope::Global};
    }

    if (auto * mergine_aggeregated = typeid_cast<MergingAggregatedStep *>(parent->step.get()))
    {
        enableMemoryBoundMerging(*parent);

        auto sort_description = mergine_aggeregated->getSortDescription();
        if (!sort_description.empty())
            return {std::move(sort_description), SortingProperty::SortScope::Global};
    }

    if (auto * distinct_step = typeid_cast<DistinctStep *>(parent->step.get()))
    {
        /// Do not apply distinct-in-order second time.
        /// Also, prefer sorting from propertires against Distinct sorting description,
        /// cause the last one might be shorter, or may haver additional monotonic functions.
        if (optimization_settings.distinct_in_order && distinct_step->getSortDescription().empty() &&
            (properties->sort_scope == SortingProperty::SortScope::Global
            || (distinct_step->isPreliminary() && properties->sort_scope == SortingProperty::SortScope::Stream)))
        {
            SortDescription prefix_sort_description;
            const auto & column_names = distinct_step->getColumnNames();
            std::unordered_set<std::string_view> columns(column_names.begin(), column_names.end());

            for (auto & sort_column_desc : properties->sort_description)
            {
                if (!columns.contains(sort_column_desc.column_name))
                    break;

                prefix_sort_description.emplace_back(sort_column_desc);
            }

            distinct_step->applyOrder(std::move(prefix_sort_description));
        }

        /// Distinct never breaks global order
        if (properties->sort_scope == SortingProperty::SortScope::Global)
            return *properties;

        /// Preliminary Distinct also does not break stream order
        if (distinct_step->isPreliminary() && properties->sort_scope == SortingProperty::SortScope::Stream)
            return *properties;
    }

    if (auto * expression_step = typeid_cast<ExpressionStep *>(parent->step.get()))
    {
        applyActionsToSortDescription(properties->sort_description, expression_step->getExpression());
        return std::move(*properties);
    }

    if (auto * filter_step = typeid_cast<FilterStep *>(parent->step.get()))
    {
        const auto & expr = filter_step->getExpression();
        const ActionsDAG::Node * out_to_skip = nullptr;
        if (filter_step->removesFilterColumn())
        {
            out_to_skip = expr.tryFindInOutputs(filter_step->getFilterColumnName());
            if (!out_to_skip)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Output nodes for ActionsDAG do not contain filter column name {}. DAG:\n{}",
                    filter_step->getFilterColumnName(),
                    expr.dumpDAG());
        }

        applyActionsToSortDescription(properties->sort_description, expr, out_to_skip);
        return std::move(*properties);
    }

    if (auto * sorting_step = typeid_cast<SortingStep *>(parent->step.get()))
    {
        if (optimization_settings.optimize_sorting_by_input_stream_properties
            && !sorting_step->hasPartitions() && sorting_step->getType() == SortingStep::Type::Full)
        {
            auto common_prefix = commonPrefix(properties->sort_description, sorting_step->getSortDescription());
            if (!common_prefix.empty())
                /// Buffering is useful for reading from MergeTree, and it is applied in optimizeReadInOrder only.
                sorting_step->convertToFinishSorting(common_prefix, /*use_buffering*/ false);
        }

        auto scope = sorting_step->hasPartitions() ? SortingProperty::SortScope::Stream : SortingProperty::SortScope::Global;
        return {sorting_step->getSortDescription(), scope};
    }

    if (auto * transforming = dynamic_cast<ITransformingStep *>(parent->step.get()))
    {
        if (transforming->getDataStreamTraits().preserves_sorting)
            return std::move(*properties);
    }

    if (auto * /*union_step*/ _ = typeid_cast<UnionStep *>(parent->step.get()))
    {
        SortDescription common_sort_description = std::move(properties->sort_description);
        auto sort_scope = properties->sort_scope;

        for (size_t i = 1; i < parent->children.size(); ++i)
        {
            common_sort_description = commonPrefix(common_sort_description, properties[i].sort_description);
            sort_scope = std::min(sort_scope, properties[i].sort_scope);
        }

        if (!common_sort_description.empty())
            return {std::move(common_sort_description), sort_scope};
    }

    return {};
}

void applyOrder(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root)
{
    Stack stack;
    stack.push_back({.node = &root});

    using SortingPropertyStack = std::vector<SortingProperty>;
    SortingPropertyStack properties;

    while (!stack.empty())
    {
        auto & frame = stack.back();

        /// Traverse all children first.
        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }

        auto * node = frame.node;
        stack.pop_back();

        auto it = properties.begin() + (properties.size() - node->children.size());
        auto property = applyOrder(node, (it == properties.end()) ? nullptr : &*it, optimization_settings);
        properties.erase(it, properties.end());
        properties.push_back(std::move(property));
    }
}

}

}
