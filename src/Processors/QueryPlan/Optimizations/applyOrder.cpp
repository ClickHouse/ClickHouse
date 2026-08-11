#include <Processors/QueryPlan/Optimizations/DataPropertyDerivation.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/NegativeLimitByStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>


namespace DB
{

namespace QueryPlanOptimizations
{

static SortingProperty applyOrder(QueryPlan::Node * parent, SortingProperty * properties, const QueryPlanOptimizationSettings & optimization_settings)
{
    const auto child_properties = parent->children.empty() ? std::span<const SortingProperty>{}
                                                           : std::span<const SortingProperty>(properties, parent->children.size());

    if (typeid_cast<MergingAggregatedStep *>(parent->step.get()))
        enableMemoryBoundMerging(*parent);

    if (auto * distinct_step = typeid_cast<DistinctStep *>(parent->step.get()); distinct_step && !child_properties.empty())
    {
        const auto & input = child_properties.front();
        /// Do not apply distinct-in-order second time. Prefer the input property because
        /// the step's own description may be shorter or include monotonic functions.
        if (optimization_settings.distinct_in_order && distinct_step->getSortDescription().empty()
            && (input.sort_scope == SortingScope::Global || (distinct_step->isPreliminary() && input.sort_scope == SortingScope::Stream)))
        {
            distinct_step->applyOrder(getCollationAwareSortPrefixInColumns(input.sort_description, distinct_step->getColumnNames()));
        }
    }

    if (auto * sorting_step = typeid_cast<SortingStep *>(parent->step.get()); sorting_step && !child_properties.empty())
    {
        if (optimization_settings.optimize_sorting_by_input_stream_properties && !sorting_step->hasPartitions()
            && sorting_step->getType() == SortingStep::Type::Full)
        {
            auto common_prefix = commonPrefix(child_properties.front().sort_description, sorting_step->getSortDescription());
            if (!common_prefix.empty())
                /// Buffering is useful for reading from `MergeTree`, and is applied in `optimizeReadInOrder` only.
                sorting_step->convertToFinishSorting(common_prefix, /*use_buffering*/ false, false);
        }
    }

    if (auto * limit_by_step = typeid_cast<LimitByStep *>(parent->step.get()); limit_by_step && !child_properties.empty())
    {
        const auto & input = child_properties.front();
        if (input.sort_scope == SortingScope::Global)
        {
            auto prefix = getCollationAwareSortPrefixInColumns(input.sort_description, limit_by_step->getColumns());
            if (prefix.size() == limit_by_step->getColumns().size())
                limit_by_step->applyOrder(prefix);
        }
    }

    if (auto * negative_limit_by_step = typeid_cast<NegativeLimitByStep *>(parent->step.get());
        negative_limit_by_step && !child_properties.empty())
    {
        const auto & input = child_properties.front();
        if (input.sort_scope == SortingScope::Global)
        {
            auto prefix = getCollationAwareSortPrefixInColumns(input.sort_description, negative_limit_by_step->getColumns());
            if (prefix.size() == negative_limit_by_step->getColumns().size())
                negative_limit_by_step->applyOrder(prefix);
        }
    }

    auto sorting_derivation = deriveSortingProperty(*parent->step, child_properties);
    if (sorting_derivation.requires_union_narrowing_disabled)
    {
        auto * union_step = typeid_cast<UnionStep *>(parent->step.get());
        chassert(union_step);
        union_step->disableNarrowing();
    }
    return std::move(sorting_derivation.property);
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
