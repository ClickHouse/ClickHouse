#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Functions/IFunction.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/SortingStep.h>

namespace DB::QueryPlanOptimizations
{

struct SortingProperty
{
    /// Sorting scope. Please keep the mutual order (more strong mode should have greater value).
    enum class SortScope : uint8_t
    {
        None   = 0,
        Chunk  = 1, /// Separate chunks are sorted
        Stream = 2, /// Each data steam is sorted
        Global = 3, /// Data is globally sorted
    };

    /// It is not guaranteed that header has columns from sort_description.
    SortDescription sort_description = {};
    SortScope sort_scope = SortScope::None;
};

struct PossiblyMonotonicChain
{
    const ActionsDAG::Node * input_node = nullptr;
    std::vector<size_t> non_const_arg_pos;
    bool changes_order = false;
    bool is_strict = true;
};

/// Build a chain of functions which may be monotonic.
static PossiblyMonotonicChain buildPossiblyMonitinicChain(const ActionsDAG::Node * node)
{
    std::vector<size_t> chain;

    while (node->type != ActionsDAG::ActionType::INPUT)
    {
        if (node->type == ActionsDAG::ActionType::ALIAS)
        {
            node = node->children.at(0);
            continue;
        }

        if (node->type != ActionsDAG::ActionType::FUNCTION)
            break;

        size_t num_children = node->children.size();
        if (num_children == 0)
            break;

        const auto & func = node->function_base;
        if (!func->hasInformationAboutMonotonicity())
            break;

        std::optional<size_t> non_const_arg;
        for (size_t i = 0; i < num_children; ++i)
        {
            const auto * child = node->children[i];
            if (child->type == ActionsDAG::ActionType::COLUMN)
                continue;

            if (non_const_arg != std::nullopt)
            {
                /// Second non-constant arg
                non_const_arg = {};
                break;
            }

            non_const_arg = i;
        }

        if (non_const_arg == std::nullopt)
            break;

        chain.push_back(*non_const_arg);
        node = node->children[*non_const_arg];
    }

    if (node->type != ActionsDAG::ActionType::INPUT)
        return {};

    return {node, std::move(chain)};
}

/// Check wheter all the function in chain are monotonic
bool isMonotonicChain(const ActionsDAG::Node * node, PossiblyMonotonicChain & chain)
{
    auto it = chain.non_const_arg_pos.begin();
    while (node != chain.input_node)
    {
        if (node->type != ActionsDAG::ActionType::FUNCTION)
        {
            node = node->children[0];
            continue;
        }

        size_t pos = *it;
        ++it;

        const auto & type = node->children[pos]->result_type;
        const Field field{};
        auto monotonicity = node->function_base->getMonotonicityForRange(*type, field, field);
        if (!monotonicity.is_monotonic)
            break;

        if (!monotonicity.is_positive)
            chain.changes_order = !chain.changes_order;

        chain.is_strict = chain.is_strict && monotonicity.is_strict;

        node = node->children[pos];
    }

    return node == chain.input_node;
}

static void applyActionsToSortDescription(
    SortDescription & description,
    const ActionsDAG & dag,
    const ActionsDAG::Node * out_to_skip = nullptr)
{
    if (description.empty())
        return;

    if (dag.hasArrayJoin())
        return;

    const size_t descr_size = description.size();

    const auto & inputs = dag.getInputs();
    const size_t num_inputs = inputs.size();

    struct SortColumn
    {
        const ActionsDAG::Node * input = nullptr;
        const ActionsDAG::Node * output = nullptr;
        bool is_monotonic_chain = false;
        bool is_strict = true;
        bool changes_order = false;
    };

    std::vector<SortColumn> sort_columns(descr_size);
    std::unordered_map<const ActionsDAG::Node *, size_t> input_to_sort_column;

    {
        std::unordered_map<std::string_view, size_t> desc_name_to_pos;
        for (size_t pos = 0; pos < descr_size; ++pos)
            desc_name_to_pos.emplace(description[pos].column_name, pos);

        for (size_t pos = 0; pos < num_inputs; ++pos)
        {
            auto it = desc_name_to_pos.find(inputs[pos]->result_name);
            if (it != desc_name_to_pos.end() && !sort_columns[it->second].input)
            {
                sort_columns[it->second].input = inputs[pos];
                input_to_sort_column[inputs[pos]] = it->second;
            }
        }
    }

    for (const auto * output : dag.getOutputs())
    {
        if (output == out_to_skip)
            continue;

        auto chain = buildPossiblyMonitinicChain(output);
        if (!chain.input_node)
            break;

        auto it = input_to_sort_column.find(chain.input_node);
        if (it == input_to_sort_column.end())
            break;

        SortColumn & sort_column = sort_columns[it->second];

        /// Already found better chain
        bool has_functions = !chain.non_const_arg_pos.empty();
        bool is_monotonicity_improved = !has_functions && sort_column.is_monotonic_chain;
        if (sort_column.output && !is_monotonicity_improved && sort_column.is_strict)
            break;

        if (has_functions && !isMonotonicChain(output, chain))
            break;

        bool is_strictness_improved = chain.is_strict && !sort_column.is_strict;
        if (sort_column.output && !is_strictness_improved)
            break;

        sort_column.output = output;
        sort_column.is_monotonic_chain = has_functions;
        sort_column.changes_order = chain.changes_order;
        sort_column.is_strict = chain.is_strict;
    }

    size_t prefix_size = 0;
    while (prefix_size < descr_size)
    {
        const auto & sort_colunm = sort_columns[prefix_size];

        /// No input is allowed : it means DAG did not use the column.
        if (sort_colunm.input && !sort_colunm.output)
            break;

        auto & descr = description[prefix_size];
        ++prefix_size;

        if (sort_colunm.output)
            descr.column_name = sort_colunm.output->result_name;

        if (sort_colunm.changes_order)
            descr.direction *= -1;

        if (!sort_colunm.is_strict)
            break;
    }

    description.resize(prefix_size);
}


SortingProperty applyOrder(QueryPlan::Node * parent, SortingProperty * properties, const QueryPlanOptimizationSettings & optimization_settings)
{
    if (const auto * aggregating_step = typeid_cast<AggregatingStep *>(parent->step.get()))
    {
        // if (optimization_settings.aggregation_in_order && !properties->sort_description.empty()
        //     (properties->sort_scope == SortingProperty::SortScope::Stream || properties->sort_scope == SortingProperty::SortScope::Global))
        //     aggregating_step->applyOrder(properties->sort_description);

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
        if (optimization_settings.distinct_in_order &&
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
        auto scope = sorting_step->hasPartitions() ? SortingProperty::SortScope::Stream : SortingProperty::SortScope::Global;
        return {sorting_step->getSortDescription(), scope};
    }

    if (auto * transforming = dynamic_cast<ITransformingStep *>(parent->step.get()))
    {
        if (transforming->getDataStreamTraits().preserves_sorting)
            return std::move(*properties);
    }

    if (auto * union_step = typeid_cast<UnionStep *>(parent->step.get()))
    {
        SortDescription common_sort_description = std::move(properties->sort_description);
        auto sort_scope = properties->sort_scope;

        for (size_t i = 1; i < parent->children.size(); ++i)
        {
            common_sort_description = commonPrefix(common_sort_description, properties[i].sort_description);
            sort_scope = std::min(sort_scope, properties[i].sort_scope);
        }

        if (!common_sort_description.empty() && sort_scope >= SortingProperty::SortScope::Chunk)
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
        auto property = applyOrder(node, &*it, optimization_settings);
        properties.erase(it, properties.end());
        properties.push_back(std::move(property));
    }
}

}
