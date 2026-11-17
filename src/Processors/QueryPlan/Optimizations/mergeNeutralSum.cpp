#include <Processors/QueryPlan/Optimizations/mergeNeutralSum.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Functions/IFunction.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Storages/StorageMerge.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

bool canCrossNeutralReduction(const ActionsDAG & actions)
{
    if (actions.hasNonDeterministicOrStatefulFunctions())
        return false;
    for (const auto & node : actions.getNodes())
        if (node.type == ActionsDAG::ActionType::ARRAY_JOIN || node.type == ActionsDAG::ActionType::PLACEHOLDER
            || !node.isDeterministic() || (node.function_base && node.function_base->isStateful()))
            return false;
    return true;
}

Names neutralSumInputs(const ActionsDAG::Node & root)
{
    Names result;
    std::vector<const ActionsDAG::Node *> pending{&root};
    std::unordered_set<const ActionsDAG::Node *> visited;
    while (!pending.empty())
    {
        const auto * node = pending.back();
        pending.pop_back();
        if (!visited.insert(node).second)
            continue;
        if (node->type == ActionsDAG::ActionType::INPUT)
        {
            if (!std::ranges::contains(result, node->result_name))
                result.push_back(node->result_name);
        }
        else
            pending.insert(pending.end(), node->children.begin(), node->children.end());
    }
    return result;
}

void collectNeutralSumProofs(QueryPlan::Node & root)
{
    std::vector<QueryPlan::Node *> pending{&root};
    while (!pending.empty())
    {
        auto * node = pending.back();
        pending.pop_back();
        pending.insert(pending.end(), node->children.begin(), node->children.end());
        const auto * aggregate = typeid_cast<AggregatingStep *>(node->step.get());
        if (!aggregate || aggregate->isGroupingSets() || node->children.size() != 1)
            continue;
        const auto params = aggregate->getAggregatorParameters();
        if (params.aggregates.size() != 1 || params.keys.empty() || params.only_merge)
            continue;
        const auto & description = params.aggregates.front();
        if (description.function->getName() != "sum" || description.argument_names.size() != 1)
            continue;
        Names required = params.keys;
        required.push_back(description.argument_names.front());
        auto * source = node->children.front();
        bool supported = true;
        while (source && supported)
        {
            if (auto * merge = typeid_cast<ReadFromMerge *>(source->step.get()))
            {
                const auto & header = source->step->getOutputHeader();
                if (header->has(required.back()) && header->getByName(required.back()).type->isNullable())
                {
                    auto measure = required.back();
                    required.pop_back();
                    merge->setNeutralSumProof(std::move(measure), std::move(required));
                }
                break;
            }
            const ActionsDAG * actions = nullptr;
            if (const auto * expression = typeid_cast<ExpressionStep *>(source->step.get()))
                actions = &expression->getExpression();
            else if (const auto * filter = typeid_cast<FilterStep *>(source->step.get()))
                actions = &filter->getExpression();
            if (!actions || source->children.size() != 1 || !canCrossNeutralReduction(*actions))
                break;
            if (const auto * filter = typeid_cast<FilterStep *>(source->step.get()))
            {
                const auto inputs = neutralSumInputs(actions->findInOutputs(filter->getFilterColumnName()));
                if (std::ranges::contains(inputs, required.back()))
                    break;
            }
            Names mapped_keys;
            for (size_t index = 0; index < required.size(); ++index)
            {
                const auto & outputs = actions->getOutputs();
                const auto it = std::ranges::find_if(outputs, [&](const auto * output) { return output->result_name == required[index]; });
                if (it == outputs.end())
                {
                    supported = false;
                    break;
                }
                const auto * output = *it;
                while (output->type == ActionsDAG::ActionType::ALIAS)
                    output = output->children.front();
                if (index + 1 == required.size())
                {
                    /// Only a direct nullable input proves a neutral measure.
                    if (output->type != ActionsDAG::ActionType::INPUT)
                        supported = false;
                    else
                        mapped_keys.push_back(output->result_name);
                    break;
                }
                /// Deterministic grouping expressions may collapse several base
                /// keys. Keeping the finer base grouping still preserves NULL sums.
                for (const auto & input : neutralSumInputs(*output))
                    if (!std::ranges::contains(mapped_keys, input))
                        mapped_keys.push_back(input);
            }
            required = std::move(mapped_keys);
            source = source->children.front();
        }
    }
}


}
