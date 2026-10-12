#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>

namespace DB::QueryPlanOptimizations
{

size_t tryMinMaxFromFormatStatistics(QueryPlan::Node * parent_node, QueryPlan::Nodes & /*nodes*/, const Optimization::ExtraSettings & /*settings*/)
{
    const auto * aggregating = typeid_cast<const AggregatingStep *>(parent_node->step.get());
    if (!aggregating || parent_node->children.size() != 1 || aggregating->isGroupingSets())
        return 0;

    const auto & params = aggregating->getParams();
    if (!params.keys.empty() || params.aggregates.empty())
        return 0;

    NameSet arguments;
    for (const auto & aggregate : params.aggregates)
    {
        const String function_name = aggregate.function->getName();
        if ((function_name != "min" && function_name != "max") || aggregate.argument_names.size() != 1 || !aggregate.parameters.empty())
            return 0;
        arguments.insert(aggregate.argument_names.front());
    }

    QueryPlan::Node * node = parent_node->children.front();
    while (const auto * expression = typeid_cast<const ExpressionStep *>(node->step.get()))
    {
        /// An ARRAY JOIN may remove rows (an empty array), and that can change the minimum or the maximum.
        if (!expression->getTransformTraits().preserves_number_of_rows)
            return 0;

        const auto & outputs = expression->getExpression().getOutputs();
        NameSet inputs;
        for (const auto & argument : arguments)
        {
            const auto it = std::find_if(outputs.begin(), outputs.end(), [&](const auto * output) { return output->result_name == argument; });
            if (it == outputs.end())
                return 0;
            const ActionsDAG::Node * input = *it;
            while (input->type == ActionsDAG::ActionType::ALIAS)
                input = input->children.front();
            if (input->type != ActionsDAG::ActionType::INPUT)
                return 0;
            inputs.insert(input->result_name);
        }
        arguments = std::move(inputs);

        if (node->children.size() != 1)
            return 0;
        node = node->children.front();
    }

    auto * source = dynamic_cast<SourceStepWithFilterBase *>(node->step.get());
    if (!source)
        return 0;

    /// The source replaces all of its rows, so all of its columns must be arguments of `min` / `max`.
    for (const auto & column : *node->step->getOutputHeader())
        if (!arguments.contains(column.name))
            return 0;

    if (source->supportsMinMaxFromStatistics())
        source->setMinMaxFromStatistics();

    return 0;
}

}
