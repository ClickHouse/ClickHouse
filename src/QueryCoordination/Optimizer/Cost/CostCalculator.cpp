#include <QueryCoordination/Optimizer/Cost/CostCalculator.h>


namespace DB
{

Cost CostCalculator::visit(QueryPlanStepPtr step)
{
    return Base::visit(step);
}

Cost CostCalculator::visitDefault(IQueryPlanStep & step)
{
    if (typeid_cast<ISourceStep *>(&step))
    {
        return Cost(statistics.getDataSize());
    }

    Float64 total_input_data_size{};
    for (auto & input : input_statistics)
        total_input_data_size += input.getDataSize();

    return Cost(total_input_data_size);
}

Cost CostCalculator::visit(ReadFromMergeTree &)
{
    return Cost(statistics.getDataSize());
}

Cost CostCalculator::visit(AggregatingStep & step)
{
    auto & input = input_statistics.front();

    /// Two stage aggregating, first stage
    if (!step.isPreliminaryAgg())
    {
        Cost cost(input.getDataSize(), statistics.getDataSize());
        cost.dividedBy(node_count); /// calculated by all nodes
        return cost;
    }
    /// Single stage aggregating
    else
    {
        Cost cost(input.getDataSize(), statistics.getDataSize());
        return cost;
    }
}

/// Two stage aggregating, second stage
Cost CostCalculator::visit(MergingAggregatedStep &)
{
    auto & input = input_statistics.front();
    Cost cost(input.getDataSize(), statistics.getDataSize());
    cost.dividedBy(node_count); /// calculated by all nodes
    return cost;
}

Cost CostCalculator::visit(ExpressionStep &)
{
    return Cost(input_statistics.front().getDataSize());
}

Cost CostCalculator::visit(FilterStep &)
{
    return Cost(input_statistics.front().getDataSize());
}

Cost CostCalculator::visit(SortingStep & step)
{
    auto & input = input_statistics.front();

    /// Two stage sorting, first stage
    if (step.getPhase() == SortingStep::Phase::Preliminary)
    {
        auto cpu_coefficient = log2(input.getOutputRowSize());
        return Cost(cpu_coefficient * input.getDataSize(), input.getDataSize());
    }
    /// Two stage sorting, second stage
    else if (step.getPhase() == SortingStep::Phase::Final)
    {
        return Cost(input.getDataSize(), input.getDataSize());
    }
    /// Single stage sorting
    else
    {
        auto cpu_coefficient = log2(input.getOutputRowSize());
        return Cost(cpu_coefficient * input.getDataSize(), input.getDataSize());
    }
}

Cost CostCalculator::visit(LimitStep &)
{
    auto & input = input_statistics.front();
    return Cost(input.getDataSize());
}

Cost CostCalculator::visit(JoinStep &)
{
    /// Now only hash join, this is a very simple algo, but it works.
    auto & input = input_statistics.front();
    return Cost(input.getDataSize(), statistics.getDataSize());
}

Cost CostCalculator::visit(UnionStep & step)
{
    return visitDefault(step);
}

Cost CostCalculator::visit(ExchangeDataStep & step)
{
    /// ExchangeDataStep is a source step which has no input.
    auto & input = statistics;
    auto & distribution_type = step.getDistribution().type;

    Cost cost(input.getDataSize(), 0.0, input.getDataSize());

    if (distribution_type == PhysicalProperties::DistributionType::Hashed)
        cost.dividedBy(node_count);
    else if (distribution_type == PhysicalProperties::DistributionType::Replicated)
        cost.multiplyBy(node_count);
    else if (distribution_type == PhysicalProperties::DistributionType::Singleton)
    {
    }
    return cost;
}

Cost CostCalculator::visit(CreatingSetStep &)
{
    auto & input = input_statistics.front();
    return Cost(input.getDataSize(), input.getDataSize());
}

Cost CostCalculator::visit(ExtremesStep & step)
{
    return visitDefault(step);
}

Cost CostCalculator::visit(RollupStep &)
{
    auto & input = input_statistics.front();
    return Cost(input.getDataSize(), input.getDataSize());
}

Cost CostCalculator::visit(CubeStep &)
{
    auto & input = input_statistics.front();
    return Cost(input.getDataSize(), input.getDataSize());
}

Cost CostCalculator::visit(TotalsHavingStep & step)
{
    return visitDefault(step);
}

Cost CostCalculator::visit(TopNStep & step)
{
    auto & input = input_statistics.front();
    if (step.getPhase() == TopNStep::Phase::Preliminary)
    {
        auto cpu_coefficient = log2(input.getOutputRowSize());
        return Cost(cpu_coefficient * input.getDataSize(), input.getDataSize());
    }
    else if (step.getPhase() == TopNStep::Phase::Final)
    {
        return Cost(input.getDataSize(), input.getDataSize());
    }
    /// Single stage sorting
    else
    {
        auto cpu_coefficient = log2(input.getOutputRowSize());
        return Cost(cpu_coefficient * input.getDataSize(), input.getDataSize());
    }
}

}
