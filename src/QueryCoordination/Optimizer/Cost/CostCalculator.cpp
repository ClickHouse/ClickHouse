#include <QueryCoordination/Optimizer/Cost/CostCalculator.h>


namespace DB
{

Float64 CostCalculator::visit(QueryPlanStepPtr step)
{
    return Base::visit(step);
}

Float64 CostCalculator::visit(ReadFromMergeTree & /*step*/)
{
    /// TODO get rows by statistics
    return std::max(1.0, 3 * statistics.getOutputRowSize());
}

Float64 CostCalculator::visitDefault(IQueryPlanStep & /*step*/)
{
    return std::max(1.0, 3 * input_statistics.front().getOutputRowSize());
}

Float64 CostCalculator::visit(AggregatingStep & step)
{
    if (!step.isPreliminaryAgg())
    {
        /// TODO get rows, cardinality by statistics
        if (child_prop.front().distribution.type == PhysicalProperties::DistributionType::Hashed)
        {
            return 6 * (Float64(input_statistics.front().getOutputRowSize()) / 3/*shard_num*/); /// fake shard_num
        }
        else
            return 6 * (input_statistics.front().getOutputRowSize());
    }
    else
    {
        /// TODO get rows, cardinality by statistics
        return 3 * (Float64(input_statistics.front().getOutputRowSize()) / 3/*shard_num*/);
    }
}

Float64 CostCalculator::visit(MergingAggregatedStep & /*step*/)
{
    /// TODO get rows, cardinality by statistics
    if (child_prop.front().distribution.type == PhysicalProperties::DistributionType::Hashed)
    {
        return 3 * (Float64(input_statistics.front().getOutputRowSize()) / 3/*shard_num*/);
    }
    return 3 * input_statistics.front().getOutputRowSize();
}

Float64 CostCalculator::visit(ExchangeDataStep & step)
{
    /// TODO get rows, cardinality by statistics
    /// TODO by type
    if (step.getDistributionType() == PhysicalProperties::DistributionType::Replicated)
    {
        return std::max(1.0, 2 * (statistics.getOutputRowSize() * 3/*shard_num*/));
    }
    return std::max(1.0, 2 * statistics.getOutputRowSize());
}

Float64 CostCalculator::visit(SortingStep & step)
{
    if (step.getPhase() == SortingStep::Phase::Preliminary)
    {
        return 3 * (Float64(input_statistics.front().getOutputRowSize()) / 3/*shard_num*/);
    }
    else if (step.getPhase() == SortingStep::Phase::Final)
    {
        return 3 * input_statistics.front().getOutputRowSize();
    }
    else
    {
        return 100 * input_statistics.front().getOutputRowSize();
    }
}

Float64 CostCalculator::visit(JoinStep & /*step*/)
{
    /// TODO Hash join memory cost low

    return 4 * (Float64(statistics.getOutputRowSize()) / 3/*shard_num*/);
}

Float64 CostCalculator::visit(LimitStep & step)
{
    if (step.getPhase() == LimitStep::Phase::Preliminary)
    {
        return 1 * (Float64(input_statistics.front().getOutputRowSize()) / 3/*shard_num*/);
    }
    else if (step.getPhase() == LimitStep::Phase::Final)
    {
        return 1 * input_statistics.front().getOutputRowSize();
    }
    else
    {
        return 100 * input_statistics.front().getOutputRowSize();
    }
}

Float64 CostCalculator::visit(TopNStep & step)
{
    if (step.getPhase() == TopNStep::Phase::Preliminary)
    {
        return 3 * (Float64(input_statistics.front().getOutputRowSize()) / 3/*shard_num*/);
    }
    else if (step.getPhase() == TopNStep::Phase::Final)
    {
        return 3 * input_statistics.front().getOutputRowSize();
    }
    else
    {
        return 100 * input_statistics.front().getOutputRowSize();
    }
}

}
