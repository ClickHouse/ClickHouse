#include <QueryCoordination/Optimizer/Cost/CostCalculator.h>


namespace DB
{

Cost CostCalculator::visit(QueryPlanStepPtr step)
{
    return Base::visit(step);
}

/// Only concerns about cpu_cost
Cost CostCalculator::visitDefault(IQueryPlanStep & step)
{
    if (typeid_cast<ISourceStep *>(&step))
        return Cost(statistics.getDataSize());

    Float64 total_input_data_size{};
    for (const auto & input : input_statistics)
        total_input_data_size += input.getDataSize();

    return Cost(total_input_data_size);
}

Cost CostCalculator::visit(ReadFromMergeTree &)
{
    return Cost(statistics.getDataSize());
}

Cost CostCalculator::visit(AggregatingStep & step)
{
    const auto & input = input_statistics.front();

    /// Two stage aggregating, first stage
    if (!step.isPreliminaryAgg())
    {
        Cost cost(input.getDataSize(), statistics.getDataSize());
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
    const auto & input = input_statistics.front();
    Cost cost(input.getDataSize(), statistics.getDataSize());
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
    const auto & input = input_statistics.front();

    /// Two stage sorting, first stage
    if (step.getPhase() == SortingStep::Phase::Preliminary)
    {
        /// cpu_cost: n * log2(n)
        auto cpu_coefficient = log2(input.getOutputRowSize());
        Cost cost(cpu_coefficient * input.getDataSize(), input.getDataSize());

        /// sorting in all shards
        cost.dividedBy(node_count);
        return cost;
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
    const auto & input = input_statistics.front();
    return Cost(input.getDataSize());
}

/// Now only hash join
Cost CostCalculator::visit(JoinStep & step)
{
    const auto & left_input = input_statistics[0];
    const auto & right_input = input_statistics[1];

    const auto & join = step.getJoin()->getTableJoin();

    /// build cost
    Float64 build_cpu_cost;
    Float64 build_mem_cost;

    build_cpu_cost = right_input.getDataSize();

    /// build table memory cost
    bool build_table_need_keep_duplicated;
    switch (join.getTableJoin().kind)
    {
        case JoinKind::Left:
        case JoinKind::Right:
        case JoinKind::Full:
            switch (join.strictness())
            {
                case JoinStrictness::Any:
                case JoinStrictness::Semi:
                case JoinStrictness::Anti:
                    build_table_need_keep_duplicated = false;
                    break;
                default:
                    build_table_need_keep_duplicated = true;
                    break;
            }
            break;
        default:
            build_table_need_keep_duplicated = true;
            break;
    }

    if (build_table_need_keep_duplicated)
    {
        build_mem_cost = right_input.getDataSize();
    }
    else
    {
        /// calculate right(build) table ndv
        Float64 right_table_ndv;

        if (right_input.hasUnknownColumn())
        {
            right_table_ndv = std::log2(right_input.getOutputRowSize());
        }
        else
        {
            Names right_table_join_on_keys;
            for (const auto & on_clause : join.getClauses())
                for (const auto & right_key : on_clause.key_names_right)
                    right_table_join_on_keys.push_back(right_key);
            right_table_ndv = 1 * right_input.getColumnStatistics(right_table_join_on_keys[0])->getNdv();
            for (size_t i = 1; i < right_table_join_on_keys.size(); i++)
                right_table_ndv = right_table_ndv * 0.8 * right_input.getColumnStatistics(right_table_join_on_keys[i])->getNdv();
        }
        build_mem_cost = (right_table_ndv / right_input.getOutputRowSize()) * right_input.getDataSize();
    }

    /// probe cost
    Float64 probe_cpu_cost = left_input.getDataSize();
    Float64 probe_mem_cost = 0.0;

    /// broad cast join
    if (child_props[1].distribution.type == Distribution::Replicated)
    {
        build_cpu_cost *= node_count;
        build_mem_cost *= node_count;
    }

    return Cost(build_cpu_cost + probe_cpu_cost, build_mem_cost + probe_mem_cost);
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

    if (distribution_type == Distribution::Replicated)
        cost.multiplyBy(node_count);

    return cost;
}

Cost CostCalculator::visit(CreatingSetStep &)
{
    const auto & input = input_statistics.front();
    return Cost(input.getDataSize(), input.getDataSize());
}

Cost CostCalculator::visit(ExtremesStep & step)
{
    return visitDefault(step);
}

Cost CostCalculator::visit(RollupStep &)
{
    const auto & input = input_statistics.front();
    return Cost(input.getDataSize(), input.getDataSize());
}

Cost CostCalculator::visit(CubeStep &)
{
    const auto & input = input_statistics.front();
    return Cost(input.getDataSize(), input.getDataSize());
}

Cost CostCalculator::visit(TotalsHavingStep & step)
{
    return visitDefault(step);
}

Cost CostCalculator::visit(TopNStep & step)
{
    const auto & input = input_statistics.front();
    if (step.getPhase() == TopNStep::Phase::Preliminary)
    {
        /// cpu_cost: n * log2(n)
        auto cpu_coefficient = log2(input.getOutputRowSize());
        Cost cost(cpu_coefficient * input.getDataSize(), input.getDataSize());

        /// sorting in all shards
        cost.dividedBy(node_count);
        return cost;
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
