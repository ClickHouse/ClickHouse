#include <Optimizer/Cost/CostCalculator.h>


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
        return Cost(cost_weight, statistics.getDataSize());

    Float64 total_input_data_size{};
    for (const auto & input : input_statistics)
        total_input_data_size += input.getDataSize();

    return Cost(cost_weight, total_input_data_size);
}

Cost CostCalculator::visit(ReadFromMergeTree &)
{
    return Cost(cost_weight, statistics.getDataSize());
}

Cost CostCalculator::visit(AggregatingStep & step)
{
    const auto & input = input_statistics.front();

    /// Single stage aggregating
    if (!step.isPreliminaryAgg())
    {
        if (cbo_settings.cbo_aggregating_mode == CBOStepExecutionMode::TWO_STAGE)
            return Cost::infinite(cost_weight);
        return Cost(cost_weight, input.getDataSize(), statistics.getDataSize());
    }
    /// Two stage aggregating, first stage
    else
    {
        if (cbo_settings.cbo_aggregating_mode == CBOStepExecutionMode::ONE_STAGE)
            return Cost::infinite(cost_weight);

        Cost cost(cost_weight, input.getDataSize(), statistics.getDataSize());
        cost.dividedBy(node_count);
        return cost;
    }
}

/// Two stage aggregating, second stage
Cost CostCalculator::visit(MergingAggregatedStep & step)
{
    const auto & input = input_statistics.front();
    Cost cost(cost_weight, input.getDataSize(), statistics.getDataSize());

    if (child_props[0].distribution.type == Distribution::Hashed)
        cost.dividedBy(node_count);

    /// Uniq and uniqExact function in merging stage takes long time than one stage agg in some data quantities.
    /// So here we add a coefficient to use one stage aggregating.
    bool has_uniq_or_uniq_exact{};
    for (const auto & aggregate : step.getParams().aggregates)
    {
        if (aggregate.function->getName() == "uniq" || aggregate.function->getName() == "uniqExact")
        {
            has_uniq_or_uniq_exact = true;
            break;
        }
    }
    if (has_uniq_or_uniq_exact)
    {
        auto coefficient = cost_settings.cost_merge_agg_uniq_calculation_weight;
        cost.multiplyBy(static_cast<size_t>(coefficient));
    }
    return cost;
}

Cost CostCalculator::visit(ExpressionStep &)
{
    return Cost(cost_weight, input_statistics.front().getDataSize());
}

Cost CostCalculator::visit(FilterStep &)
{
    return Cost(cost_weight, input_statistics.front().getDataSize());
}

Cost CostCalculator::visit(SortingStep & step)
{
    const auto & input = input_statistics.front();
    auto weight = cost_settings.cost_pre_sorting_operation_weight;

    /// Two stage sorting, first stage
    if (step.getPhase() == SortingStep::Phase::Preliminary)
    {
        if (cbo_settings.cbo_sorting_mode == CBOStepExecutionMode::ONE_STAGE)
            return Cost::infinite(cost_weight);

        /// cpu_cost: n * log2(n)
        auto cpu_coefficient = log2(input.getOutputRowSize()) * weight;
        Cost cost(cost_weight, cpu_coefficient * input.getDataSize(), input.getDataSize());

        /// TODO Just like ExchangeDataStep, we do not know the real nodes count sending data simultaneously.
        cost.dividedBy(node_count);
        return cost;
    }
    /// Two stage sorting, second stage
    else if (step.getPhase() == SortingStep::Phase::Final)
    {
        return Cost(cost_weight, input.getDataSize(), input.getDataSize());
    }
    /// Single stage sorting
    else
    {
        if (cbo_settings.cbo_sorting_mode == CBOStepExecutionMode::TWO_STAGE)
            return Cost::infinite(cost_weight);

        auto cpu_coefficient = log2(input.getOutputRowSize()) * weight;
        return Cost(cost_weight, cpu_coefficient * input.getDataSize(), input.getDataSize());
    }
}

Cost CostCalculator::visit(LimitStep & step)
{
    const auto & input = input_statistics.front();

    /// Two stage limiting, first stage
    if (step.getPhase() == LimitStep::Phase::Preliminary)
    {
        if (cbo_settings.cbo_limiting_mode == CBOStepExecutionMode::ONE_STAGE)
            return Cost::infinite(cost_weight);

        /// cpu_cost
        Cost cost(cost_weight, input.getDataSize());

        /// sorting in all shards
        cost.dividedBy(node_count);
        return cost;
    }
    /// Two stage limiting, second stage
    else if (step.getPhase() == LimitStep::Phase::Final)
    {
        return Cost(cost_weight, input.getDataSize(), input.getDataSize());
    }
    /// Single stage limiting
    else
    {
        if (cbo_settings.cbo_limiting_mode == CBOStepExecutionMode::TWO_STAGE)
            return Cost::infinite(cost_weight);
        else
            return Cost(cost_weight, input.getDataSize());
    }
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

    return Cost(cost_weight, build_cpu_cost + probe_cpu_cost, build_mem_cost + probe_mem_cost);
}

Cost CostCalculator::visit(UnionStep & step)
{
    return visitDefault(step);
}

Cost CostCalculator::visit(ExchangeDataStep & step)
{
    /// ExchangeDataStep is a source step which has no input.
    const auto & input = statistics;
    auto distribution_type = step.getDistribution().type;

    Cost cost(cost_weight, 0.0, 0.0, input.getDataSize());

    if (distribution_type != Distribution::Replicated)
        cost.multiplyBy(node_count);

    /// TODO ExchangeDataStep required child distribution is any and here we do not know the real nodes count
    /// sending data simultaneously, but most time it is all shards, so we divide node_count.
    cost.dividedBy(node_count);
    return cost;
}

Cost CostCalculator::visit(CreatingSetStep &)
{
    const auto & input = input_statistics.front();
    return Cost(cost_weight, input.getDataSize(), input.getDataSize());
}

Cost CostCalculator::visit(ExtremesStep & step)
{
    return visitDefault(step);
}

Cost CostCalculator::visit(RollupStep &)
{
    const auto & input = input_statistics.front();
    return Cost(cost_weight, input.getDataSize(), input.getDataSize());
}

Cost CostCalculator::visit(CubeStep &)
{
    const auto & input = input_statistics.front();
    return Cost(cost_weight, input.getDataSize(), input.getDataSize());
}

Cost CostCalculator::visit(TotalsHavingStep & step)
{
    return visitDefault(step);
}

Cost CostCalculator::visit(TopNStep & step)
{
    const auto & input = input_statistics.front();
    auto weight = cost_settings.cost_pre_sorting_operation_weight;

    /// Two stage TopN, first stage
    if (step.getPhase() == TopNStep::Phase::Preliminary)
    {
        if (cbo_settings.cbo_topn_mode == CBOStepExecutionMode::ONE_STAGE)
            return Cost::infinite(cost_weight);

        /// cpu_cost
        auto cpu_coefficient = log2(input.getOutputRowSize()) * weight;
        Cost cost(cost_weight, cpu_coefficient * input.getDataSize(), input.getDataSize());

        /// TODO Just like ExchangeDataStep, we do not know the real nodes count sending data simultaneously.
        cost.dividedBy(node_count);
        return cost;
    }
    /// Two stage TopN, second stage
    else if (step.getPhase() == TopNStep::Phase::Final)
    {
        return Cost(cost_weight, input.getDataSize(), input.getDataSize());
    }
    /// Single stage TopN
    else
    {
        if (cbo_settings.cbo_topn_mode == CBOStepExecutionMode::TWO_STAGE)
            return Cost::infinite(cost_weight);

        auto cpu_coefficient = log2(input.getOutputRowSize()) * weight;
        return Cost(cost_weight, cpu_coefficient * input.getDataSize(), input.getDataSize());
    }
}

}
