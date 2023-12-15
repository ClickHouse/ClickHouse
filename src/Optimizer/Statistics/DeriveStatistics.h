#pragma once

#include <Interpreters/Cluster.h>
#include <Optimizer/PlanStepVisitor.h>
#include <Optimizer/Statistics/Stats.h>
#include <Optimizer/Statistics/StatsSettings.h>

namespace DB
{

class DeriveStatistics : public PlanStepVisitor<Stats>
{
public:
    using Base = PlanStepVisitor<Stats>;

    explicit DeriveStatistics(const StatsList & input_statistics_, ContextPtr context_)
        : input_statistics(input_statistics_)
        , context(context_)
        , stats_settings(StatsSettings::fromContext(context_))
        , log(&Poco::Logger::get("DeriveStatistics"))
    {
        auto query_coordination_info = context->getQueryCoordinationMetaInfo();
        auto cluster = context->getCluster(query_coordination_info.cluster_name);
        node_count = cluster->getShardCount();
    }

    Stats visit(QueryPlanStepPtr step) override;

    Stats visitDefault(IQueryPlanStep & step) override;

    Stats visit(ReadFromMergeTree & step) override;

    Stats visit(ExpressionStep & step) override;

    Stats visit(FilterStep & step) override;

    Stats visit(AggregatingStep & step) override;

    Stats visit(MergingAggregatedStep & step) override;

    Stats visit(CreatingSetsStep & step) override;

    Stats visit(SortingStep & step) override;

    Stats visit(LimitStep & step) override;

    Stats visit(JoinStep & step) override;

    Stats visit(UnionStep & step) override;

    Stats visit(ExchangeDataStep & step) override;

    Stats visit(TopNStep & step) override;

private:
    /// Inputs for step
    const StatsList & input_statistics;

    /// Query context
    ContextPtr context;

    /// Node count which participating the query.
    size_t node_count;

    /// Settings for statistics
    StatsSettings stats_settings;

    Poco::Logger * log;
};

}
