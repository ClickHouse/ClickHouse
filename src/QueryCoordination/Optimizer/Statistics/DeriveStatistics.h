#pragma once

#include <Interpreters/Cluster.h>
#include <QueryCoordination/Optimizer/PlanStepVisitor.h>
#include <QueryCoordination/Optimizer/Statistics/Statistics.h>
#include <QueryCoordination/Optimizer/Statistics/StatisticsSettings.h>

namespace DB
{

class DeriveStatistics : public PlanStepVisitor<Statistics>
{
public:
    using Base = PlanStepVisitor<Statistics>;

    explicit DeriveStatistics(const StatisticsList & input_statistics_, ContextPtr context_)
        : input_statistics(input_statistics_)
        , context(context_)
        , stats_settings(StatisticsSettings::fromContext(context_))
        , log(&Poco::Logger::get("DeriveStatistics"))
    {
        auto query_coordination_info = context->getQueryCoordinationMetaInfo();
        auto cluster = context->getCluster(query_coordination_info.cluster_name);
        node_count = cluster->getShardCount();
    }

    Statistics visit(QueryPlanStepPtr step) override;

    Statistics visitDefault(IQueryPlanStep & step) override;

    Statistics visit(ReadFromMergeTree & step) override;

    Statistics visit(ExpressionStep & step) override;

    Statistics visit(FilterStep & step) override;

    Statistics visit(AggregatingStep & step) override;

    Statistics visit(MergingAggregatedStep & step) override;

    Statistics visit(CreatingSetsStep & step) override;

    Statistics visit(SortingStep & step) override;

    Statistics visit(LimitStep & step) override;

    Statistics visit(JoinStep & step) override;

    Statistics visit(UnionStep & step) override;

    Statistics visit(ExchangeDataStep & step) override;

    Statistics visit(TopNStep & step) override;

private:
    /// Inputs for step
    const StatisticsList & input_statistics;

    /// Query context
    ContextPtr context;

    /// Node count which participating the query.
    size_t node_count;

    /// Settings for statistics
    StatisticsSettings stats_settings;

    Poco::Logger * log;
};

}
