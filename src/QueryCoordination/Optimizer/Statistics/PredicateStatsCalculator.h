#pragma once

#include <Interpreters/ActionsDAG.h>
#include <QueryCoordination/Optimizer/PlanStepVisitor.h>
#include <QueryCoordination/Optimizer/Statistics/Statistics.h>

namespace DB
{

class PredicateStatsVisitor : public PlanStepVisitor<Statistics>
{

};

class PredicateStatsCalculator
{
public:
    static Statistics calculateStatistics(const ActionsDAGPtr & predicates, const StatisticsList & inputs);
};


}
