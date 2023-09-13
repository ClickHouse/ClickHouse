#pragma once

#include <Interpreters/ActionsDAG.h>
#include <QueryCoordination/Optimizer/Statistics/Statistics.h>

namespace DB
{

class PredicateStatsCalculator
{
public:
    static Statistics calculateStatistics(const ActionsDAGPtr & predicates, const Statistics & input);
};

}
