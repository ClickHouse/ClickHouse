#pragma once

#include <QueryCoordination/Optimizer/Rule/Rule.h>

namespace DB
{

class SplitAggregation final : public Rule
{
public:
    SplitAggregation();

    std::vector<SubQueryPlan> transform(SubQueryPlan & sub_plan, ContextPtr context) override;
};

}
