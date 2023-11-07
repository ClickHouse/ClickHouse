#pragma once

#include <QueryCoordination/Optimizer/Rule/Rule.h>

namespace DB
{

class SplitSort final : public Rule
{
public:
    SplitSort();

    std::vector<SubQueryPlan> transform(SubQueryPlan & sub_plan, ContextPtr context) override;
};

}
