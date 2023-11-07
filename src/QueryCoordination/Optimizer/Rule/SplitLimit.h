#pragma once

#include <QueryCoordination/Optimizer/Rule/Rule.h>

namespace DB
{

class SplitLimit final : public Rule
{
public:
    SplitLimit();

    std::vector<SubQueryPlan> transform(SubQueryPlan & sub_plan, ContextPtr context) override;
};

}
