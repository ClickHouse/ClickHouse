#pragma once

#include <QueryCoordination/Optimizer/Rule/Rule.h>

namespace DB
{

class SplitTopN final : public Rule
{
public:
    SplitTopN();

    std::vector<SubQueryPlan> transform(SubQueryPlan & sub_plan, ContextPtr context) override;
};

}
