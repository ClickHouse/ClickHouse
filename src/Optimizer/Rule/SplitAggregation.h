#pragma once

#include <Optimizer/Rule/Rule.h>

namespace DB
{

class SplitAggregation final : public Rule
{
public:
    SplitAggregation(size_t id_);

    std::vector<SubQueryPlan> transform(SubQueryPlan & sub_plan, ContextPtr context) override;
};

}
