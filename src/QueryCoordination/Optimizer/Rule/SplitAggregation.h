#pragma once

#include <QueryCoordination/Optimizer/Rule/Rule.h>

namespace DB
{

class SplitAggregation final : public Rule
{
public:
    SplitAggregation();

    std::vector<StepTree> transform(StepTree & step_tree, ContextPtr context) override;
};

}
