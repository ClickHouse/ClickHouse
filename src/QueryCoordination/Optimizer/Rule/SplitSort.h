#pragma once

#include <QueryCoordination/Optimizer/Rule/Rule.h>

namespace DB
{

class SplitSort final : public Rule
{
public:
    SplitSort();

    std::vector<StepTree> transform(StepTree & step_tree, ContextPtr context) override;
};

}
