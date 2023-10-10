#pragma once

#include <QueryCoordination/Optimizer/Rule/Rule.h>

namespace DB
{

class SplitLimit final : public Rule
{
public:
    SplitLimit();

    std::vector<StepTree> transform(StepTree & step_tree, ContextPtr context) override;
};

}
