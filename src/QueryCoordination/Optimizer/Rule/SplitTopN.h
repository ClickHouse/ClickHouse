#pragma once

#include <QueryCoordination/Optimizer/Rule/Rule.h>

namespace DB
{

class SplitTopN final : public Rule
{
public:
    SplitTopN();

    std::vector<StepTree> transform(StepTree & step_tree, ContextPtr context) override;
};

}
