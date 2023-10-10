#pragma once

#include <QueryCoordination/Optimizer/Rule/Rule.h>

namespace DB
{

class ConvertToTopN final : public Rule
{
public:
    ConvertToTopN();

    std::vector<StepTree> transform(StepTree & step_tree, ContextPtr context) override;
};

}
