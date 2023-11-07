#pragma once

#include <QueryCoordination/Optimizer/Rule/Pattern.h>
#include <QueryCoordination/Optimizer/StepTree.h>

namespace DB
{

class Rule
{
public:
    Rule() = default;
    virtual ~Rule() = default;

    virtual std::vector<StepTree> transform(StepTree & step_tree, ContextPtr context) = 0;

    const Pattern & getPattern() const;

protected:
    Pattern pattern;
};

using RulePtr = std::shared_ptr<Rule>;

}
