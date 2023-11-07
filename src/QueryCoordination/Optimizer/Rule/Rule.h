#pragma once

#include <QueryCoordination/Optimizer/Rule/Pattern.h>
#include <QueryCoordination/Optimizer/SubQueryPlan.h>

namespace DB
{

class Rule
{
public:
    Rule() = default;
    virtual ~Rule() = default;

    virtual std::vector<SubQueryPlan> transform(SubQueryPlan & sub_plan, ContextPtr context) = 0;

    const Pattern & getPattern() const;

protected:
    Pattern pattern;
};

using RulePtr = std::shared_ptr<Rule>;

}
