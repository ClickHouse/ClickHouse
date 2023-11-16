#pragma once

#include <QueryCoordination/Optimizer/Rule/Pattern.h>
#include <QueryCoordination/Optimizer/SubQueryPlan.h>

namespace DB
{

class Rule
{
public:
    Rule(size_t id_) : id(id_) { }
    virtual ~Rule() = default;

    virtual std::vector<SubQueryPlan> transform(SubQueryPlan & sub_plan, ContextPtr context) = 0;

    size_t getRuleId() const;

    const Pattern & getPattern() const;

protected:
    size_t id;
    Pattern pattern;
};

using RulePtr = std::shared_ptr<Rule>;

}
