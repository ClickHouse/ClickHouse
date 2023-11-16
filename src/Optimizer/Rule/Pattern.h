#pragma once

#include <vector>
#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class Pattern
{
public:
    Pattern() = default;
    Pattern(StepType step_type_);

    Pattern & addChildren(const std::vector<Pattern> & children_);

    StepType getStepType() const;

    void setStepType(StepType step_type_);

    const std::vector<Pattern> & getChildren() const;

    static Pattern create(StepType step_type_) { return Pattern(step_type_); }

private:
    StepType step_type;
    std::vector<Pattern> children;
};

}
