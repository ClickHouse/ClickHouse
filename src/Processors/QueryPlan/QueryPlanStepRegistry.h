#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class QueryPlanStepRegistry
{
public:
    using StepCreateFunction = std::function<QueryPlanStepPtr(IQueryPlanStep::Deserialization &)>;

    QueryPlanStepRegistry() = default;
    QueryPlanStepRegistry(const QueryPlanStepRegistry &) = delete;
    QueryPlanStepRegistry & operator=(const QueryPlanStepRegistry &) = delete;

    static QueryPlanStepRegistry & instance();

    static void registerPlanSteps();

    void registerStep(const std::string & name, StepCreateFunction && create_function);

    QueryPlanStepPtr createStep(
        const std::string & name,
        IQueryPlanStep::Deserialization & ctx) const;

private:
    std::unordered_map<std::string, StepCreateFunction> steps;

};

}
