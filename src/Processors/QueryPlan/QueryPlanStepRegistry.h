#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class QueryPlanStepRegistry
{
public:
    using StepCreateFunction = std::function<QueryPlanStepPtr(ReadBuffer &, const DataStreams &, QueryPlanSerializationSettings &)>;

    QueryPlanStepRegistry() = default;
    QueryPlanStepRegistry(const QueryPlanStepRegistry &) = delete;
    QueryPlanStepRegistry & operator=(const QueryPlanStepRegistry &) = delete;

    static QueryPlanStepRegistry & instance();

    static void registerPlanSteps();

    void registerStep(const std::string & name, StepCreateFunction && create_function);

    QueryPlanStepPtr createStep(
        ReadBuffer & buf,
        const std::string & name,
        const DataStreams & input_streams,
        QueryPlanSerializationSettings & settings) const;

private:
    std::unordered_map<std::string, StepCreateFunction> steps;

};

}
