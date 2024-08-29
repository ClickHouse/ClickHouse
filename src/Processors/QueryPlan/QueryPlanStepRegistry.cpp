#include <Processors/QueryPlan/QueryPlanStepRegistry.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_IDENTIFIER;
    extern const int LOGICAL_ERROR;
}

QueryPlanStepRegistry & QueryPlanStepRegistry::instance()
{
    static QueryPlanStepRegistry registry;
    return registry;
}

void QueryPlanStepRegistry::registerStep(const std::string & name, StepCreateFunction && create_function)
{
    if (steps.contains(name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query plan step '{}' is already registered", name);
    steps[name] = std::move(create_function);
}

QueryPlanStepPtr QueryPlanStepRegistry::createStep(
    ReadBuffer & buf,
    const std::string & name,
    const DataStreams & input_streams,
    const DataStream * output_stream,
    QueryPlanSerializationSettings & settings) const
{
    StepCreateFunction create_function;
    {
        auto it = steps.find(name);
        if (it == steps.end())
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown query plan step: {}", name);
        create_function = it->second;
    }
    return create_function(buf, input_streams, output_stream, settings);
}

void registerExpressionStep(QueryPlanStepRegistry & registry);
void registerUnionStep(QueryPlanStepRegistry & registry);
void registerDistinctStep(QueryPlanStepRegistry & registry);

void registerPlanSteps()
{
    QueryPlanStepRegistry & registry = QueryPlanStepRegistry::instance();

    registerExpressionStep(registry);
    registerUnionStep(registry);
    registerDistinctStep(registry);
}

}
