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
    const std::string & name,
    IQueryPlanStep::Deserialization & ctx) const
{
    StepCreateFunction create_function;
    {
        auto it = steps.find(name);
        if (it == steps.end())
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown query plan step: {}", name);
        create_function = it->second;
    }
    return create_function(ctx);
}

void registerExpressionStep(QueryPlanStepRegistry & registry);
void registerUnionStep(QueryPlanStepRegistry & registry);
void registerDistinctStep(QueryPlanStepRegistry & registry);
void registerSortingStep(QueryPlanStepRegistry & registry);
void registerAggregatingStep(QueryPlanStepRegistry & registry);
void registerArrayJoinStep(QueryPlanStepRegistry & registry);
void registerLimitByStep(QueryPlanStepRegistry & registry);
void registerLimitStep(QueryPlanStepRegistry & registry);
void registerOffsetStep(QueryPlanStepRegistry & registry);
void registerFilterStep(QueryPlanStepRegistry & registry);
void registerTotalsHavingStep(QueryPlanStepRegistry & registry);
void registerExtremesStep(QueryPlanStepRegistry & registry);

void QueryPlanStepRegistry::registerPlanSteps()
{
    QueryPlanStepRegistry & registry = QueryPlanStepRegistry::instance();

    registerExpressionStep(registry);
    registerUnionStep(registry);
    registerDistinctStep(registry);
    registerSortingStep(registry);
    registerAggregatingStep(registry);
    registerArrayJoinStep(registry);
    registerLimitByStep(registry);
    registerLimitStep(registry);
    registerOffsetStep(registry);
    registerFilterStep(registry);
    registerTotalsHavingStep(registry);
    registerExtremesStep(registry);
}

}
