#include <Common/Exception.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_IDENTIFIER;
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_DATA;
}

QueryPlanStepRegistry & QueryPlanStepRegistry::instance()
{
    static QueryPlanStepRegistry registry;
    return registry;
}

void QueryPlanStepRegistry::registerStep(const std::string & name, StepCreateFunction && create_function)
{
    registerStep(name, std::move(create_function), Versions{});
}

void QueryPlanStepRegistry::registerStep(const std::string & name, StepCreateFunction && create_function, Versions versions)
{
    if (steps.contains(name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query plan step '{}' is already registered", name);

    /// The writer scans `write` for the highest plan version not above the one it serializes with, so
    /// keep the entries ordered by plan version regardless of how they were declared.
    std::sort(versions.write.begin(), versions.write.end());

    steps[name] = Entry{std::move(create_function), std::move(versions)};
}

const QueryPlanStepRegistry::Entry & QueryPlanStepRegistry::getEntry(const std::string & name) const
{
    auto it = steps.find(name);
    if (it == steps.end())
        throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown query plan step: {}", name);
    return it->second;
}

QueryPlanStepPtr QueryPlanStepRegistry::createStep(
    const std::string & name,
    IQueryPlanStep::Deserialization & ctx) const
{
    return getEntry(name).create_function(ctx);
}

UInt64 QueryPlanStepRegistry::writeStepVersion(const std::string & name, UInt64 plan_version) const
{
    const Versions & versions = getEntry(name).versions;

    UInt64 step_version = 0;
    bool found = false;
    for (const auto & [at_plan_version, at_step_version] : versions.write)
    {
        if (at_plan_version > plan_version)
            break;
        step_version = at_step_version;
        found = true;
    }

    if (!found)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Query plan step '{}' has no serialization version for plan version {}", name, plan_version);

    return step_version;
}

void QueryPlanStepRegistry::checkStepVersionReadable(const std::string & name, UInt64 step_version) const
{
    const Versions & versions = getEntry(name).versions;
    if (std::find(versions.readable.begin(), versions.readable.end(), step_version) == versions.readable.end())
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Query plan step '{}' cannot be read at serialization version {}; this server does not know it",
            name, step_version);
}

void registerExpressionStep(QueryPlanStepRegistry & registry);
void registerUnionStep(QueryPlanStepRegistry & registry);
void registerIntersectOrExceptStep(QueryPlanStepRegistry & registry);
void registerDistinctStep(QueryPlanStepRegistry & registry);
void registerSortingStep(QueryPlanStepRegistry & registry);
void registerAggregatingStep(QueryPlanStepRegistry & registry);
void registerMergingAggregatedStep(QueryPlanStepRegistry & registry);
void registerRollupStep(QueryPlanStepRegistry & registry);
void registerCubeStep(QueryPlanStepRegistry & registry);
void registerWindowStep(QueryPlanStepRegistry & registry);
void registerArrayJoinStep(QueryPlanStepRegistry & registry);
void registerLimitByStep(QueryPlanStepRegistry & registry);
void registerLimitStep(QueryPlanStepRegistry & registry);
void registerLimitRangeStep(QueryPlanStepRegistry & registry);
void registerFractionalLimitStep(QueryPlanStepRegistry & registry);
void registerOffsetStep(QueryPlanStepRegistry & registry);
void registerFractionalOffsetStep(QueryPlanStepRegistry & registry);
void registerNegativeLimitStep(QueryPlanStepRegistry & registry);
void registerNegativeLimitByStep(QueryPlanStepRegistry & registry);
void registerNegativeOffsetStep(QueryPlanStepRegistry & registry);
void registerFilterStep(QueryPlanStepRegistry & registry);
void registerTotalsHavingStep(QueryPlanStepRegistry & registry);
void registerExtremesStep(QueryPlanStepRegistry & registry);
void registerJoinStep(QueryPlanStepRegistry & registry);
void registerShuffleSendStep(QueryPlanStepRegistry & registry);
void registerShuffleReceiveStep(QueryPlanStepRegistry & registry);
void registerGatherSendStep(QueryPlanStepRegistry & registry);
void registerGatherReceiveStep(QueryPlanStepRegistry & registry);
void registerBroadcastSendStep(QueryPlanStepRegistry & registry);
void registerBroadcastReceiveStep(QueryPlanStepRegistry & registry);
void registerReadFromMergeTreeStep(QueryPlanStepRegistry & registry);

void registerReadNothingStep(QueryPlanStepRegistry & registry);
void registerReadFromTableStep(QueryPlanStepRegistry & registry);
void registerReadFromTableFunctionStep(QueryPlanStepRegistry & registry);
void registerBuildRuntimeFilterStep(QueryPlanStepRegistry & registry);
void registerObjectFilterStep(QueryPlanStepRegistry & registry);


void registerReadFromStorageStep(QueryPlanStepRegistry & registry);


void QueryPlanStepRegistry::registerPlanSteps()
{
    QueryPlanStepRegistry & registry = QueryPlanStepRegistry::instance();

    registerExpressionStep(registry);
    registerUnionStep(registry);
    registerIntersectOrExceptStep(registry);
    registerDistinctStep(registry);
    registerSortingStep(registry);
    registerAggregatingStep(registry);
    registerMergingAggregatedStep(registry);
    registerRollupStep(registry);
    registerCubeStep(registry);
    registerWindowStep(registry);
    registerArrayJoinStep(registry);
    registerLimitByStep(registry);
    registerLimitStep(registry);
    registerLimitRangeStep(registry);
    registerFractionalLimitStep(registry);
    registerFractionalOffsetStep(registry);
    registerNegativeLimitStep(registry);
    registerNegativeLimitByStep(registry);
    registerOffsetStep(registry);
    registerNegativeOffsetStep(registry);
    registerFilterStep(registry);
    registerTotalsHavingStep(registry);
    registerExtremesStep(registry);
    registerJoinStep(registry);

    registerShuffleSendStep(registry);
    registerShuffleReceiveStep(registry);
    registerGatherSendStep(registry);
    registerGatherReceiveStep(registry);
    registerBroadcastSendStep(registry);
    registerBroadcastReceiveStep(registry);
    registerReadFromMergeTreeStep(registry);

    registerReadNothingStep(registry);
    registerReadFromTableStep(registry);
    registerReadFromTableFunctionStep(registry);
    registerBuildRuntimeFilterStep(registry);
    registerObjectFilterStep(registry);


    registerReadFromStorageStep(registry);
}

}
