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
    registerStep(name, std::move(create_function), StepVersions{{0, 0}});
}

void QueryPlanStepRegistry::registerStep(const std::string & name, StepCreateFunction && create_function, StepVersions versions)
{
    if (steps.contains(name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query plan step '{}' is already registered", name);
    if (versions.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query plan step '{}' must declare at least one serialization version", name);

    /// `versionToWrite` takes the last entry whose release the peer knows, so order the entries by the
    /// release that introduced them. Two versions introduced in the same release (a backported fix)
    /// resolve to the newer one.
    std::sort(versions.begin(), versions.end(), [](const StepVersion & lhs, const StepVersion & rhs)
    {
        if (lhs.since_plan_version != rhs.since_plan_version)
            return lhs.since_plan_version < rhs.since_plan_version;
        return lhs.version < rhs.version;
    });

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

UInt64 QueryPlanStepRegistry::versionToWrite(const std::string & name, UInt64 plan_version) const
{
    const StepVersions & versions = getEntry(name).versions;

    const StepVersion * chosen = nullptr;
    for (const auto & candidate : versions)
    {
        if (candidate.since_plan_version > plan_version)
            break;
        chosen = &candidate;
    }

    if (!chosen)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Query plan step '{}' has no serialization version for plan version {}", name, plan_version);

    return chosen->version;
}

void QueryPlanStepRegistry::checkVersionReadable(const std::string & name, UInt64 version) const
{
    const StepVersions & versions = getEntry(name).versions;
    const bool known = std::any_of(versions.begin(), versions.end(),
        [version](const StepVersion & candidate) { return candidate.version == version; });
    if (!known)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Query plan step '{}' cannot be read at serialization version {}; this server does not know it",
            name, version);
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
void registerFillingStep(QueryPlanStepRegistry & registry);
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
void registerReadFromSystemOneStep(QueryPlanStepRegistry & registry);
void registerReadFromSystemNumbersStep(QueryPlanStepRegistry & registry);


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
    registerFillingStep(registry);
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
    registerReadFromSystemOneStep(registry);
    registerReadFromSystemNumbersStep(registry);
    registerReadFromTableFunctionStep(registry);
    registerBuildRuntimeFilterStep(registry);
    registerObjectFilterStep(registry);


    registerReadFromStorageStep(registry);
}

}
