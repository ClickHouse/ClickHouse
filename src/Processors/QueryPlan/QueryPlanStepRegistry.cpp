#include <Common/Exception.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>

#include <map>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_IDENTIFIER;
    extern const int LOGICAL_ERROR;
}

namespace
{

thread_local QueryPlanStepRegistry * registry_for_this_thread = nullptr;

}

QueryPlanStepRegistry & QueryPlanStepRegistry::instance()
{
    if (registry_for_this_thread)
        return *registry_for_this_thread;
    static QueryPlanStepRegistry registry;
    return registry;
}

QueryPlanStepRegistry::ScopedInstance::ScopedInstance(QueryPlanStepRegistry & registry_)
    : previous(registry_for_this_thread)
{
    registry_for_this_thread = &registry_;
}

QueryPlanStepRegistry::ScopedInstance::~ScopedInstance()
{
    registry_for_this_thread = previous;
}

void QueryPlanStepRegistry::registerStep(const std::string & name, StepCreateFunction && create_function)
{
    registerStep(name, std::move(create_function), StepSerializationInfo{});
}

void QueryPlanStepRegistry::registerStep(const std::string & name, StepCreateFunction && create_function, StepSerializationInfo info)
{
    registerStep(name, std::move(create_function), std::move(info), String{});
}

String QueryPlanStepRegistry::dumpManifests() const
{
    std::map<std::string, const String *> by_name;
    for (const auto & [name, entry] : steps)
        if (!entry.manifest_description.empty())
            by_name.emplace(name, &entry.manifest_description);

    String result;
    for (const auto & [name, description] : by_name)
        result += *description;
    return result;
}

void QueryPlanStepRegistry::registerStep(
    const std::string & name, StepCreateFunction && create_function, StepSerializationInfo info, String manifest_description)
{
    if (steps.contains(name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query plan step '{}' is already registered", name);

    steps[name] = Entry{std::move(create_function), std::move(info), std::move(manifest_description)};
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
        create_function = it->second.create_function;
    }
    return create_function(ctx);
}

bool QueryPlanStepRegistry::hasStep(const std::string & name) const
{
    return steps.contains(name);
}

const QueryPlanStepRegistry::StepSerializationInfo * QueryPlanStepRegistry::getStepSerializationInfo(const std::string & name) const
{
    auto it = steps.find(name);
    if (it == steps.end())
        return nullptr;
    return &it->second.info;
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
