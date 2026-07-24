#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>

#include <map>

namespace DB
{

class QueryPlanStepRegistry
{
public:
    using StepCreateFunction = std::function<QueryPlanStepPtr(IQueryPlanStep::Deserialization &)>;

    /// How a step's payload serialization may evolve on the wire.
    /// A step starts at payload format version 1. Appending fields an old reader may safely
    /// ignore bumps only the step format version. A change an old reader must understand to
    /// execute correctly maps that step format version to a minimum plan version here, and the
    /// writer must not emit it for older streams.
    struct StepSerializationInfo
    {
        UInt64 max_step_format_version = 1;
        /// step format version -> minimum plan version that may carry it.
        /// Versions not listed are ignorable-extended and safe to prefix-read.
        std::map<UInt64, UInt64> min_plan_version_for_step_version;
        /// The plan version this step name first appeared in. A step name unknown to a reader is
        /// inherently must-understand, so plans containing the step need at least this version.
        /// 0 means "as old as serialization itself" (folded to the base version).
        UInt64 introduced_in_plan_version = 0;
    };

    QueryPlanStepRegistry() = default;
    QueryPlanStepRegistry(const QueryPlanStepRegistry &) = delete;
    QueryPlanStepRegistry & operator=(const QueryPlanStepRegistry &) = delete;

    static QueryPlanStepRegistry & instance();

    static void registerPlanSteps();

    void registerStep(const std::string & name, StepCreateFunction && create_function);
    void registerStep(const std::string & name, StepCreateFunction && create_function, StepSerializationInfo info);

    QueryPlanStepPtr createStep(
        const std::string & name,
        IQueryPlanStep::Deserialization & ctx) const;

    bool hasStep(const std::string & name) const;

    /// nullptr if the step name is not registered.
    const StepSerializationInfo * getStepSerializationInfo(const std::string & name) const;

    /// All registered serialization names (for test harnesses; do not hardcode counts).
    std::vector<std::string> getAllStepNames() const;

private:
    struct Entry
    {
        StepCreateFunction create_function;
        StepSerializationInfo info;
    };

    std::unordered_map<std::string, Entry> steps;
};

}
