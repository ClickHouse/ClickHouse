#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class QueryPlanStepRegistry
{
public:
    using StepCreateFunction = std::function<QueryPlanStepPtr(IQueryPlanStep::Deserialization &)>;

    /// The step's own serialization version and the global plan version when it was introduced. The
    /// global version is bumped only once per release, while several steps may change their
    /// serialization within that release.
    struct StepVersion
    {
        UInt64 version = 0;
        UInt64 since_plan_version = 0;
    };

    /// The serialization versions a step supports, each with the global version when it was introduced.
    /// This lets the writer pick the step's serialization version from the chosen global version, or
    /// from the global version negotiated with the peer. A reader accepts only a listed version.
    /// Only the versions needed for compatibility with the few latest supported releases have to be
    /// kept; older ones can be dropped.
    /// Example: {{0, 0}, {1, 12}, {2, 15}} - a stream at global version 11 gets version 0, at 12 to 14
    /// gets version 1, at 15 or later gets version 2.
    using StepVersions = std::vector<StepVersion>;

    QueryPlanStepRegistry() = default;
    QueryPlanStepRegistry(const QueryPlanStepRegistry &) = delete;
    QueryPlanStepRegistry & operator=(const QueryPlanStepRegistry &) = delete;

    static QueryPlanStepRegistry & instance();

    static void registerPlanSteps();

    /// Registers a step whose bytes have never changed: it stays at version 0.
    void registerStep(const std::string & name, StepCreateFunction && create_function);
    /// Registers a step with the versions of its bytes.
    void registerStep(const std::string & name, StepCreateFunction && create_function, StepVersions versions);

    QueryPlanStepPtr createStep(
        const std::string & name,
        IQueryPlanStep::Deserialization & ctx) const;

    /// The version to write for a stream serialized at `plan_version`: the newest one the peer's
    /// release knows.
    UInt64 versionToWrite(const std::string & name, UInt64 plan_version) const;

    /// Throws unless `version` is one this binary can read. Called before `createStep`, so an unknown
    /// version is refused up front and never misparsed.
    void checkVersionReadable(const std::string & name, UInt64 version) const;

private:
    struct Entry
    {
        StepCreateFunction create_function;
        StepVersions versions;
    };

    const Entry & getEntry(const std::string & name) const;

    std::unordered_map<std::string, Entry> steps;

};

}
