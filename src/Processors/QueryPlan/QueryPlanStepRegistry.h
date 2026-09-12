#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>

namespace DB
{

class QueryPlanStepRegistry
{
public:
    using StepCreateFunction = std::function<QueryPlanStepPtr(IQueryPlanStep::Deserialization &)>;

    /// The serialization versions of one step.
    ///
    /// A step owns its version. The author bumps it on any change to the step's bytes, independent of
    /// other steps and of the global plan version. The version travels on the wire, so a reader can
    /// refuse a version it does not know instead of misparsing the bytes.
    ///
    /// `readable` lists every step version this binary can deserialize. `write` maps a stable global
    /// plan version to the step version to write at it: the writer takes the entry with the highest
    /// plan version not above the version it serializes with. A release keeps, per step, the previous
    /// stable versions plus at most one new version. A step that has never changed its bytes stays at
    /// version 0 and needs none of this.
    struct Versions
    {
        std::vector<UInt64> readable = {0};
        /// (stable global plan version, step version), sorted by the plan version ascending.
        std::vector<std::pair<UInt64, UInt64>> write = {{0, 0}};
    };

    QueryPlanStepRegistry() = default;
    QueryPlanStepRegistry(const QueryPlanStepRegistry &) = delete;
    QueryPlanStepRegistry & operator=(const QueryPlanStepRegistry &) = delete;

    static QueryPlanStepRegistry & instance();

    static void registerPlanSteps();

    /// Registers a step whose bytes have never changed: it stays at version 0.
    void registerStep(const std::string & name, StepCreateFunction && create_function);
    /// Registers a step with an explicit set of serialization versions.
    void registerStep(const std::string & name, StepCreateFunction && create_function, Versions versions);

    QueryPlanStepPtr createStep(
        const std::string & name,
        IQueryPlanStep::Deserialization & ctx) const;

    /// The step version to write for a stream serialized at the stable global `plan_version`.
    UInt64 writeStepVersion(const std::string & name, UInt64 plan_version) const;

    /// Throws unless this binary can read `step_version` of the step. Called before `createStep`, so a
    /// step version the reader does not know is refused up front and never misparsed.
    void checkStepVersionReadable(const std::string & name, UInt64 step_version) const;

private:
    struct Entry
    {
        StepCreateFunction create_function;
        Versions versions;
    };

    const Entry & getEntry(const std::string & name) const;

    std::unordered_map<std::string, Entry> steps;

};

}
