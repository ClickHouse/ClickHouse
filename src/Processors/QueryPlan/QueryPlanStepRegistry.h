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
    /// A step owns its version. The author bumps it when the bytes the step's own `serialize` and
    /// `deserialize` write and read change. The version travels on the wire, so a reader refuses a
    /// version it does not know instead of misparsing the bytes.
    ///
    /// The contract this relies on:
    ///  - Several steps may change in one release cycle, each bumping its own version. One bump of
    ///    the global plan version per release covers all of them.
    ///  - On master a step may change several times between releases; each change bumps its version.
    ///    When a release branch is cut, each step keeps the previous release's version plus the one
    ///    new version, and the global plan version moves once.
    ///  - Within a released line step versions are frozen, so two servers that advertise the same
    ///    global version agree on every step. Only critical bugfixes are backported. When one has to
    ///    change a step's bytes, it bumps the step version without a global bump; during that fix's
    ///    rolling upgrade a peer that does not know the new version refuses it, and the affected
    ///    queries fail with an error until the rollout completes. That is accepted: a fix critical
    ///    enough to backport matters more than mixed-version compatibility while it rolls out. The
    ///    on-wire version only makes sure the failure is a clean error, not misparsed bytes.
    ///  - `write` is keyed by the global plan version: the writer serializes at the version the peers
    ///    agreed on (the lowest both support, or one pinned in config) and picks the step version
    ///    stored for it, so an older-release peer gets the encoding it knows.
    ///  - This covers the step's own payload only. The step settings channel (`serializeSettings` and
    ///    the changed-settings blob) is versioned by the global plan version, so a change to a step's
    ///    settings schema also needs a global plan-version bump.
    ///
    /// `readable` lists every step version this binary can deserialize. `write` maps a global plan
    /// version to the step version to write at it: the writer takes the entry with the highest plan
    /// version not above the one it serializes with. A step that has never changed its bytes stays at
    /// version 0 and needs none of this.
    struct Versions
    {
        std::vector<UInt64> readable = {0};
        /// (global plan version, step version), sorted by the plan version ascending.
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
