#pragma once

#include <Core/ProtocolDefines.h>
#include <limits>
#include <Processors/QueryPlan/IQueryPlanStep.h>


namespace DB
{

class QueryPlanStepRegistry
{
public:
    using StepCreateFunction = std::function<QueryPlanStepPtr(IQueryPlanStep::Deserialization &)>;

    struct StepSerializationInfo
    {
        /// The plan version this step name first appeared in. A reader that does not know the name
        /// cannot run the plan at all, so plans using the step need at least this version.
        /// 0 means the step is as old as plan serialization itself.
        UInt64 introduced_in_plan_version = 0;

        /// The payload format of this step name. Each name owns exactly one immutable layout, so this
        /// is always 1; a step that changes its wire content takes a new name instead of a higher
        /// number here. Kept as a reserved field.
        UInt64 max_format_version = 1;

        /// Set when a manifest declares the payload and the framework writes it: the step's content
        /// is then available as a plain wire struct, which a converter to another plan format can
        /// read without knowing the step class.
        bool has_wire_struct = false;

        /// The number of input streams the step reads, checked against a node's child count before the
        /// step is built. `SIZE_MAX` means the count is not fixed (a variable-input step, or a step
        /// registered without a manifest), so no check is applied.
        size_t input_count = std::numeric_limits<size_t>::max();
    };

    QueryPlanStepRegistry() = default;
    QueryPlanStepRegistry(const QueryPlanStepRegistry &) = delete;
    QueryPlanStepRegistry & operator=(const QueryPlanStepRegistry &) = delete;

    static QueryPlanStepRegistry & instance();

    /// Tests act as another server in the same process, an older or a newer build that knows other
    /// steps and formats and speaks another plan version. While an object of this class lives,
    /// `instance()` on this thread returns the registry it was given.
    class ScopedInstance
    {
    public:
        explicit ScopedInstance(QueryPlanStepRegistry & registry_);
        ~ScopedInstance();
        ScopedInstance(const ScopedInstance &) = delete;
        ScopedInstance & operator=(const ScopedInstance &) = delete;

    private:
        QueryPlanStepRegistry * previous;
    };

    /// The newest plan version this build reads and writes: `DBMS_QUERY_PLAN_SERIALIZATION_VERSION`
    /// for the server, whatever a test registry that acts as another build sets.
    UInt64 supportedVersion() const { return supported_version; }
    void setSupportedVersion(UInt64 version) { supported_version = version; }

    static void registerPlanSteps();

    /// A step without a manifest: every production step has one, these serve the test steps that
    /// exercise the frame with hand-written payloads.
    void registerStep(const std::string & name, StepCreateFunction && create_function);
    void registerStep(const std::string & name, StepCreateFunction && create_function, StepSerializationInfo info);
    /// A step declared by a manifest also leaves the description of its declaration, for the baseline test.
    void registerStep(const std::string & name, StepCreateFunction && create_function, StepSerializationInfo info, String manifest_description);

    /// The declarations of every step registered through a manifest, in their canonical text form,
    /// sorted by name.
    String dumpManifests() const;

    QueryPlanStepPtr createStep(
        const std::string & name,
        IQueryPlanStep::Deserialization & ctx) const;

    bool hasStep(const std::string & name) const;

    /// nullptr if the step name is not registered.
    const StepSerializationInfo * getStepSerializationInfo(const std::string & name) const;

private:
    struct Entry
    {
        StepCreateFunction create_function;
        StepSerializationInfo info;
        String manifest_description;
    };

    std::unordered_map<std::string, Entry> steps;
    UInt64 supported_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
};

}
