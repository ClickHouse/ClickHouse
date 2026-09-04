#pragma once

#include <Core/ProtocolDefines.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>

#include <map>

namespace DB
{

class QueryPlanStepRegistry
{
public:
    using StepCreateFunction = std::function<QueryPlanStepPtr(IQueryPlanStep::Deserialization &)>;

    /// How one payload format version differs from the one before it. With an `Append` a reader
    /// that knows an older format reads the fields it knows from the front and lets the frame skip
    /// the rest; doing the same with a `Restructure` would hand it garbage.
    enum class PayloadChange
    {
        Append,
        Restructure,
    };

    /// One payload format version of a step. A step starts at format version 1 and every change to
    /// its payload adds the next version here, so a change can never reach the wire unclassified.
    struct PayloadFormat
    {
        /// Defaulted to the strict one: a format whose entry says nothing must not be read by
        /// older readers.
        PayloadChange change = PayloadChange::Restructure;
        /// The oldest plan version that may carry this format. 0 means any: a reader that skips
        /// what it does not know needs nothing from this format.
        UInt64 min_plan_version = 0;
    };

    struct StepSerializationInfo
    {
        /// Payload format versions from 2 up, contiguous. Empty for a step whose payload has never
        /// changed.
        std::map<UInt64, PayloadFormat> payload_formats;

        /// The plan version this step name first appeared in. A reader that does not know the name
        /// cannot run the plan at all, so plans using the step need at least this version.
        /// 0 means the step is as old as plan serialization itself.
        UInt64 introduced_in_plan_version = 0;

        /// Set when a manifest declares the payload and the framework writes it: the step's content
        /// is then available as a plain wire struct, which a converter to another plan format can
        /// read without knowing the step class.
        bool has_wire_struct = false;

        /// The newest payload format this server writes and knows in full.
        UInt64 maxFormatVersion() const { return payload_formats.empty() ? 1 : payload_formats.rbegin()->first; }

        /// The oldest payload format that can still read the front of a `format_version` payload:
        /// everything after the last restructure only added fields, so a reader that knows that
        /// much reads what it understands and the frame skips the rest.
        UInt64 prefixReadableFrom(UInt64 format_version) const
        {
            UInt64 base = 1;
            for (const auto & [version, format] : payload_formats)
            {
                if (version > format_version)
                    break;
                if (format.change == PayloadChange::Restructure)
                    base = version;
            }
            return base;
        }

        /// The oldest plan version able to read a payload of `format_version`.
        UInt64 minPlanVersionForFormat(UInt64 format_version) const
        {
            UInt64 required = introduced_in_plan_version;
            for (const auto & [version, format] : payload_formats)
            {
                if (version > format_version)
                    break;
                required = std::max(required, format.min_plan_version);
            }
            return required;
        }
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
