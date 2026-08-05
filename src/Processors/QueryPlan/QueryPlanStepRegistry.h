#pragma once

#include <Processors/QueryPlan/IQueryPlanStep.h>

#include <map>

namespace DB
{

class QueryPlanStepRegistry
{
public:
    using StepCreateFunction = std::function<QueryPlanStepPtr(IQueryPlanStep::Deserialization &)>;

    /// How one payload format version differs from the one before it. A reader that knows an
    /// older format prefix-reads an `Append` and lets the frame skip the rest, which would give it
    /// garbage for a `Restructure`.
    enum class PayloadChange
    {
        Append,
        Restructure,
    };

    /// One payload format version of a step. A step starts at format version 1 and every change to
    /// its payload adds the next version here, so a change can never reach the wire unclassified.
    struct PayloadFormat
    {
        /// Defaulted to the strict one: a format whose entry says nothing must not let older
        /// readers prefix-read it.
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

        /// The plan version this step name first appeared in. A step name unknown to a reader is
        /// inherently must-understand, so plans containing the step need at least this version.
        /// 0 means "as old as serialization itself" (folded to the base version).
        UInt64 introduced_in_plan_version = 0;

        /// The newest payload format this server writes and knows in full.
        UInt64 maxFormatVersion() const { return payload_formats.empty() ? 1 : payload_formats.rbegin()->first; }

        /// The oldest payload format whose reader can prefix-read `format_version`: everything
        /// after the last restructure is an append onto it, so a reader that knows that much reads
        /// the part it understands and the frame skips the rest.
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

    static void registerPlanSteps();

    void registerStep(const std::string & name, StepCreateFunction && create_function);
    void registerStep(const std::string & name, StepCreateFunction && create_function, StepSerializationInfo info);

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
    };

    std::unordered_map<std::string, Entry> steps;
};

}
