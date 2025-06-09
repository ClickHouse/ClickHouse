#pragma once

#include <Common/CurrentThread.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>

namespace DB
{

class QueryPipelineBuilder;
using QueryPipelineBuilderPtr = std::unique_ptr<QueryPipelineBuilder>;
using QueryPipelineBuilders = std::vector<QueryPipelineBuilderPtr>;

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;
using Processors = std::vector<ProcessorPtr>;

namespace JSONBuilder { class JSONMap; }

class QueryPlan;
using QueryPlanRawPtrs = std::list<QueryPlan *>;

struct QueryPlanSerializationSettings;

using Header = Block;
using Headers = std::vector<Header>;

struct ExplainPlanOptions;

class IQueryPlanStep;
using QueryPlanStepPtr = std::unique_ptr<IQueryPlanStep>;

/// Single step of query plan.
class IQueryPlanStep
{
public:
    IQueryPlanStep();

    IQueryPlanStep(const IQueryPlanStep &) = default;
    IQueryPlanStep(IQueryPlanStep &&) = default;

    virtual ~IQueryPlanStep() = default;

    virtual String getName() const = 0;
    virtual String getSerializationName() const { return getName(); }

    /// Add processors from current step to QueryPipeline.
    /// Calling this method, we assume and don't check that:
    ///   * pipelines.size() == getInputHeaders.size()
    ///   * header from each pipeline is the same as header from corresponding input
    /// Result pipeline must contain any number of ports with compatible output header if hasOutputHeader(),
    ///   or pipeline should be completed otherwise.
    virtual QueryPipelineBuilderPtr updatePipeline(QueryPipelineBuilders pipelines, const BuildQueryPipelineSettings & settings) = 0;

    const Headers & getInputHeaders() const { return input_headers; }

    bool hasOutputHeader() const { return output_header.has_value(); }
    const Header & getOutputHeader() const;

    /// Methods to describe what this step is needed for.
    const std::string & getStepDescription() const { return step_description; }
    void setStepDescription(std::string description) { step_description = std::move(description); }

    struct Serialization;
    struct Deserialization;

    virtual void serializeSettings(QueryPlanSerializationSettings & /*settings*/) const {}
    virtual void serialize(Serialization & /*ctx*/) const;
    virtual bool isSerializable() const { return false; }

    virtual QueryPlanStepPtr clone() const;

    virtual const SortDescription & getSortDescription() const;

    struct FormatSettings
    {
        WriteBuffer & out;
        size_t offset = 0;
        const size_t indent = 2;
        const char indent_char = ' ';
        const bool write_header = false;
    };

    /// Get detailed description of step actions. This is shown in EXPLAIN query with options `actions = 1`.
    virtual void describeActions(JSONBuilder::JSONMap & /*map*/) const {}
    virtual void describeActions(FormatSettings & /*settings*/) const {}

    /// Get detailed description of read-from-storage step indexes (if any). Shown in with options `indexes = 1`.
    virtual void describeIndexes(JSONBuilder::JSONMap & /*map*/) const {}
    virtual void describeIndexes(FormatSettings & /*settings*/) const {}

    /// Get detailed description of read-from-storage step projections (if any). Shown in with options `projections = 1`.
    virtual void describeProjections(JSONBuilder::JSONMap & /*map*/) const {}
    virtual void describeProjections(FormatSettings & /*settings*/) const {}

    /// Get description of the distributed plan. Shown in with options `distributed = 1
    virtual void describeDistributedPlan(FormatSettings & /*settings*/, const ExplainPlanOptions & /*options*/) {}

    /// Get description of processors added in current step. Should be called after updatePipeline().
    virtual void describePipeline(FormatSettings & /*settings*/) const {}

    /// Get child plans contained inside some steps (e.g ReadFromMerge) so that they are visible when doing EXPLAIN.
    virtual QueryPlanRawPtrs getChildPlans() { return {}; }

    /// Append extra processors for this step.
    void appendExtraProcessors(const Processors & extra_processors);

    /// Updates the input streams of the given step. Used during query plan optimizations.
    /// It won't do any validation of new streams, so it is your responsibility to ensure that this update doesn't break anything
    String getUniqID() const;

    /// (e.g. you correctly remove / add columns).
    void updateInputHeaders(Headers input_headers_);
    void updateInputHeader(Header input_header, size_t idx = 0);

    virtual bool hasCorrelatedExpressions() const;

protected:
    virtual void updateOutputHeader() = 0;

    Headers input_headers;
    std::optional<Header> output_header;

    /// Text description about what current step does.
    std::string step_description;

    /// This field is used to store added processors from this step.
    /// It is used only for introspection (EXPLAIN PIPELINE).
    Processors processors;

    static void describePipeline(const Processors & processors, FormatSettings & settings);

private:
    size_t step_index = 0;
};

}
