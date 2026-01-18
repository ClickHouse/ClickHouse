#pragma once

#include <Common/CurrentThread.h>
#include <Core/Block_fwd.h>
#include <Core/SortDescription.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <variant>

namespace DB
{

class QueryPipelineBuilder;
using QueryPipelineBuilderPtr = std::unique_ptr<QueryPipelineBuilder>;
using QueryPipelineBuilders = std::vector<QueryPipelineBuilderPtr>;

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;
using Processors = std::vector<ProcessorPtr>;

class RuntimeDataflowStatisticsCacheUpdater;
using RuntimeDataflowStatisticsCacheUpdaterPtr = std::shared_ptr<RuntimeDataflowStatisticsCacheUpdater>;

namespace JSONBuilder { class JSONMap; }

class QueryPlan;
using QueryPlanRawPtrs = std::list<QueryPlan *>;

struct QueryPlanSerializationSettings;

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

    const SharedHeaders & getInputHeaders() const { return input_headers; }

    bool hasOutputHeader() const { return output_header != nullptr; }
    const SharedHeader & getOutputHeader() const;

    /// Methods to describe what this step is needed for.
    std::string_view getStepDescription() const;
    void setStepDescription(std::string description, size_t limit);
    void setStepDescription(const IQueryPlanStep & step);

    template <size_t size>
    ALWAYS_INLINE void setStepDescription(const char (&description)[size]) { step_description = std::string_view(description, size - 1); }

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
    void updateInputHeaders(SharedHeaders input_headers_);
    void updateInputHeader(SharedHeader input_header, size_t idx = 0);

    virtual bool hasCorrelatedExpressions() const;

    virtual bool supportsDataflowStatisticsCollection() const { return false; }

    void setRuntimeDataflowStatisticsCacheUpdater(RuntimeDataflowStatisticsCacheUpdaterPtr updater)
    {
        dataflow_cache_updater = std::move(updater);
    }

    /// Returns true if the step has implemented removeUnusedColumns.
    virtual bool canRemoveUnusedColumns() const { return false; }

    enum class RemovedUnusedColumns
    {
        None,
        OutputOnly,
        OutputAndInput
    };

    /// Removes the unnecessary inputs and outputs from the step based on required_outputs.
    /// required_outputs must be a maybe empty subset of the current outputs of the step.
    /// It is guaranteed that the output header of the step will contain all columns from
    /// required_outputs and might contain some other columns too.
    /// Can be used only if canRemoveUnusedColumns returns true.
    /// The order of the remaining outputs must be preserved.
    virtual RemovedUnusedColumns removeUnusedColumns(NameMultiSet /*required_outputs*/, bool /*remove_inputs*/);

    /// Returns true if the step can remove any columns from the output using removeUnusedColumns.
    virtual bool canRemoveColumnsFromOutput() const;

protected:
    virtual void updateOutputHeader() = 0;

    SharedHeaders input_headers;
    SharedHeader output_header;

    /// Text description about what current step does.
    std::variant<std::string, std::string_view> step_description;

    friend class DescriptionHolder;

    /// This field is used to store added processors from this step.
    /// It is used only for introspection (EXPLAIN PIPELINE).
    Processors processors;

    RuntimeDataflowStatisticsCacheUpdaterPtr dataflow_cache_updater;

    static void describePipeline(const Processors & processors, FormatSettings & settings);

private:
    size_t step_index = 0;
};

}
