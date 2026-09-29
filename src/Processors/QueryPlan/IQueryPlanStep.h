#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <Core/Block_fwd.h>
#include <Core/SortDescription.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/StepAnalyzeInfo.h>
#include <span>
#include <string_view>
#include <variant>
#include <list>

namespace DB
{

class QueryPipelineBuilder;
using QueryPipelineBuilderPtr = std::unique_ptr<QueryPipelineBuilder>;
using QueryPipelineBuilders = VectorWithMemoryTracking<QueryPipelineBuilderPtr>;

class IProcessor;
using ProcessorPtr = std::shared_ptr<IProcessor>;
using Processors = std::list<ProcessorPtr>;

class RuntimeDataflowStatisticsCacheUpdater;
using RuntimeDataflowStatisticsCacheUpdaterPtr = std::shared_ptr<RuntimeDataflowStatisticsCacheUpdater>;

namespace JSONBuilder { class JSONMap; }

class QueryPlan;
using QueryPlanRawPtrs = std::list<QueryPlan *>;

struct QueryPlanSerializationSettings;

struct ExplainPlanOptions;

class IQueryPlanStep;
using QueryPlanStepPtr = std::unique_ptr<IQueryPlanStep>;

struct ExplainFormatSettings;

using StepProcessors = std::span<IProcessor * const>;

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

    virtual void serializeSettings(QueryPlanSerializationSettings & /*settings*/, UInt64 /*version*/) const {}
    virtual void serialize(Serialization & /*ctx*/) const;
    virtual bool isSerializable() const { return false; }

    virtual QueryPlanStepPtr clone() const;

    virtual const SortDescription & getSortDescription() const;

    using FormatSettings = ExplainFormatSettings;

    /// Get detailed description of step actions. This is shown in EXPLAIN query with options `actions = 1`.
    virtual void describeActions(JSONBuilder::JSONMap & /*map*/) const {}
    virtual void describeActions(FormatSettings & /*settings*/) const {}

    /// Get detailed description of read-from-storage step indexes (if any). Shown in with options `indexes = 1`.
    virtual void describeIndexes(JSONBuilder::JSONMap & /*map*/) const {}
    virtual void describeIndexes(FormatSettings & /*settings*/) const {}

    /// Get detailed description of read-from-storage step projections (if any). Shown in with options `projections = 1`.
    virtual void describeProjections(JSONBuilder::JSONMap & /*map*/) const {}
    virtual void describeProjections(FormatSettings & /*settings*/) const {}

    /// Get description of the distributed plan. Shown with option `distributed = 1`.
    virtual void describeDistributedPlan(FormatSettings & /*settings*/, const ExplainPlanOptions & /*options*/) {}

    /// Get description of the distributed pipeline. Shown with option `distributed = 1` in EXPLAIN PIPELINE.
    virtual void describeDistributedPipeline(FormatSettings & /*settings*/, bool /*distributed*/) {}

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

    /// Returns true if this step's expressions contain correlated columns (`PLACEHOLDER` action nodes).
    /// Such plans cannot be executed standalone and require decorrelation first.
    /// The default returns false; every subclass that stores an `ActionsDAG` (or any
    /// other container of expression actions that may hold `PLACEHOLDER` nodes) MUST
    /// override this to check its expressions. Otherwise correlated subqueries may
    /// silently bypass the guards in `FutureSetFromSubquery::buildSetInplace` and
    /// `buildOrderedSetInplace`, and trigger `Trying to execute PLACEHOLDER action`.
    virtual bool hasCorrelatedExpressions() const;

    /// `considerEnablingParallelReplicas` gates on the whole plan: one step returning false rejects it
    /// and no statistics are collected. A step that returns true must also attach a
    /// `RuntimeDataflowStatisticsCollector` in `transformPipeline` when `dataflow_cache_updater` is set,
    /// otherwise, should it end up at the replica-output boundary, the cached `output_bytes` stays 0 and
    /// the transfer to the initiator is priced at zero.
    virtual bool supportsDataflowStatisticsCollection() const { return false; }

    void setRuntimeDataflowStatisticsCacheUpdater(RuntimeDataflowStatisticsCacheUpdaterPtr updater);

    /// Returns true if the step has implemented removeUnusedColumns.
    virtual bool canRemoveUnusedColumns() const { return false; }

    struct RemoveUnusedColumnsResult
    {
        /// Whether the step itself changed: its expressions, or the columns it outputs.
        bool step_changed = false;

        /// Whether some child has to produce fewer columns than it does now. Implies `step_changed`, but
        /// not the other way round: a step that may not remove its inputs can change all the same.
        bool inputs_changed = false;

        /// Per child, in the order of the children: the positions of the child's current output header
        /// the step still reads, sorted. Always one entry per child, and every position of a child whose
        /// columns are all still read. An empty entry means the step reads nothing of that child.
        std::vector<std::vector<size_t>> required_input_positions;

        /// The positions of the step's former output header that remain, in their order, which is also the
        /// order of the new output header. Always filled.
        std::vector<size_t> kept_output_positions;

        /// How many columns the step now outputs that it did not before, such as the dummy column a join
        /// adds when nothing else is left. They come after the kept ones.
        size_t added_output_count = 0;
    };

    /// The answer of removeUnusedColumns when nothing changes: every input read, every output kept.
    RemoveUnusedColumnsResult keepEverything() const;

    /// What one column of a step's input header is to the step once the unused columns are gone.
    enum class InputColumnUsage : uint8_t
    {
        ReadNeeded,           /// an input reads it, and what that input feeds is still needed
        ReadDropped,          /// an input reads it, and nothing needs that input any more
        PassesThroughNeeded,  /// no input reads it, and the caller asked for the column itself
        PassesThroughDropped, /// no input reads it, and nobody asked for it
    };

    /// Removes the unnecessary inputs and outputs from the step based on required_output_positions.
    /// required_output_positions must be a sorted vector of indices into the step's current output header.
    /// Each position uniquely identifies a column even when names are duplicated.
    /// It is guaranteed that the output header of the step will contain all columns at those positions
    /// and might contain some other columns too.
    /// Can be used only if canRemoveUnusedColumns returns true.
    /// The order of the remaining outputs must be preserved.
    virtual RemoveUnusedColumnsResult removeUnusedColumns(const std::vector<size_t> & /*required_output_positions*/, bool /*remove_inputs*/);

    /// Same answer as removeUnusedColumns, but leaves this step untouched, so a pass can try a
    /// candidate set of required columns and back out. Where both are implemented, removeUnusedColumns
    /// is this calculation plus its application. Requires canGetRequiredColumns.
    virtual RemoveUnusedColumnsResult getRequiredColumns(const std::vector<size_t> & /*required_output_positions*/, bool /*remove_inputs*/) const;

    /// Returns true if the step has implemented getRequiredColumns.
    virtual bool canGetRequiredColumns() const { return false; }

    /// Returns true if the step can remove any columns from the output using removeUnusedColumns.
    virtual bool canRemoveColumnsFromOutput() const;

    /// Different Steps have different stages of execution.
    /// For example JoinStep has build and probe stages.
    /// The group tag is used in EXPLAIN ANALYZE in order to track
    /// correctly the time that a step spent doing work in a stage.
    /// Each step knows its stages (see AggregatingStage,
    /// JoinStage, SortingStage etc.). When adding new steps with stages
    /// In order for EXPLAIN ANALYZE to track all the time for every step
    /// redefine the methods below when adding a new step with several stages
    /// Follow the pattern of classes with multi stage execution that already implements these methods
    virtual std::vector<size_t> getStepGroups() const { return {0}; }
    virtual String getStepGroupName(size_t) const { return {}; }

    virtual StepAnalysisReport getAnalysisReport(StepProcessors /*step_processors*/) const { return {}; }

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
