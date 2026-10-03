#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <Core/Block_fwd.h>
#include <Core/SortDescription.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Profiling/Metrics/StepAnalyzeInfo.h>
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
    /// EXPLAIN sets `for_explain`: a step whose plan runs with privileges the current user does not hold may hide it there.
    virtual QueryPlanRawPtrs getChildPlans(bool /*for_explain*/) { return {}; }

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

    /// Removing unused columns. Every list of positions below is a sorted list of positions in a header.

    /// Returns true if the step can take part in removing unused columns: it implements removeUnusedColumns,
    /// and getUnneededColumns unless it has no children to ask about.
    virtual bool canRemoveUnusedColumns() const { return false; }

    /// What removeUnusedColumns did. The default is the answer when nothing changes.
    struct RemoveUnusedColumnsResult
    {
        /// Whether the step itself changed: its expressions, its input headers, or the columns it outputs.
        bool step_changed = false;

        /// The positions of the step's former output header that went away.
        /// They are the subset of the given unneeded positions that the step can remove.
        /// For example, a `FINAL` read keeps the columns of its sorting key.
        /// The new output header has the remaining columns first, in their former order,
        /// and may append columns of its own after them, such as the dummy column a join adds.
        /// Appended columns are not in `dropped_output_positions`; only the header shows them.
        std::vector<size_t> dropped_output_positions;
    };

    /// Per child: the positions of the child's current output header the step does not need.
    /// Always one entry per child; an empty entry means the step needs every column of that child.
    using UnneededInputPositions = std::vector<std::vector<size_t>>;

    /// What one column of a step's input header is to the step once the unused columns are gone.
    enum class InputColumnUsage : uint8_t
    {
        ReadNeeded,           /// an input reads it, and what that input feeds is still needed
        ReadDropped,          /// an input reads it, and nothing needs that input any more
        PassesThroughNeeded,  /// no input reads it, and the caller asked for the column itself
        PassesThroughDropped, /// no input reads it, and nobody asked for it
    };

    /// What a child produces once its own unused columns are gone:
    /// its `dropped_output_positions` and its new output header.
    /// The header has the remaining columns first, in their order, and may append new ones after them,
    /// such as the dummy column a join adds.
    /// The positions say what the child no longer produces, not what it was asked for:
    /// a child that cannot drop columns drops none, and the step consumes the ones it does not need.
    struct PrunedInput
    {
        std::vector<size_t> dropped_positions;
        SharedHeader header;

        /// A child that did not change.
        static PrunedInput unchanged(const SharedHeader & header);
    };

    /// Removes what the step no longer needs once nobody needs the columns at `unneeded_output_positions`
    /// of its current output header.
    /// `inputs` has one `PrunedInput` per child: the children are pruned first, since columns are dropped bottom-up.
    /// A column a child keeps that the step does not need is consumed by the step.
    /// A column the child dropped must be one the step does not need.
    /// The order of the remaining outputs is preserved.
    /// Can be used only if canRemoveUnusedColumns returns true.
    virtual RemoveUnusedColumnsResult removeUnusedColumns(
        const std::vector<size_t> & /*unneeded_output_positions*/, const std::vector<PrunedInput> & /*inputs*/);

    /// What removeUnusedColumns would not need of each child once nobody needs these outputs.
    /// The children are to be pruned by that before the step itself is.
    /// Can be used only if canRemoveUnusedColumns returns true, for a step with children.
    virtual UnneededInputPositions getUnneededColumns(const std::vector<size_t> & /*unneeded_output_positions*/) const;

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
    /// For a step with one child and one expression: brings the inputs of `dag`, whose outputs are pruned
    /// already, in line with the child's new header. An input reading a column the child keeps stays, also
    /// where nothing needs it any more, and one reading a column the child dropped goes. A column the child
    /// keeps or appends that the step neither reads nor passes on is consumed by a new input, so that it
    /// stops here. `usages` says what each column of `old_header` is to the step. Returns whether anything
    /// changed.
    static bool alignInputsWithPrunedChild(
        ActionsDAG & dag, const std::vector<InputColumnUsage> & usages, const Block & old_header, const PrunedInput & pruned);

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
