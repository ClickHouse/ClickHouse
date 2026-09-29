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

    /// What removeUnusedColumns did. The default is the answer when nothing changes.
    struct RemoveUnusedColumnsResult
    {
        /// Whether the step itself changed: its expressions, its input headers, or the columns it outputs.
        bool step_changed = false;

        /// The positions of the step's former output header that went away, sorted; empty when all of
        /// them remain. The new output header has the remaining columns first, in their former order, and
        /// may append columns of its own after them, such as the dummy column a join adds when nothing
        /// else is left; only the header shows those.
        std::vector<size_t> dropped_output_positions;
    };

    /// Per child, in the order of the children: the positions of the child's current output header a step
    /// reads, sorted. Always one entry per child, and every position of a child whose columns are all
    /// still read. An empty entry means the step reads nothing of that child.
    using RequiredInputPositions = std::vector<std::vector<size_t>>;

    /// The sorted positions in `[0, count)` that are not in `positions`, which is sorted too: the dropped
    /// positions of the kept ones, and the other way round.
    static std::vector<size_t> complementPositions(size_t count, const std::vector<size_t> & positions);

    /// What one column of a step's input header is to the step once the unused columns are gone.
    enum class InputColumnUsage : uint8_t
    {
        ReadNeeded,           /// an input reads it, and what that input feeds is still needed
        ReadDropped,          /// an input reads it, and nothing needs that input any more
        PassesThroughNeeded,  /// no input reads it, and the caller asked for the column itself
        PassesThroughDropped, /// no input reads it, and nobody asked for it
    };

    /// What a child produces once its own unused columns are gone: the positions of its former output
    /// header it dropped, sorted, and its new output header, which has the remaining columns first, in
    /// their order, and may append columns after them. That is the child's own `RemoveUnusedColumnsResult`
    /// and output header. The positions say what the child no longer produces, not what it was asked for:
    /// a child that cannot drop columns drops none, and the step consumes the ones it does not need.
    struct PrunedInput
    {
        std::vector<size_t> dropped_positions;
        SharedHeader header;

        /// A child that did not change.
        static PrunedInput unchanged(const SharedHeader & header);
    };

    /// Removes what the step no longer needs to produce the columns at `required_output_positions`, a sorted
    /// list of positions in its current output header, and takes on the new headers of its children, which
    /// have already been pruned themselves, one `PrunedInput` per child. A column a child keeps that the step
    /// does not need - more than it was asked for, or one it appended - is consumed by the step, so the step
    /// outputs the required columns only. A column the child dropped must be one the step does not need.
    /// The order of the remaining outputs is preserved. Can be used only if canRemoveUnusedColumns returns
    /// true.
    virtual RemoveUnusedColumnsResult removeUnusedColumns(
        const std::vector<size_t> & /*required_output_positions*/, const std::vector<PrunedInput> & /*inputs*/);

    /// What removeUnusedColumns would need of each child for these outputs, leaving the step untouched. The
    /// children are to be pruned to that before the step itself is. Requires canGetRequiredColumns.
    virtual RequiredInputPositions getRequiredColumns(const std::vector<size_t> & /*required_output_positions*/) const;

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
    /// Every position of every input header: what a step needs of its children when it drops nothing.
    RequiredInputPositions allInputPositions() const;

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
