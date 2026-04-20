#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>


namespace DB
{

/// A pass-through step that blocks query-plan optimizations from crossing it.
///
/// By default no optimization can cross the barrier.
/// Specific optimizations (push-downs, column pruning, read-in-order) can be opted in
/// via `AllowedOptimizations`.
///
/// Typical use: a storage's `readImpl` appends the barrier to the top of its
/// returned plan so outer expressions cannot be reordered relative to
/// side-effecting or throw-prone steps below.
///
class OptimizationBarrierStep : public ITransformingStep
{
public:
    /// Which optimizations are allowed to cross this barrier.
    /// Anything not set here is blocked.
    struct AllowedOptimizations
    {
        /// Allow an outer `LimitStep` to be pushed below the barrier.
        /// Handled in tryPushDownLimit().
        bool push_down_limit = false;

        /// Allow column pruning to remove columns from the barrier's output
        /// when no downstream consumer reads them.
        /// Handled in this class via the canRemoveUnusedColumns() /
        /// canRemoveColumnsFromOutput() / removeUnusedColumns() overrides.
        bool remove_unused_columns = false;

        /// Allow `optimizeReadInOrder` to treat this step as transparent when
        /// matching an outer `SortingStep` against storage sort order.
        /// Handled in findReadingStep() (optimizeReadInOrder.cpp).
        bool read_in_order = false;
    };

    OptimizationBarrierStep(
        const SharedHeader & header,
        AllowedOptimizations allowed_optimizations_);

    OptimizationBarrierStep(const OptimizationBarrierStep &) = default;

    String getName() const override { return "OptimizationBarrier"; }

    const AllowedOptimizations & getAllowedOptimizations() const { return allowed_optimizations; }

    bool allow_push_down_limit() const { return allowed_optimizations.push_down_limit; }
    bool allow_remove_unused_columns() const { return allowed_optimizations.remove_unused_columns; }
    bool allow_read_in_order() const { return allowed_optimizations.read_in_order; }

    void transformPipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override {}

    QueryPlanStepPtr clone() const override
    {
        return std::make_unique<OptimizationBarrierStep>(*this);
    }

    void describeActions(FormatSettings & settings) const override;
    void describeActions(JSONBuilder::JSONMap & map) const override;

    bool isSerializable() const override { return true; }
    void serialize(Serialization & ctx) const override;
    static QueryPlanStepPtr deserialize(Deserialization & ctx);

    bool canRemoveUnusedColumns() const override { return allow_remove_unused_columns(); }
    bool canRemoveColumnsFromOutput() const override { return allow_remove_unused_columns(); }
    RemovedUnusedColumns removeUnusedColumns(NameMultiSet required_outputs, bool remove_inputs) override;

private:
    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    AllowedOptimizations allowed_optimizations;
};

}
