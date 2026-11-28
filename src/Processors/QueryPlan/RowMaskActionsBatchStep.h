#pragma once

#include <Common/VectorWithMemoryTracking.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ITransformingStep.h>

#include <functional>
#include <optional>

namespace DB
{

/// Executes already prepared row-mask expressions in their original order with one chunk handoff.
/// Admission and the original per-action projections are owned by `MutationsInterpreter`.
class RowMaskActionsBatchStep final : public ITransformingStep
{
public:
    using MakeUnion = std::function<std::optional<ActionsDAG>()>;

    RowMaskActionsBatchStep(SharedHeader input_header_, VectorWithMemoryTracking<ActionsDAG> actions_, MakeUnion make_union_ = {});

    [[nodiscard]] String getName() const override { return "RowMaskActionsBatch"; }

    void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

    [[nodiscard]] bool hasCorrelatedExpressions() const override;

private:
    void updateOutputHeader() override;

    VectorWithMemoryTracking<ActionsDAG> actions;
    MakeUnion make_union;
    SharedHeaders action_output_headers;
};

}
