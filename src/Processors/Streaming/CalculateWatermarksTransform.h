#pragma once

#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ActionsDAG.h>

#include <Processors/IInflatingTransform.h>

#include <Core/Field.h>

#include <queue>

namespace DB
{

/// Evaluates the watermark expression on a data chunk, appends the time-attribute and watermark columns.
class CalculateWatermarksTransform final : public IInflatingTransform
{
public:
    CalculateWatermarksTransform(
        SharedHeader input_header_,
        SharedHeader output_header_,
        std::string event_time_column_,
        ActionsDAG watermark_expression_,
        Field initial_watermark_,
        ContextPtr context_);

    String getName() const override { return "CalculateWatermarks"; }

    void consume(Chunk chunk) override;
    bool canGenerate() override;
    Chunk generate() override;
    Chunk getRemaining() override;

private:
    const std::string event_time_column;
    const ExpressionActionsPtr watermark_expression;

    Field watermark;
    std::queue<Chunk> pending_chunks;
};

}
