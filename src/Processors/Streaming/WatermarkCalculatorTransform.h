#pragma once

#include <Columns/IColumn_fwd.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ActionsDAG.h>

#include <Processors/IInflatingTransform.h>

#include <Core/Field.h>
#include <base/Decimal.h>

#include <queue>

namespace DB
{

/// Processor that consumes a data chunk, evaluates the watermark expression on it and filters out late rows.
class WatermarkCalculatorTransform final : public IInflatingTransform
{
public:
    WatermarkCalculatorTransform(SharedHeader header_, std::string event_time_column_, ActionsDAG watermark_expression_, ContextPtr context_);

    String getName() const override { return "WatermarkCalculator"; }

protected:
    void consume(Chunk chunk) override;
    bool canGenerate() override;
    Chunk generate() override;

private:
    const std::string event_time_column;
    const ExpressionActionsPtr watermark_expression;

    /// Output queue: typically a filtered data chunk followed by a marker chunk.
    std::queue<Chunk> pending_chunks;

    /// Running watermark across all consumed chunks. Null means "type-min / no observation yet".
    std::optional<Field> emitted_watermark;
};

}
