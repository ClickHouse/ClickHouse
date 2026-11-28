#pragma once

#include <Processors/ISimpleTransform.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/ExpressionActions.h>

#include <Core/Field.h>

namespace DB
{

struct WatermarkColumn
{
    static constexpr const char * name = "_watermark";
};

/// Appends the watermark column.
class CalculateWatermarksTransform final : public ISimpleTransform
{
    void transform(Chunk & chunk) override;

public:
    CalculateWatermarksTransform(
        SharedHeader input_header_,
        SharedHeader output_header_,
        ActionsDAG watermark_expression_,
        Field initial_watermark_,
        ContextPtr context_);

    String getName() const override { return "CalculateWatermarks"; }

private:
    const String result_name;
    const ExpressionActionsPtr watermark_expression;

    Field watermark;
};

}
