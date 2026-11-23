#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{

class CommonSubplanStep : public ITransformingStep
{
public:
    explicit CommonSubplanStep(const SharedHeader & header_);

    CommonSubplanStep(const CommonSubplanStep &) = default;

    [[nodiscard]] String getName() const override { return "CommonSubplan"; }

    void transformPipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override;

    void updateOutputHeader() override
    {
        output_header = input_headers.front();
    }

    [[nodiscard]] QueryPlanStepPtr clone() const override
    {
        return std::make_unique<CommonSubplanStep>(*this);
    }
};

}
