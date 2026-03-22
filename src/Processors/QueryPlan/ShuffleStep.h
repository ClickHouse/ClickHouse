#pragma once

#include <Processors/QueryPlan/ITransformingStep.h>

namespace DB
{
    class ShuffleStep : public ITransformingStep
    {
    public:
        ShuffleStep(SharedHeader header, size_t limit_);
        String getName() const override { return "ShuffleStep"; }

        void transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    private:
        void updateOutputHeader() override
        {
            output_header = input_headers.front();
        }

        size_t limit;
    };
}
