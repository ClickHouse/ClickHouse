#pragma once
#include <Processors/QueryPlan/ISourceStep.h>

namespace DB
{

/// Create NullSource with specified structure.
class ReadNothingStep : public ISourceStep
{
public:
    explicit ReadNothingStep(Block output_header, bool is_streaming = false);

    String getName() const override { return "ReadNothing"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;
};

}
