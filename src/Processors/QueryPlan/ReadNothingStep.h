#pragma once
#include <Processors/QueryPlan/ISourceStep.h>

namespace DB
{

/// Create NullSource with specified structure.
class ReadNothingStep : public ISourceStep
{
public:
    explicit ReadNothingStep(Block output_header);

    String getName() const override { return "ReadNothing"; }

    void initializePipeline(QueryPipeline & pipeline, const BuildQueryPipelineSettings &) override;
};

}
