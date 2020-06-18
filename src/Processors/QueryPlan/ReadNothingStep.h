#pragma once
#include <Processors/QueryPlan/ISourceStep.h>

namespace DB
{

class ReadNothingStep : public ISourceStep
{
public:
    explicit ReadNothingStep(Block output_header);

    String getName() const override { return "ReadNothing"; }

    void initializePipeline(QueryPipeline & pipeline) override;
};

}
