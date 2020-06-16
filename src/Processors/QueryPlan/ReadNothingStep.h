#pragma once
#include <Processors/QueryPlan/ISourceStep.h>

namespace DB
{

class ReadNothingStep : public ISourceStep
{
public:
    explicit ReadNothingStep(DataStream output_stream_);

    String getName() const override { return "ReadNothing"; }

    void initializePipeline(QueryPipeline & pipeline) override;
};

}
