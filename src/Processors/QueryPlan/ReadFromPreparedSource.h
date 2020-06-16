#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/Pipe.h>

namespace DB
{

class ReadFromPreparedSource : public ISourceStep
{
public:
    explicit ReadFromPreparedSource(Pipe pipe_);

    String getName() const override { return "ReadNothing"; }

    void initializePipeline(QueryPipeline & pipeline) override;

private:
    Pipe pipe;
};

}
