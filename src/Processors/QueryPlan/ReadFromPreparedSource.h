#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/Pipe.h>

namespace DB
{

/// Create source from prepared pipe.
class ReadFromPreparedSource : public ISourceStep
{
public:
    explicit ReadFromPreparedSource(Pipe pipe_, std::shared_ptr<Context> context_);

    String getName() const override { return "ReadNothing"; }

    void initializePipeline(QueryPipeline & pipeline) override;

private:
    Pipe pipe;
    std::shared_ptr<Context> context;
};

}
