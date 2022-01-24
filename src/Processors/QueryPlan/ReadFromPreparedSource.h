#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/Pipe.h>

namespace DB
{

/// Create source from prepared pipe.
class ReadFromPreparedSource : public ISourceStep
{
public:
    explicit ReadFromPreparedSource(Pipe pipe_, ContextPtr context_ = nullptr);

    String getName() const override { return "ReadFromPreparedSource"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

private:
    Pipe pipe;
    ContextPtr context;
};

class ReadFromStorageStep : public ReadFromPreparedSource
{
public:
    ReadFromStorageStep(Pipe pipe_, String storage_name)
        : ReadFromPreparedSource(std::move(pipe_))
    {
        setStepDescription(storage_name);
    }

    String getName() const override { return "ReadFromStorage"; }
};

}
