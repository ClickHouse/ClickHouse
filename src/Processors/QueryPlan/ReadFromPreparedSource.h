#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/Pipe.h>

namespace DB
{

/// Create source from prepared pipe.
class ReadFromPreparedSource : public ISourceStep
{
public:
    explicit ReadFromPreparedSource(Pipe pipe_);

    String getName() const override { return "ReadFromPreparedSource"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

protected:
    Pipe pipe;
    ContextPtr context;
};

class ReadFromStorageStep : public ReadFromPreparedSource
{
public:
    ReadFromStorageStep(Pipe pipe_, String storage_name, std::shared_ptr<const StorageLimitsList> storage_limits_)
        : ReadFromPreparedSource(std::move(pipe_)), storage_limits(std::move(storage_limits_))
    {
        setStepDescription(storage_name);

        for (const auto & processor : pipe.getProcessors())
            processor->setStorageLimits(storage_limits);
    }

    String getName() const override { return "ReadFromStorage"; }

private:
    std::shared_ptr<const StorageLimitsList> storage_limits;
};

}
