#pragma once
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/Pipe.h>

namespace DB
{

/// Create source from prepared pipe.
class ReadFromPreparedSource : public ISourceStep
{
public:
    explicit ReadFromPreparedSource(Pipe pipe_, std::shared_ptr<Context> context_ = nullptr);

    String getName() const override { return "ReadFromPreparedSource"; }

    void initializePipeline(QueryPipeline & pipeline) override;

private:
    Pipe pipe;
    std::shared_ptr<Context> context;
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
