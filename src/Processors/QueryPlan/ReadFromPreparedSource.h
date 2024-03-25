#pragma once

#include <Interpreters/Context.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/SelectQueryInfo.h>

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
};

class ReadFromStorageStep : public ReadFromPreparedSource
{
public:
    ReadFromStorageStep(Pipe pipe_, String storage_name, ContextPtr context_, const SelectQueryInfo & query_info_);

    String getName() const override { return "ReadFromStorage"; }

private:
    ContextPtr context;
    SelectQueryInfo query_info;
};

}
