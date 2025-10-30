#pragma once

#include <Interpreters/Context_fwd.h>
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

class ReadFromStorageStep final : public ReadFromPreparedSource
{
public:
    ReadFromStorageStep(Pipe pipe_, StoragePtr storage_, ContextPtr context_, const SelectQueryInfo & query_info_);

    String getName() const override { return "ReadFromStorage"; }

    const StoragePtr & getStorage() const { return storage; }

private:
    StoragePtr storage;

    ContextPtr context;
    SelectQueryInfo query_info;
};

}
