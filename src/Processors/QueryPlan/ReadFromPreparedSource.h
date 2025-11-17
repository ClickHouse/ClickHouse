#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

/// Create source from prepared pipe.
class ReadFromPreparedSource : public ISourceStep
{
public:
    explicit ReadFromPreparedSource(Pipe pipe_);

    String getName() const override { return "ReadFromPreparedSource"; }
    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    QueryPlanStepPtr clone() const override
    {
        return std::make_unique<ReadFromPreparedSource>(pipe.clone());
    }

protected:
    Pipe pipe;
};

class ReadFromStorageStep final : public ReadFromPreparedSource
{
public:
    ReadFromStorageStep(Pipe pipe_, StoragePtr storage_, ContextPtr context_, const SelectQueryInfo & query_info_);

    String getName() const override { return "ReadFromStorage"; }

    const StoragePtr & getStorage() const { return storage; }

    QueryPlanStepPtr clone() const override
    {
        return std::make_unique<ReadFromStorageStep>(pipe.clone(), storage, context, query_info);
    }

private:
    StoragePtr storage;

    ContextPtr context;
    SelectQueryInfo query_info;
};

}
