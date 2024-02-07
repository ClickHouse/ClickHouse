#pragma once

#include <Interpreters/Context.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

/// Create source from prepared pipe.
class ReadFromPreparedSource : public SourceStepWithFilter
{
public:
    explicit ReadFromPreparedSource(
        Pipe pipe_, ContextPtr context_ = nullptr, Context::QualifiedProjectionName qualified_projection_name_ = {});

    String getName() const override { return "ReadFromPreparedSource"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

protected:
    Pipe pipe;
    ContextPtr context;
    Context::QualifiedProjectionName qualified_projection_name;
};

class ReadFromStorageStep : public ReadFromPreparedSource
{
public:
    ReadFromStorageStep(Pipe pipe_, String storage_name, const SelectQueryInfo & query_info_, ContextPtr context_)
        : ReadFromPreparedSource(std::move(pipe_), std::move(context_)), query_info(query_info_)
    {
        setStepDescription(storage_name);

        for (const auto & processor : pipe.getProcessors())
            processor->setStorageLimits(query_info.storage_limits);
    }

    String getName() const override { return "ReadFromStorage"; }

    void applyFilters() override;

private:
    SelectQueryInfo query_info;
};

}
