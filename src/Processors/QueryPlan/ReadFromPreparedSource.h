#pragma once
#include <Common/config.h>
#include <Interpreters/StorageID.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/Pipe.h>
#if USE_HIVE
#include <Storages/Hive/StorageHive.h>
#endif

namespace DB
{

/// Create source from prepared pipe.
class ReadFromPreparedSource : public ISourceStep
{
public:
    explicit ReadFromPreparedSource(Pipe pipe_, ContextPtr context_ = nullptr);

    String getName() const override { return "ReadFromPreparedSource"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override;

    void updateEstimate(MutableColumns &) const override { }

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
        setStepDescription(std::move(storage_name));
    }

    String getName() const override { return "ReadFromStorage"; }
};

#if USE_HIVE
class ReadFromHiveStep : public ReadFromStorageStep
{
public:
    ReadFromHiveStep(Pipe pipe_, String storage_name, StorageID storage_id_, HiveSelectAnalysisResultPtr analysis_result_)
        : ReadFromStorageStep(std::move(pipe_), std::move(storage_name))
        , storage_id(std::move(storage_id_))
        , analysis_result(std::move(analysis_result_))
    {
        setStepDescription(storage_id.getFullNameNotQuoted());
    }

    void updateEstimate(MutableColumns & columns) const override;

private:
    const StorageID storage_id;
    HiveSelectAnalysisResultPtr analysis_result;
};
#endif

}
