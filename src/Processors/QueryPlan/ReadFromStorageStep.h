#pragma once
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Core/QueryProcessingStage.h>
#include <Storages/TableLockHolder.h>
#include <DataStreams/StreamLocalLimits.h>

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

struct SelectQueryInfo;

struct PrewhereInfo;

class EnabledQuota;

/// Reads from storage.
class ReadFromStorageStep : public IQueryPlanStep
{
public:
    ReadFromStorageStep(
        TableLockHolder table_lock,
        StorageMetadataPtr metadata_snapshot,
        StreamLocalLimits & limits,
        SizeLimits & leaf_limits,
        std::shared_ptr<const EnabledQuota> quota,
        StoragePtr storage,
        const Names & required_columns,
        const SelectQueryInfo & query_info,
        std::shared_ptr<Context> context,
        QueryProcessingStage::Enum processing_stage,
        size_t max_block_size,
        size_t max_streams);

    ~ReadFromStorageStep() override;

    String getName() const override { return "ReadFromStorage"; }

    QueryPipelinePtr updatePipeline(QueryPipelines) override;

    void describePipeline(FormatSettings & settings) const override;

private:
    QueryPipelinePtr pipeline;
    Processors processors;
};

}
