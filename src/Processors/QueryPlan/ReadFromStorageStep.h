#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Core/QueryProcessingStage.h>
#include <Storages/TableLockHolder.h>
#include <Interpreters/SelectQueryOptions.h>

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;

struct SelectQueryInfo;

struct PrewhereInfo;

/// Reads from storage.
class ReadFromStorageStep : public IQueryPlanStep
{
public:
    ReadFromStorageStep(
        TableLockHolder table_lock,
        StorageMetadataPtr & metadata_snapshot,
        SelectQueryOptions options,
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
    TableLockHolder table_lock;
    StorageMetadataPtr metadata_snapshot;
    SelectQueryOptions options;

    StoragePtr storage;
    const Names & required_columns;
    const SelectQueryInfo & query_info;
    std::shared_ptr<Context> context;
    QueryProcessingStage::Enum processing_stage;
    size_t max_block_size;
    size_t max_streams;

    QueryPipelinePtr pipeline;
    Processors processors;
};

}
