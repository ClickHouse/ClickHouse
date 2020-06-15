#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Core/QueryProcessingStage.h>
#include <Storages/TableStructureLockHolder.h>
#include <Interpreters/SelectQueryOptions.h>

namespace DB
{

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

struct SelectQueryInfo;

struct PrewhereInfo;

/// Reads from storage.
class ReadFromStorageStep : public IQueryPlanStep
{
public:
    ReadFromStorageStep(
        TableStructureReadLockHolder table_lock,
        SelectQueryOptions options,
        StoragePtr storage,
        const Names & required_columns,
        const SelectQueryInfo & query_info,
        const Context & context,
        QueryProcessingStage::Enum processing_stage,
        size_t max_block_size,
        size_t max_streams);

    ~ReadFromStorageStep() override;

    String getName() const override { return "ReadFromStorage"; }

    QueryPipelinePtr updatePipeline(QueryPipelines) override;

private:
    TableStructureReadLockHolder table_lock;
    SelectQueryOptions options;

    StoragePtr storage;
    const Names & required_columns;
    const SelectQueryInfo & query_info;
    const Context & context;
    QueryProcessingStage::Enum processing_stage;
    size_t max_block_size;
    size_t max_streams;

    QueryPipelinePtr pipeline;
};

}
