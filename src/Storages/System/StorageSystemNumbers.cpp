#include <Storages/System/StorageSystemNumbers.h>

#include <mutex>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Processors/LimitTransform.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromSystemNumbersStep.h>
#include <Storages/SelectQueryInfo.h>


namespace DB
{

StorageSystemNumbers::StorageSystemNumbers(
    const StorageID & table_id,
    bool multithreaded_,
    const std::string & column_name_,
    std::optional<UInt64> limit_,
    UInt64 offset_,
    UInt64 step_)
    : IStorage(table_id), multithreaded(multithreaded_), limit(limit_), offset(offset_), column_name(column_name_), step(step_)
{
    StorageInMemoryMetadata storage_metadata;
    /// This column doesn't have a comment, because otherwise it will be added to all the tables which were created via
    /// CREATE TABLE test as numbers(5)
    storage_metadata.setColumns(ColumnsDescription({{column_name_, std::make_shared<DataTypeUInt64>()}}));
    setInMemoryMetadata(storage_metadata);
}

void StorageSystemNumbers::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    query_plan.addStep(std::make_unique<ReadFromSystemNumbersStep>(
        column_names, query_info, storage_snapshot, context, shared_from_this(), max_block_size, num_streams));
}
}
