#include <Storages/System/StorageSystemPrimes.h>

#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromSystemPrimesStep.h>

namespace DB
{

StorageSystemPrimes::StorageSystemPrimes(
    const StorageID & table_id, const std::string & column_name_, std::optional<UInt64> limit_, UInt64 offset_, UInt64 step_)
    : IStorage(table_id)
    , limit(limit_)
    , offset(offset_)
    , column_name(column_name_)
    , step(step_)
{
    StorageInMemoryMetadata storage_metadata;
    /// This column doesn't have a comment, because otherwise it will be added to all the tables which were created via
    /// CREATE TABLE test as primes(5)
    storage_metadata.setColumns(ColumnsDescription({{column_name_, std::make_shared<DataTypeUInt64>()}}));
    setInMemoryMetadata(storage_metadata);
}

void StorageSystemPrimes::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*num_streams*/)
{
    query_plan.addStep(
        std::make_unique<ReadFromSystemPrimesStep>(
            column_names, query_info, storage_snapshot, context, shared_from_this(), max_block_size));
}

}
