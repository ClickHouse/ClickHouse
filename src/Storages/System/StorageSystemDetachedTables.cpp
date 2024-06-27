#include "StorageSystemDetachedTables.h"

#include <Access/ContextAccess.h>
#include <Core/NamesAndTypes.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/System/ReadFromSystemTables.h>
#include <Storages/System/getQueriedColumnsMaskAndHeader.h>
#include <Storages/VirtualColumnUtils.h>

#include <boost/range/adaptor/map.hpp>


namespace DB
{

StorageSystemDetachedTables::StorageSystemDetachedTables(const StorageID & table_id_) : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;

    auto description = ColumnsDescription{
        ColumnDescription{"database", std::make_shared<DataTypeString>(), "The name of the database the table is in."},
        ColumnDescription{"table", std::make_shared<DataTypeString>(), "Table name."},
        ColumnDescription{"uuid", std::make_shared<DataTypeUUID>(), "Table uuid (Atomic database)."},
        ColumnDescription{"metadata_path", std::make_shared<DataTypeString>(), "Path to the table metadata in the file system."},
        ColumnDescription{"is_permanently", std::make_shared<DataTypeUInt8>(), "Table was detached permanently."},
    };

    storage_metadata.setColumns(std::move(description));

    setInMemoryMetadata(storage_metadata);
}

void StorageSystemDetachedTables::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);
    auto sample_block = storage_snapshot->metadata->getSampleBlock();

    auto [columns_mask, res_block] = getQueriedColumnsMaskAndHeader(sample_block, column_names);

    auto reading = std::make_unique<ReadFromSystemDetachedTables>(
        column_names, query_info, storage_snapshot, context, std::move(res_block), std::move(columns_mask), max_block_size);

    query_plan.addStep(std::move(reading));
}
}
