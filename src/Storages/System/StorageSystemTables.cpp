#include "StorageSystemTables.h"

#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Disks/IStoragePolicy.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Processors/ISource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/System/ReadFromSystemTables.h>
#include <Storages/System/getQueriedColumnsMaskAndHeader.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/StringUtils.h>
#include <Common/typeid_cast.h>

#include <boost/range/adaptor/map.hpp>


namespace DB
{


StorageSystemTables::StorageSystemTables(const StorageID & table_id_) : IStorage(table_id_)
{
    StorageInMemoryMetadata storage_metadata;

    auto description = ColumnsDescription{
        {"database", std::make_shared<DataTypeString>(), "The name of the database the table is in."},
        {"name", std::make_shared<DataTypeString>(), "Table name."},
        {"uuid", std::make_shared<DataTypeUUID>(), "Table uuid (Atomic database)."},
        {"engine", std::make_shared<DataTypeString>(), "Table engine name (without parameters)."},
        {"is_temporary", std::make_shared<DataTypeUInt8>(), "Flag that indicates whether the table is temporary."},
        {"data_paths", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Paths to the table data in the file systems."},
        {"metadata_path", std::make_shared<DataTypeString>(), "Path to the table metadata in the file system."},
        {"metadata_modification_time", std::make_shared<DataTypeDateTime>(), "Time of latest modification of the table metadata."},
        {"metadata_version",
         std::make_shared<DataTypeInt32>(),
         "Metadata version for ReplicatedMergeTree table, 0 for non ReplicatedMergeTree table."},
        {"dependencies_database", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Database dependencies."},
        {"dependencies_table",
         std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
         "Table dependencies (materialized views the current table)."},
        {"create_table_query", std::make_shared<DataTypeString>(), "The query that was used to create the table."},
        {"engine_full", std::make_shared<DataTypeString>(), "Parameters of the table engine."},
        {"as_select", std::make_shared<DataTypeString>(), "SELECT query for view."},
        {"partition_key", std::make_shared<DataTypeString>(), "The partition key expression specified in the table."},
        {"sorting_key", std::make_shared<DataTypeString>(), "The sorting key expression specified in the table."},
        {"primary_key", std::make_shared<DataTypeString>(), "The primary key expression specified in the table."},
        {"sampling_key", std::make_shared<DataTypeString>(), "The sampling key expression specified in the table."},
        {"storage_policy", std::make_shared<DataTypeString>(), "The storage policy."},
        {"total_rows",
         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
         "Total number of rows, if it is possible to quickly determine exact number of rows in the table, otherwise NULL (including "
         "underlying Buffer table)."},
        {"total_bytes",
         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
         "Total number of bytes, if it is possible to quickly determine exact number "
         "of bytes for the table on storage, otherwise NULL (does not includes any underlying storage). "
         "If the table stores data on disk, returns used space on disk (i.e. compressed). "
         "If the table stores data in memory, returns approximated number of used bytes in memory."},
        {"total_bytes_uncompressed",
         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
         "Total number of uncompressed bytes, if it's possible to quickly determine the exact number "
         "of bytes from the part checksums for the table on storage, otherwise NULL (does not take underlying storage (if any) into "
         "account)."},
        {"parts", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The total number of parts in this table."},
        {"active_parts",
         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
         "The number of active parts in this table."},
        {"total_marks",
         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
         "The total number of marks in all parts in this table."},
        {"lifetime_rows",
         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
         "Total number of rows INSERTed since server start (only for Buffer tables)."},
        {"lifetime_bytes",
         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()),
         "Total number of bytes INSERTed since server start (only for Buffer tables)."},
        {"comment", std::make_shared<DataTypeString>(), "The comment for the table."},
        {"has_own_data",
         std::make_shared<DataTypeUInt8>(),
         "Flag that indicates whether the table itself stores some data on disk or only accesses some other source."},
        {"loading_dependencies_database",
         std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
         "Database loading dependencies (list of objects which should be loaded before the current object)."},
        {"loading_dependencies_table",
         std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
         "Table loading dependencies (list of objects which should be loaded before the current object)."},
        {"loading_dependent_database", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Dependent loading database."},
        {"loading_dependent_table", std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Dependent loading table."},
    };

    description.setAliases({{"table", std::make_shared<DataTypeString>(), "name"}});

    storage_metadata.setColumns(std::move(description));
    setInMemoryMetadata(storage_metadata);
}

void StorageSystemTables::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    const size_t max_block_size,
    const size_t /*num_streams*/)
{
    storage_snapshot->check(column_names);
    Block sample_block = storage_snapshot->metadata->getSampleBlock();

    auto [columns_mask, res_block] = getQueriedColumnsMaskAndHeader(sample_block, column_names);

    auto reading = std::make_unique<ReadFromSystemTables>(
        column_names, query_info, storage_snapshot, context, std::move(res_block), std::move(columns_mask), max_block_size, false);

    query_plan.addStep(std::move(reading));
}
}
