#include "config.h"

#if USE_MYSQL

#include <Storages/StorageMaterializedMySQL.h>

#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

#include <Parsers/ASTTablesInSelectQuery.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/Transforms/FilterTransform.h>

#include <Databases/MySQL/DatabaseMaterializedMySQL.h>
#include <Storages/ReadFinalForExternalReplicaStorage.h>
#include <Storages/SelectQueryInfo.h>

namespace DB
{

StorageMaterializedMySQL::StorageMaterializedMySQL(const StoragePtr & nested_storage_, const IDatabase * database_)
    : StorageProxy(nested_storage_->getStorageID()), nested_storage(nested_storage_), database(database_)
{
    setInMemoryMetadata(nested_storage->getInMemoryMetadata());
    setVirtuals(*nested_storage->getVirtualsPtr());
}

bool StorageMaterializedMySQL::needRewriteQueryWithFinal(const Names & column_names) const
{
    return needRewriteQueryWithFinalForStorage(column_names, nested_storage);
}

void StorageMaterializedMySQL::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & /*storage_snapshot*/,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    if (const auto * db = typeid_cast<const DatabaseMaterializedMySQL *>(database))
        db->rethrowExceptionIfNeeded();

    readFinalFromNestedStorage(query_plan, nested_storage, column_names,
            query_info, context, processed_stage, max_block_size, num_streams);
}

IStorage::ColumnSizeByName StorageMaterializedMySQL::getColumnSizes() const
{
    auto sizes = nested_storage->getColumnSizes();
    auto nested_header = nested_storage->getInMemoryMetadataPtr()->getSampleBlock();
    String sign_column_name = nested_header.getByPosition(nested_header.columns() - 2).name;
    String version_column_name = nested_header.getByPosition(nested_header.columns() - 1).name;
    sizes.erase(sign_column_name);
    sizes.erase(version_column_name);
    return sizes;
}

}

#endif
