#include "config_core.h"

#if USE_MYSQL

#include <Storages/StorageMaterializedMySQL.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>

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
    StorageInMemoryMetadata in_memory_metadata;
    in_memory_metadata = nested_storage->getInMemoryMetadata();
    setInMemoryMetadata(in_memory_metadata);
}

bool StorageMaterializedMySQL::needRewriteQueryWithFinal(const Names & column_names) const
{
    return needRewriteQueryWithFinalForStorage(column_names, nested_storage);
}

Pipe StorageMaterializedMySQL::read(
    const Names & column_names,
    const StorageMetadataPtr & metadata_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    unsigned int num_streams)
{
    /// If the background synchronization thread has exception.
    rethrowSyncExceptionIfNeed(database);

    return readFinalFromNestedStorage(nested_storage, column_names, metadata_snapshot,
            query_info, context, processed_stage, max_block_size, num_streams);
}

NamesAndTypesList StorageMaterializedMySQL::getVirtuals() const
{
    /// If the background synchronization thread has exception.
    rethrowSyncExceptionIfNeed(database);
    return nested_storage->getVirtuals();
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
