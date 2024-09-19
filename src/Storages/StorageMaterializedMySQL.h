#pragma once

#include "config.h"

#if USE_MYSQL

#include <Storages/StorageProxy.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class StorageMaterializedMySQL final : public StorageProxy
{
public:
    StorageMaterializedMySQL(const StoragePtr & nested_storage_, const IDatabase * database_);

    String getName() const override { return "MaterializedMySQL"; }

    bool needRewriteQueryWithFinal(const Names & column_names) const override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & metadata_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size, size_t num_streams) override;

    SinkToStoragePtr write(const ASTPtr &, const StorageMetadataPtr &, ContextPtr, bool) override { throwNotAllowed(); }

    ColumnSizeByName getColumnSizes() const override;

    StoragePtr getNested() const override { return nested_storage; }

    void drop() override { nested_storage->drop(); }

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return false; }

    IndexSizeByName getSecondaryIndexSizes() const override
    {
        return nested_storage->getSecondaryIndexSizes();
    }

private:
    [[noreturn]] static void throwNotAllowed()
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "This method is not allowed for MaterializedMySQL");
    }

    StoragePtr nested_storage;
    const IDatabase * database;
};

}

#endif
